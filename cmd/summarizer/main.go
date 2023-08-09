package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/flashbots/mempool-archiver/summarizer"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	version = "dev" // is set during build process

	// Default values
	defaultDebug   = os.Getenv("DEBUG") == "1"
	defaultLogProd = os.Getenv("LOG_PROD") == "1"

	// Flags
	debugPtr   = flag.Bool("debug", defaultDebug, "print debug output")
	logProdPtr = flag.Bool("log-prod", defaultLogProd, "log in production mode (json)")
	dirPtr     = flag.String("dir", "", "which path to archive")
	outDirPtr  = flag.String("out", "", "where to save output files")
	// saveCSV    = flag.Bool("csv", false, "save a csv summary")
	// limit = flag.Int("limit", 0, "max number of txs to process")

	// Errors
	errLimitReached = errors.New("limit reached")

	// Helpers
	log     *zap.SugaredLogger
	printer = message.NewPrinter(language.English)
)

func main() {
	flag.Parse()

	// Logger setup
	var logger *zap.Logger
	zapLevel := zap.NewAtomicLevel()
	if *debugPtr {
		zapLevel.SetLevel(zap.DebugLevel)
	}
	if *logProdPtr {
		encoderCfg := zap.NewProductionEncoderConfig()
		encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		logger = zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderCfg),
			zapcore.Lock(os.Stdout),
			zapLevel,
		))
	} else {
		logger = zap.New(zapcore.NewCore(
			zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
			zapcore.Lock(os.Stdout),
			zapLevel,
		))
	}

	defer func() { _ = logger.Sync() }()
	log = logger.Sugar()

	log.Infow("Starting mempool-summarizer", "version", version)

	if *dirPtr == "" {
		log.Fatal("-dir argument is required")
	}
	log.Infof("Input directory:  %s", *dirPtr)

	if *outDirPtr == "" {
		*outDirPtr = *dirPtr
		// log.Infof("Using %s as output directory", *outDirPtr)
	}
	log.Infof("Output directory: %s", *outDirPtr)

	archiveDirectory()
}

// archiveDirectory extracts the relevant information from all JSON files in the directory into text files
func archiveDirectory() { //nolint:gocognit
	// Ensure the input directory exists
	if _, err := os.Stat(*dirPtr); os.IsNotExist(err) {
		log.Fatalw("dir does not exist", "dir", *dirPtr)
	}

	// Ensure the output directory exists
	err := os.MkdirAll(*outDirPtr, os.ModePerm)
	if err != nil {
		log.Errorw("os.MkdirAll", "error", err)
		return
	}

	// Setup parquet writer
	fnParquet := filepath.Join(*outDirPtr, "transactions.parquet")
	log.Infof("Parquet output:   %s", fnParquet)
	fw, err := local.NewLocalFileWriter(fnParquet)
	if err != nil {
		log.Fatal("Can't create parquet file", "error", err)
	}
	pw, err := writer.NewParquetWriter(fw, new(summarizer.TxSummaryEntry), 4)
	if err != nil {
		log.Fatal("Can't create parquet writer", "error", err)
	}

	// Parquet config: https://parquet.apache.org/docs/file-format/configurations/
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8K

	// Parquet compression: must be gzip for compatibility with both Clickhouse and S3 Select
	pw.CompressionType = parquet.CompressionCodec_GZIP

	// Process files
	cntProcessedFiles := 0
	cntProcessedTx := 0
	err = filepath.Walk(*dirPtr, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			log.Errorw("filepath.Walk", "error", err)
			return nil
		}

		if fi.IsDir() || filepath.Ext(file) != ".csv" {
			return nil
		}

		log.Infof("Processing: %s", file)
		cntProcessedFiles += 1

		readFile, err := os.Open(file)
		if err != nil {
			log.Errorw("os.Open", "error", err, "file", file)
			return nil
		}
		defer readFile.Close()

		fileReader := bufio.NewReader(readFile)
		for {
			l, err := fileReader.ReadString('\n')
			if len(l) == 0 && err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Errorw("fileReader.ReadString", "error", err)
				break
			}

			l = strings.Trim(l, "\n")
			items := strings.Split(l, ",")
			if len(items) != 3 {
				log.Errorw("invalid line", "line", l)
				continue
			}

			txSummary, err := parseTx(items[0], items[1], items[2])
			if err != nil {
				log.Errorw("parseTx", "error", err, "line", l)
				continue
			}

			if err = pw.Write(txSummary); err != nil {
				log.Errorw("parquet.Write", "error", err)
			}

			cntProcessedTx += 1
		}

		// if *limit > 0 && cntProcessedFiles%*limit == 0 {
		// 	return errLimitReached
		// }
		return nil
	})
	if err != nil && !errors.Is(err, errLimitReached) {
		log.Errorw("filepath.Walk", "error", err)
	}

	if err = pw.WriteStop(); err != nil {
		log.Errorw("parquet.WriteStop", "error", err)
	}
	fw.Close()

	log.Infof("Finished processing %d CSV files, %d transactions", printer.Sprintf("%d", cntProcessedFiles), printer.Sprintf("%d", cntProcessedTx))
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s := fmt.Sprintf("Alloc = %v MiB, tTotalAlloc = %v MiB, Sys = %v MiB, tNumGC = %v", m.Alloc/1024/1024, m.TotalAlloc/1024/1024, m.Sys/1024/1024, m.NumGC)
	log.Info(s)
}

func parseTx(timestampMs, hash, rawTx string) (summarizer.TxSummaryEntry, error) {
	ts, err := strconv.Atoi(timestampMs)
	if err != nil {
		return summarizer.TxSummaryEntry{}, err
	}

	rawTxBytes, err := hexutil.Decode(rawTx)
	if err != nil {
		return summarizer.TxSummaryEntry{}, err
	}

	var tx types.Transaction
	err = rlp.DecodeBytes(rawTxBytes, &tx)
	if err != nil {
		return summarizer.TxSummaryEntry{}, err
	}

	// prepare 'from' address, fails often because of unsupported tx type
	from, err := types.Sender(types.NewEIP155Signer(tx.ChainId()), &tx)
	if err != nil {
		_ = err
	}

	// prepare 'to' address
	to := ""
	if tx.To() != nil {
		to = tx.To().Hex()
	}

	// prepare '4 bytes' of data (function name)
	data4Bytes := ""
	if len(tx.Data()) >= 4 {
		data4Bytes = hexutil.Encode(tx.Data()[:4])
	}

	return summarizer.TxSummaryEntry{
		Timestamp: int64(ts),
		Hash:      tx.Hash().Hex(),

		ChainID:   tx.ChainId().String(),
		From:      from.Hex(),
		To:        to,
		Value:     tx.Value().String(),
		Nonce:     tx.Nonce(),
		Gas:       tx.Gas(),
		GasPrice:  tx.GasPrice().String(),
		GasTipCap: tx.GasTipCap().String(),
		GasFeeCap: tx.GasFeeCap().String(),

		DataSize:   int64(len(tx.Data())),
		Data4Bytes: data4Bytes,
	}, nil
}
