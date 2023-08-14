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
	printVersion = flag.Bool("version", false, "only print version")
	debugPtr     = flag.Bool("debug", defaultDebug, "print debug output")
	logProdPtr   = flag.Bool("log-prod", defaultLogProd, "log in production mode (json)")
	outDirPtr    = flag.String("out", "", "where to save output files")
	// limit = flag.Int("limit", 0, "max number of txs to process")

	// Errors
	// errLimitReached = errors.New("limit reached")

	// Helpers
	log     *zap.SugaredLogger
	printer = message.NewPrinter(language.English)
)

func main() {
	flag.Parse()

	// perhaps only print the version
	if *printVersion {
		fmt.Printf("mempool-summarizer %s\n", version)
		return
	}

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

	// Ensure output directory is set
	if *outDirPtr == "" {
		log.Fatal("-out argument is required")
	}

	// Collect input files from arguments
	files := flag.Args()
	if flag.NArg() == 0 {
		fmt.Println("No input files specified as arguments.")
		os.Exit(1)
	}

	log.Infow("Starting mempool-summarizer", "version", version)
	log.Infof("Output directory: %s", *outDirPtr)

	// Ensure the output directory exists
	err := os.MkdirAll(*outDirPtr, os.ModePerm)
	if err != nil {
		log.Errorw("os.MkdirAll", "error", err)
		return
	}

	archiveDirectory(files)
}

// archiveDirectory parses all input CSV files into one output CSV and one output Parquet file.
func archiveDirectory(files []string) {
	// Ensure all files exist and are CSVs
	for _, filename := range files {
		if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
			log.Fatalf("Input file does not exist: %s", filename)
		}
		if filepath.Ext(filename) != ".csv" {
			log.Fatalf("Input file is not a CSV file: %s", filename)
		}
	}

	// Both output
	// _fn := fmt.Sprintf("%s_transactions_%s.csv", t.Format(time.DateOnly), p.uid)
	// fn := filepath.Join(dir, _fn)
	// f, err = os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	// if err != nil {
	// 	p.log.Errorw("os.Create", "error", err)
	// 	return nil, err
	// }

	// // Setup parquet writer
	// fnParquet := filepath.Join(*outDirPtr, "transactions.parquet")
	// log.Infof("Parquet output:   %s", fnParquet)
	// fw, err := local.NewLocalFileWriter(fnParquet)
	// if err != nil {
	// 	log.Fatal("Can't create parquet file", "error", err)
	// }
	// pw, err := writer.NewParquetWriter(fw, new(summarizer.TxSummaryEntry), 4)
	// if err != nil {
	// 	log.Fatal("Can't create parquet writer", "error", err)
	// }

	// // Parquet config: https://parquet.apache.org/docs/file-format/configurations/
	// pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	// pw.PageSize = 8 * 1024              // 8K

	// // Parquet compression: must be gzip for compatibility with both Clickhouse and S3 Select
	// pw.CompressionType = parquet.CompressionCodec_GZIP

	// Process files
	cntProcessedFiles := 0
	cntProcessedTx := 0
	type txMeta struct {
		summary *summarizer.TxSummaryEntry
		tx      *types.Transaction
	}

	txs := make(map[string]txMeta)

	// Collect transactions in-memory from all input files
	for _, filename := range files {
		log.Infof("Processing: %s", filename)
		cntProcessedFiles += 1
		cntTxInFileTotal := 0
		cntTxInFileNew := 0

		readFile, err := os.Open(filename)
		if err != nil {
			log.Errorw("os.Open", "error", err, "file", filename)
			return
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
			items := strings.Split(l, ",") // timestamp,hash,rlp
			if len(items) != 3 {
				log.Errorw("invalid line", "line", l)
				continue
			}

			cntTxInFileTotal += 1

			// Check if we already have this tx
			if _, ok := txs[items[1]]; ok {
				log.Debugf("Skipping duplicate tx: %s", items[1])
				continue
			}

			// Process this tx for the first time
			txSummary, tx, err := parseTx(items[0], items[1], items[2])
			if err != nil {
				log.Errorw("parseTx", "error", err, "line", l)
				continue
			}

			// Add to map
			txs[items[1]] = txMeta{summary: &txSummary, tx: tx}
			cntProcessedTx += 1
			cntTxInFileNew += 1
		}
		log.Infow("Processed file",
			"txInFile", printer.Sprintf("%d", cntTxInFileTotal),
			"txInFileNew", printer.Sprintf("%d", cntTxInFileNew),
			"txTotal", printer.Sprintf("%d", cntProcessedTx),
			"memUsedMiB", printer.Sprintf("%d", GetMemUsageMb()),
		)
		break
	}
	PrintMemUsage()

	// if err = pw.Write(txSummary); err != nil {
	// 	log.Errorw("parquet.Write", "error", err)
	// }

	// 		// if *limit > 0 && cntProcessedFiles%*limit == 0 {
	// 		// 	return errLimitReached
	// 		// }
	// 		return nil
	// 	})
	// 	if err != nil && !errors.Is(err, errLimitReached) {
	// 		log.Errorw("filepath.Walk", "error", err)
	// 	}

	// 	if err = pw.WriteStop(); err != nil {
	// 		log.Errorw("parquet.WriteStop", "error", err)
	// 	}
	// 	fw.Close()

	log.Infof("Finished processing %s CSV files, %s transactions", printer.Sprintf("%d", cntProcessedFiles), printer.Sprintf("%d", cntProcessedTx))
}

func GetMemUsageMb() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / 1024 / 1024
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s := fmt.Sprintf("Alloc = %v MiB, tTotalAlloc = %v MiB, Sys = %v MiB, tNumGC = %v", m.Alloc/1024/1024, m.TotalAlloc/1024/1024, m.Sys/1024/1024, m.NumGC)
	log.Info(s)
}

func parseTx(timestampMs, hash, rawTx string) (summarizer.TxSummaryEntry, *types.Transaction, error) {
	ts, err := strconv.Atoi(timestampMs)
	if err != nil {
		return summarizer.TxSummaryEntry{}, nil, err
	}

	rawTxBytes, err := hexutil.Decode(rawTx)
	if err != nil {
		return summarizer.TxSummaryEntry{}, nil, err
	}

	var tx types.Transaction
	err = rlp.DecodeBytes(rawTxBytes, &tx)
	if err != nil {
		return summarizer.TxSummaryEntry{}, nil, err
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
	}, &tx, nil
}
