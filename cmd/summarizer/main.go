package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/flashbots/mempool-archiver/collector"
	"github.com/flashbots/mempool-archiver/summarizer"
	jsoniter "github.com/json-iterator/go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	saveCSV    = flag.Bool("csv", false, "save a csv summary")
	limit      = flag.Int("limit", 0, "max number of txs to process")

	// Errors
	errLimitReached = errors.New("limit reached")

	// Helpers
	log *zap.SugaredLogger
)

func main() {
	flag.Parse()
	_ = *saveCSV
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

	log.Infow("Starting mempool-archiver", "version", version, "dir", *dirPtr)

	if *dirPtr == "" {
		log.Fatal("-dir argument is required")
	}

	if *outDirPtr == "" {
		*outDirPtr = *dirPtr
		log.Infof("Using %s as output directory", *outDirPtr)
	}

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

	// Create output files
	fnFileList := filepath.Join(*outDirPtr, "filelist.txt.gz")
	log.Infof("Writing file list to %s", fnFileList)
	_fFileList, err := os.Create(fnFileList)
	if err != nil {
		log.Errorw("os.Create", "error", err)
		return
	}
	_fFileListGz := gzip.NewWriter(_fFileList)
	fFileListGz := bufio.NewWriter(_fFileListGz)

	// var csvWriter *csv.Writer
	// if *saveCSV {
	// 	fnCSV := filepath.Join(*outDirPtr, "summary.csv")
	// 	log.Infof("Writing CSV to %s", fnCSV)
	// 	fCSV, err := os.Create(fnCSV)
	// 	if err != nil {
	// 		log.Errorw("os.Create", "error", err)
	// 		return
	// 	}
	// 	csvWriter = csv.NewWriter(fCSV)
	// 	err = csvWriter.Write(summarizer.CSVHeader)
	// 	if err != nil {
	// 		log.Errorw("csvWriter.Write", "error", err)
	// 		return
	// 	}
	// }

	// Setup parquet writer
	fnParquet := filepath.Join(*outDirPtr, "summary.parquet")
	log.Infof("Writing parquet to %s", fnParquet)
	fw, err := local.NewLocalFileWriter(fnParquet)
	if err != nil {
		log.Fatal("Can't create parquet file", "error", err)
	}
	pw, err := writer.NewParquetWriter(fw, new(summarizer.TxSummaryEntry), 4)
	if err != nil {
		log.Fatal("Can't create parquet writer", "error", err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8K
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	log.Infof("Counting files...")
	cnt := 0
	err = filepath.Walk(*dirPtr, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			log.Errorw("filepath.Walk", "error", err)
			return nil
		}

		if fi.IsDir() || filepath.Ext(file) != ".json" {
			return nil
		}

		cnt += 1
		return nil
	})
	if err != nil {
		log.Errorw("filepath.Walk", "error", err)
	}
	log.Infof("Found %d files", cnt)

	// Process files
	cntProcessed := 0
	err = filepath.Walk(*dirPtr, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			log.Errorw("filepath.Walk", "error", err)
			return nil
		}

		if fi.IsDir() || filepath.Ext(file) != ".json" {
			return nil
		}

		log.Debug(file)
		cntProcessed += 1
		if cntProcessed%10000 == 0 {
			log.Infof("Processing file %d/%d", cntProcessed, cnt)
		}
		if cntProcessed%100000 == 0 {
			PrintMemUsage()
		}

		fn := strings.Replace(file, *dirPtr, "", 1)
		_, err = fFileListGz.WriteString(fn + "\n")
		if err != nil {
			log.Errorw("fFileList.WriteString", "error", err)
		}

		dat, err := os.ReadFile(file)
		if err != nil {
			log.Errorw("os.ReadFile", "error", err)
			return nil
		}

		json := jsoniter.ConfigCompatibleWithStandardLibrary
		var tx collector.TxDetail
		err = json.Unmarshal(dat, &tx)
		if err != nil {
			if strings.HasPrefix(err.Error(), "Unmarshal: there are bytes left after unmarshal") { // this error still unmarshals correctly
				log.Warnw("json.Unmarshal", "error", err, "fn", file)
			} else {
				log.Errorw("json.Unmarshal", "error", err, "fn", file)
				return nil
			}
		}

		txSummary, err := parseTx(tx)
		if err != nil {
			log.Errorw("parseTx", "error", err, "fn", file)
			return nil
		}

		// if *saveCSV {
		// 	err = csvWriter.Write(summarizer.TxDetailToCSV(tx, false))
		// 	if err != nil {
		// 		log.Errorw("csvWriter.Write", "error", err)
		// 	}
		// }

		if err = pw.Write(txSummary); err != nil {
			log.Errorw("parquet.Write", "error", err)
		}

		if *limit > 0 && cntProcessed%*limit == 0 {
			return errLimitReached
		}
		return nil
	})
	if err != nil && !errors.Is(err, errLimitReached) {
		log.Errorw("filepath.Walk", "error", err)
	}

	if err = pw.WriteStop(); err != nil {
		log.Errorw("parquet.WriteStop", "error", err)
	}
	fw.Close()

	fFileListGz.Flush()
	_fFileListGz.Close()
	_fFileList.Close()

	// if *saveCSV {
	// 	csvWriter.Flush()
	// }

	log.Infof("Finished processing %d JSON files", cntProcessed)
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s := fmt.Sprintf("Alloc = %v MiB, tTotalAlloc = %v MiB, Sys = %v MiB, tNumGC = %v", m.Alloc/1024/1024, m.TotalAlloc/1024/1024, m.Sys/1024/1024, m.NumGC)
	log.Info(s)
}

func parseTx(txDetail collector.TxDetail) (summarizer.TxSummaryEntry, error) {
	rawTxBytes, err := hexutil.Decode(txDetail.RawTx)
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
		Timestamp: txDetail.Timestamp,
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
