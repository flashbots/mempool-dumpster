package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/flashbots/mempool-dumpster/summarizer"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	version = "dev" // is set during build process

	// Default values
	defaultDebug = os.Getenv("DEBUG") == "1"
	// defaultLogProd = os.Getenv("LOG_PROD") == "1"

	// Flags
	printVersion = flag.Bool("version", false, "only print version")
	debugPtr     = flag.Bool("debug", defaultDebug, "print debug output")
	// logProdPtr   = flag.Bool("log-prod", defaultLogProd, "log in production mode (json)")
	outDirPtr  = flag.String("out", "", "where to save output files")
	outDatePtr = flag.String("out-date", "", "date to use in output file names")
	// limit = flag.Int("limit", 0, "max number of txs to process")

	// Errors
	// errLimitReached = errors.New("limit reached")

	// Helpers
	log     *zap.SugaredLogger
	printer = message.NewPrinter(language.English)
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Use: %s -out <output_directory> <input_file1> <input_file2> <input_dir>/*.csv ... \n\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	// perhaps only print the version
	if *printVersion {
		fmt.Printf("mempool-summarizer %s\n", version)
		return
	}

	// Logger setup
	log = common.GetLogger(*debugPtr, false)
	defer func() { _ = log.Sync() }()

	// Ensure output directory is set
	if *outDirPtr == "" {
		flag.Usage()
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
func archiveDirectory(files []string) { //nolint:gocognit
	// Prepare output file paths, and make sure they don't exist yet
	fnParquet := filepath.Join(*outDirPtr, "transactions.parquet")
	if *outDatePtr != "" {
		fnParquet = filepath.Join(*outDirPtr, fmt.Sprintf("%s.parquet", *outDatePtr))
	}

	fnTransactions := filepath.Join(*outDirPtr, "transactions.csv")
	if *outDatePtr != "" {
		fnTransactions = filepath.Join(*outDirPtr, fmt.Sprintf("%s_transactions.csv", *outDatePtr))
	}

	if _, err := os.Stat(fnParquet); !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("Output file already exists: %s", fnParquet)
	}
	if _, err := os.Stat(fnTransactions); !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("Output file already exists: %s", fnTransactions)
	}

	// Ensure all files exist and are CSVs
	for _, filename := range files {
		s, err := os.Stat(filename)
		if errors.Is(err, os.ErrNotExist) {
			log.Fatalf("Input file does not exist: %s", filename)
		} else if err != nil {
			log.Fatalf("os.Stat: %s", err)
		}

		if s.IsDir() {
			log.Fatalf("Input file is a directory: %s", filename)
		} else if filepath.Ext(filename) != ".csv" {
			log.Fatalf("Input file is not a CSV file: %s", filename)
		}
	}

	// Process input files
	type txMeta struct {
		rlp     string
		summary *summarizer.TxSummaryEntry
	}

	// Collect transactions from all input files to memory (deduped)
	cntProcessedFiles := 0
	txs := make(map[string]*txMeta)
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

			ts, err := strconv.Atoi(items[0])
			if err != nil {
				log.Errorw("strconv.Atoi", "error", err, "line", l)
				continue
			}
			txTimestamp := int64(ts)

			// Dedupe transactions, and make sure to store the lowest timestamp
			if _, ok := txs[items[1]]; ok {
				log.Debugf("Skipping duplicate tx: %s", items[1])

				if txTimestamp < txs[items[1]].summary.Timestamp {
					txs[items[1]].summary.Timestamp = txTimestamp
					log.Debugw("Updating timestamp for duplicate tx", "line", l)
				}

				continue
			}

			// Process this tx
			txSummary, _, err := parseTx(txTimestamp, items[1], items[2])
			if err != nil {
				log.Errorw("parseTx", "error", err, "line", l)
				continue
			}

			// Add to map
			txs[items[1]] = &txMeta{items[2], &txSummary}
			cntTxInFileNew += 1
		}
		log.Infow("Processed file",
			"txInFile", printer.Sprintf("%d", cntTxInFileTotal),
			"txNew", printer.Sprintf("%d", cntTxInFileNew),
			"txTotal", printer.Sprintf("%d", len(txs)),
			"memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()),
		)
		// break
	}

	log.Infow("Processed all input files", "files", cntProcessedFiles, "txTotal", printer.Sprintf("%d", len(txs)), "memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()))

	// Convert map to slice sorted by summary.timestamp
	log.Info("Sorting transactions by timestamp...")
	txsSlice := make([]*txMeta, 0, len(txs))
	for _, v := range txs {
		txsSlice = append(txsSlice, v)
	}
	sort.Slice(txsSlice, func(i, j int) bool {
		return txsSlice[i].summary.Timestamp < txsSlice[j].summary.Timestamp
	})
	log.Infow("Transactions sorted...", "txs", printer.Sprintf("%d", len(txsSlice)), "memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()))

	// Starting to write output files
	log.Infof("Output CSV file: %s", fnTransactions)
	log.Infof("Output Parquet file: %s", fnParquet)
	fTransactions, err := os.OpenFile(fnTransactions, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		log.Errorw("os.Create", "error", err)
		return
	}

	// Setup parquet writer
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

	log.Info("Writing output files...")
	cntTxWritten := 0
	cntTxTotal := len(txsSlice)
	for _, tx := range txsSlice {
		// Write to parquet
		if err = pw.Write(tx.summary); err != nil {
			log.Errorw("parquet.Write", "error", err)
		}

		// Write to CSV
		if _, err = fmt.Fprintf(fTransactions, "%d,%s,%s\n", tx.summary.Timestamp, tx.summary.Hash, tx.rlp); err != nil {
			log.Errorw("fTransactions.WriteString", "error", err)
		}

		cntTxWritten += 1

		if cntTxWritten%100000 == 0 {
			log.Infow(printer.Sprintf("- wrote transactions %d / %d", cntTxWritten, cntTxTotal), "memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()))
		}
	}
	log.Infow(printer.Sprintf("- wrote transactions %d / %d", cntTxWritten, cntTxTotal), "memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()))
	log.Info("Flushing and closing files...")

	if err = pw.WriteStop(); err != nil {
		log.Errorw("parquet.WriteStop", "error", err)
	}
	fw.Close()

	log.Infof("Finished processing %s CSV files, wrote %s transactions", printer.Sprintf("%d", cntProcessedFiles), printer.Sprintf("%d", cntTxWritten))
}

func parseTx(timestampMs int64, hash, rawTx string) (summarizer.TxSummaryEntry, *types.Transaction, error) {
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
		Timestamp: timestampMs,
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
