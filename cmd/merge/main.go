// Loads many source CSV files (produced by the collector), creates summary files in CSV and Parquet, and writes a single CSV file with all raw transactions
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/flashbots/mempool-dumpster/common"
	"github.com/urfave/cli/v2"
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
	debug = os.Getenv("DEBUG") == "1"
	// defaultLogProd = os.Getenv("LOG_PROD") == "1"

	// Flags
	// outDirPtr  = flag.String("out", "", "where to save output files")
	// outDatePtr = flag.String("out-date", "", "date to use in output file names")
	// limit = flag.Int("limit", 0, "max number of txs to process")

	// Errors
	// errLimitReached = errors.New("limit reached")

	commonFlags = []cli.Flag{
		&cli.StringFlag{ //nolint:exhaustruct
			Name:  "out",
			Value: "out/",
			Usage: "output directory",
		},
		&cli.StringFlag{ //nolint:exhaustruct
			Name:  "fn-prefix",
			Value: "",
			Usage: "output file prefix (i.e. date)",
		},
	}

	// Helpers
	log     *zap.SugaredLogger
	printer = message.NewPrinter(language.English)
)

func check(err error, msg string) {
	if err != nil {
		log.Fatalw(msg, "error", err)
	}
}

func main() {
	log = common.GetLogger(debug, false)
	defer func() { _ = log.Sync() }()

	app := &cli.App{ //nolint:exhaustruct
		Name:  "merger",
		Usage: "Load input CSV files, deduplicate, sort and produce single output file",
		Commands: []*cli.Command{
			{
				Name:    "transactions",
				Aliases: []string{"tx", "t"},
				Usage:   "merge transaction CSVs",
				Flags:   commonFlags,
				Action:  mergeTransactions,
			},
			{
				Name:    "sourcelog",
				Aliases: []string{"s"},
				Usage:   "merge sourcelog CSVs",
				Flags:   commonFlags,
				Action:  mergeSourcelog,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func mergeSourcelog(cCtx *cli.Context) error {
	fmt.Println("merge sourcelog: ", cCtx.Args().Slice())
	if cCtx.NArg() == 0 {
		log.Fatal("no input files specified as arguments")
	}
	return nil
}

func mergeTransactions(cCtx *cli.Context) error {
	outDir := cCtx.String("out")
	fnPrefix := cCtx.String("fn-prefix")
	if cCtx.NArg() == 0 {
		log.Fatal("no input files specified as arguments")
	}
	inputFiles := cCtx.Args().Slice()
	log.Infow("Merge transactions", "outDir", outDir, "fnPrefix", fnPrefix, "version", version)

	err := os.MkdirAll(outDir, os.ModePerm)
	check(err, "os.MkdirAll")

	// Ensure output files are don't yet exist
	fnParquetMeta := filepath.Join(outDir, "metadata.parquet")
	fnCSVMeta := filepath.Join(outDir, "metadata.csv")
	fnCSVTxs := filepath.Join(outDir, "transactions.csv")
	if fnPrefix != "" {
		fnParquetMeta = filepath.Join(outDir, fmt.Sprintf("%s.parquet", fnPrefix))
		fnCSVMeta = filepath.Join(outDir, fmt.Sprintf("%s.csv", fnPrefix))
		fnCSVTxs = filepath.Join(outDir, fmt.Sprintf("%s_transactions.csv", fnPrefix))
	}
	mustNotExist(fnParquetMeta)
	mustNotExist(fnCSVMeta)
	mustNotExist(fnCSVTxs)
	log.Infow("Output files", "fnParquetMeta", fnParquetMeta, "fnCSVMeta", fnCSVMeta, "fnCSVTxs", fnCSVTxs)

	// Check input files
	for _, fn := range inputFiles {
		mustBeFile(fn)
	}

	// Load input files
	txs := common.LoadTransactionCSVFiles(log, inputFiles)
	log.Infow("Processed all input files", "txTotal", printer.Sprintf("%d", len(txs)), "memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()))

	// Convert map to slice sorted by summary.timestamp
	log.Info("Sorting transactions by timestamp...")
	txsSlice := make([]*common.TxEnvelope, 0, len(txs))
	for _, v := range txs {
		txsSlice = append(txsSlice, v)
	}
	sort.Slice(txsSlice, func(i, j int) bool {
		return txsSlice[i].Summary.Timestamp < txsSlice[j].Summary.Timestamp
	})
	log.Infow("Transactions sorted...", "txs", printer.Sprintf("%d", len(txsSlice)), "memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()))

	// Starting to write output files
	log.Infof("Output transactions CSV file: %s", fnCSVTxs)
	fCSVTxs, err := os.OpenFile(fnCSVTxs, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	check(err, "os.Create")
	_, err = fmt.Fprintf(fCSVTxs, "timestamp_ms,hash,raw_tx\n")
	check(err, "fCSVTxs.WriteCSVHeader")

	log.Infof("Output summary CSV file: %s", fnCSVMeta)
	fCSVMeta, err := os.OpenFile(fnCSVMeta, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	check(err, "os.Create")
	csvHeader := strings.Join(common.TxSummaryEntryCSVHeader, ",")
	_, err = fmt.Fprintf(fCSVMeta, "%s\n", csvHeader)
	check(err, "fCSVTxs.WriteCSVHeader")

	// Setup parquet writer
	log.Infof("Output Parquet summary file: %s", fnParquetMeta)
	fw, err := local.NewLocalFileWriter(fnParquetMeta)
	check(err, "parquet.NewLocalFileWriter")
	pw, err := writer.NewParquetWriter(fw, new(common.TxSummaryEntry), 4)
	check(err, "parquet.NewParquetWriter")

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
		if err = pw.Write(tx.Summary); err != nil {
			log.Errorw("parquet.Write", "error", err)
		}

		// Write to transactions CSV
		if _, err = fmt.Fprintf(fCSVTxs, "%d,%s,%s\n", tx.Summary.Timestamp, tx.Summary.Hash, tx.Rlp); err != nil {
			log.Errorw("fCSVTxs.WriteString", "error", err)
		}

		// Write to summary CSV
		csvRow := strings.Join(tx.Summary.ToCSVRow(), ",")
		if _, err = fmt.Fprintf(fCSVMeta, "%s\n", csvRow); err != nil {
			log.Errorw("fCSV.WriteString", "error", err)
		}

		cntTxWritten += 1
		if cntTxWritten%100000 == 0 {
			log.Infow(printer.Sprintf("- wrote transactions %d / %d", cntTxWritten, cntTxTotal), "memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()))
		}
	}
	log.Infow(printer.Sprintf("- wrote transactions %d / %d", cntTxWritten, cntTxTotal), "memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()))

	log.Info("Flushing and closing files...")
	err = fCSVTxs.Close()
	check(err, "fCSVTxs.Close")
	err = fCSVMeta.Close()
	check(err, "fCSVMeta.Close")
	err = pw.WriteStop()
	check(err, "pw.WriteStop")
	fw.Close()

	log.Infof("Finished processing CSV files, wrote %s transactions", printer.Sprintf("%d", cntTxWritten))
	return nil
}

func mustNotExist(fn string) {
	if _, err := os.Stat(fn); !os.IsNotExist(err) {
		log.Fatalf("Output file already exists: %s", fn)
	}
}

func mustBeFile(fn string) {
	s, err := os.Stat(fn)
	if errors.Is(err, os.ErrNotExist) {
		log.Fatalf("Input file does not exist: %s", fn)
	} else if err != nil {
		log.Fatalf("os.Stat: %s", err)
	}
	if s.IsDir() {
		log.Fatalf("Input file is a directory: %s", fn)
	} else if filepath.Ext(fn) != ".csv" {
		log.Fatalf("Input file is not a CSV file: %s", fn)
	}
}

// 	archiveDirectory(files)
// }

// // archiveDirectory parses all input CSV files into one output CSV and one output Parquet file.
// func archiveDirectory(files []string) { //nolint:gocognit,gocyclo,maintidx

// 	// Collect transactions from all input files to memory (deduped)

// 	log.Infow("Processed all input files", "files", cntProcessedFiles, "txTotal", printer.Sprintf("%d", len(txs)), "memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()))
