package cmd_merge //nolint:stylecheck

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/flashbots/mempool-dumpster/common"
	"github.com/urfave/cli/v2"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// Number of RPC workers for checking transaction inclusion status
var txLimit = 0 // max transactions to process

// mergeTransactions merges multiple transaction CSV files into transactions.parquet + metadata.csv files
func mergeTransactions(cCtx *cli.Context) error {
	var err error
	timeStart := time.Now().UTC()

	outDir := cCtx.String("out")
	fnPrefix := cCtx.String("fn-prefix")
	txBlacklistFiles := cCtx.StringSlice("tx-blacklist")
	sourcelogFiles := cCtx.StringSlice("sourcelog")
	writeTxCSV := cCtx.Bool("write-tx-csv")
	checkNodeURIs := cCtx.StringSlice("check-node")
	writeSummary := cCtx.Bool("write-summary")
	inputFiles := cCtx.Args().Slice()

	log = common.GetLogger(false, false)
	defer func() { _ = log.Sync() }()

	if cCtx.NArg() == 0 {
		log.Fatal("no input files specified as arguments")
	}

	log.Infow("Merge transactions",
		"version", common.Version,
		"outDir", outDir,
		"fnPrefix", fnPrefix,
		"checkNodes", checkNodeURIs,
	)

	err = os.MkdirAll(outDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("os.MkdirAll: %w", err)
	}

	// Ensure output files are don't yet exist
	fnCSVMeta := filepath.Join(outDir, "metadata.csv")
	fnParquetTxs := filepath.Join(outDir, "transactions.parquet")
	fnCSVTxs := filepath.Join(outDir, "transactions.csv")
	fnSummary := filepath.Join(outDir, "summary.txt")
	if fnPrefix != "" {
		fnParquetTxs = filepath.Join(outDir, fmt.Sprintf("%s.parquet", fnPrefix))
		fnCSVMeta = filepath.Join(outDir, fmt.Sprintf("%s.csv", fnPrefix))
		fnCSVTxs = filepath.Join(outDir, fmt.Sprintf("%s_transactions.csv", fnPrefix))
		fnSummary = filepath.Join(outDir, fmt.Sprintf("%s_summary.txt", fnPrefix))
	}
	common.MustNotExist(log, fnParquetTxs)
	common.MustNotExist(log, fnCSVMeta)
	common.MustNotExist(log, fnCSVTxs)
	if writeSummary {
		common.MustNotExist(log, fnSummary)
	}

	log.Infof("Output Parquet file: %s", fnParquetTxs)
	log.Infof("Output metadata CSV file: %s", fnCSVMeta)
	if writeTxCSV {
		log.Infof("Output transactions CSV file: %s", fnCSVTxs)
	}

	// Check input files
	for _, fn := range append(inputFiles, sourcelogFiles...) {
		common.MustBeCSVFile(log, fn)
	}

	//
	// Load sourcelog files
	//
	log.Infow("Loading sourcelog files...", "files", sourcelogFiles)
	sourcelog, _ := common.LoadSourcelogFiles(log, sourcelogFiles)
	log.Infow("Loaded sourcelog files", "memUsed", common.GetMemUsageHuman())

	//
	// Load input files
	//
	txs, err := common.LoadTransactionCSVFiles(log, inputFiles, txBlacklistFiles)
	if err != nil {
		return fmt.Errorf("LoadTransactionCSVFiles: %w", err)
	}

	log.Infow("Processed all input tx files", "txTotal", printer.Sprintf("%d", len(txs)), "memUsed", common.GetMemUsageHuman())

	// Attach sources (sorted by timestamp) to transactions
	cntUpdated := 0
	type srcWithTS struct {
		source    string
		timestamp int64
	}
	for hash, tx := range txs {
		txSources := make([]srcWithTS, 0, len(sourcelog[hash]))
		for source := range sourcelog[hash] {
			txSources = append(txSources, srcWithTS{source: source, timestamp: sourcelog[hash][source]})
		}

		// sort by timestamp
		sort.Slice(txSources, func(i, j int) bool {
			return txSources[i].timestamp < txSources[j].timestamp
		})

		// add to tx
		tx.Sources = make([]string, len(txSources))
		for i, src := range txSources {
			tx.Sources[i] = src.source
		}

		cntUpdated += 1
	}
	log.Infow("Updated transactions with sources", "txUpdated", printer.Sprintf("%d", cntUpdated), "memUsed", common.GetMemUsageHuman())

	//
	// Update txs with inclusion status
	//
	err = updateInclusionStatus(log, checkNodeURIs, txs)
	if err != nil {
		return fmt.Errorf("updateInclusionStatus: %w", err)
	}

	//
	// Convert map to slice sorted by summary.timestamp
	//
	log.Info("Sorting transactions by timestamp...")
	txsSlice := make([]*common.TxSummaryEntry, 0, len(txs))
	for _, v := range txs {
		txsSlice = append(txsSlice, v)
	}
	sort.Slice(txsSlice, func(i, j int) bool {
		return txsSlice[i].Timestamp < txsSlice[j].Timestamp
	})
	log.Infow("Transactions sorted...", "txs", printer.Sprintf("%d", len(txsSlice)), "memUsed", common.GetMemUsageHuman())

	//
	// Write output files
	//
	cntTxWritten := writeFiles(txsSlice, fnParquetTxs, fnCSVTxs, fnCSVMeta)
	log.Infow("Finished merging!", "cntTx", printer.Sprintf("%d", cntTxWritten), "duration", time.Since(timeStart).String())

	// Analyze and write summary
	if writeSummary {
		log.Info("Analyzing...")
		analyzer := common.NewAnalyzer2(common.Analyzer2Opts{ //nolint:exhaustruct
			Transactions: txs,
			Sourelog:     sourcelog,
			SourceComps:  common.DefaultSourceComparisons,
		})

		err = analyzer.WriteToFile(fnSummary)
		if err != nil {
			return fmt.Errorf("analyzer.WriteToFile: %w", err)
		}
		log.Infof("Wrote summary file %s", fnSummary)
	}
	return nil
}

func writeFiles(txs []*common.TxSummaryEntry, fnParquetTxs, fnCSVTxs, fnCSVMeta string) (cntTxWritten int) { //nolint:gocognit
	writeTxCSV := fnCSVTxs != ""

	fCSVMeta, err := os.OpenFile(fnCSVMeta, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		log.Fatalw("os.Create", "error", err, "file", fnCSVMeta)
	}

	csvHeader := strings.Join(common.TxSummaryEntryCSVHeader, ",")
	_, err = fmt.Fprintf(fCSVMeta, "%s\n", csvHeader)
	if err != nil {
		log.Fatalw("fCSVMeta.WriteCSVHeader", "error", err, "file", fnCSVMeta)
	}

	var fCSVTxs *os.File
	if writeTxCSV {
		fCSVTxs, err = os.OpenFile(fnCSVTxs, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			log.Fatalw("os.Create", "error", err, "file", fnCSVTxs)
		}

		_, err = fmt.Fprintf(fCSVTxs, "timestamp_ms,hash,raw_tx\n")
		if err != nil {
			log.Fatalw("fCSVTxs.WriteCSVHeader", "error", err, "file", fnCSVTxs)
		}
	}

	// Setup parquet writer
	fw, err := local.NewLocalFileWriter(fnParquetTxs)
	if err != nil {
		log.Fatalw("parquet.NewLocalFileWriter", "error", err, "file", fnParquetTxs)
	}

	pw, err := writer.NewParquetWriter(fw, new(common.TxSummaryEntry), 4)
	if err != nil {
		log.Fatalw("parquet.NewParquetWriter", "error", err, "file", fnParquetTxs)
	}

	// Parquet config: https://parquet.apache.org/docs/file-format/configurations/
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 1024 * 1024           // 1M

	// Parquet compression: must be gzip for compatibility with both ClickHouse and S3 Select
	pw.CompressionType = parquet.CompressionCodec_GZIP

	//
	// Write output files
	//
	log.Info("Writing output files...")

	cntTxTotal := len(txs)
	cntTxAlreadyIncluded := 0
	for _, tx := range txs {
		// Skip transactions that were included before they were received
		if tx.WasIncludedBeforeReceived() {
			cntTxAlreadyIncluded += 1
			log.Infow("Skipping already included tx", "tx", tx.Hash, "src", tx.Sources, "block", tx.IncludedAtBlockHeight, "blockTs", tx.IncludedBlockTimestamp, "receivedAt", tx.Timestamp, "inclusionDelayMs", tx.InclusionDelayMs)
			continue
		}

		// Write to parquet
		if err = pw.Write(tx); err != nil {
			log.Errorw("parquet.Write", "error", err)
		}

		// Write to transactions CSV
		if writeTxCSV {
			if _, err = fmt.Fprintf(fCSVTxs, "%d,%s,%s\n", tx.Timestamp, tx.Hash, tx.RawTxHex()); err != nil {
				log.Errorw("fCSVTxs.WriteString", "error", err)
			}
		}

		// Write to summary CSV
		csvRow := strings.Join(tx.ToCSVRow(), ",")
		if _, err = fmt.Fprintf(fCSVMeta, "%s\n", csvRow); err != nil {
			log.Errorw("fCSV.WriteString", "error", err)
		}

		cntTxWritten += 1
		if cntTxWritten%100000 == 0 {
			log.Infow(printer.Sprintf("- wrote transactions %d / %d", cntTxWritten, cntTxTotal), "memUsed", common.GetMemUsageHuman())
		}
		if txLimit > 0 && cntTxWritten == txLimit {
			break
		}
	}

	log.Infow(
		printer.Sprintf("- wrote transactions %d / %d", cntTxWritten, cntTxTotal),
		"cntTxAlreadyIncluded", common.PrettyInt(cntTxAlreadyIncluded),
		"memUsed", common.GetMemUsageHuman(),
	)

	log.Info("Flushing and closing files...")
	if writeTxCSV {
		err = fCSVTxs.Close()
		if err != nil {
			log.Fatalw("os.Close", "error", err, "file", fnCSVTxs)
		}
	}
	err = fCSVMeta.Close()
	if err != nil {
		log.Fatalw("os.Close", "error", err, "file", fnCSVMeta)
	}

	err = pw.WriteStop()
	if err != nil {
		log.Fatalw("pw.WriteStop", "error", err, "file", fnParquetTxs)
	}

	fw.Close()

	return cntTxWritten
}
