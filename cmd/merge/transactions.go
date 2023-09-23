package main

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

var (
	// Number of RPC workers for checking transaction inclusion status
	numRPCWorkers = common.GetEnvInt("MERGER_RPC_WORKERS", 8)
	txLimit       = 0 // max transactions to process
)

// mergeTransactions merges multiple transaction CSV files into transactions.parquet + metadata.csv files
func mergeTransactions(cCtx *cli.Context) error {
	var err error
	timeStart := time.Now().UTC()

	outDir := cCtx.String("out")
	fnPrefix := cCtx.String("fn-prefix")
	knownTxsFiles := cCtx.StringSlice("tx-blacklist")
	sourcelogFiles := cCtx.StringSlice("sourcelog")
	writeTxCSV := cCtx.Bool("write-tx-csv")
	checkNodeURIs := cCtx.StringSlice("check-node")
	inputFiles := cCtx.Args().Slice()

	if cCtx.NArg() == 0 {
		log.Fatal("no input files specified as arguments")
	}

	log.Infow("Merge transactions",
		"version", version,
		"outDir", outDir,
		"fnPrefix", fnPrefix,
		"checkNodes", checkNodeURIs,
	)

	err = os.MkdirAll(outDir, os.ModePerm)
	check(err, "os.MkdirAll")

	// Ensure output files are don't yet exist
	fnCSVMeta := filepath.Join(outDir, "metadata.csv")
	fnParquetTxs := filepath.Join(outDir, "transactions.parquet")
	fnCSVTxs := filepath.Join(outDir, "transactions.csv")
	if fnPrefix != "" {
		fnParquetTxs = filepath.Join(outDir, fmt.Sprintf("%s.parquet", fnPrefix))
		fnCSVMeta = filepath.Join(outDir, fmt.Sprintf("%s.csv", fnPrefix))
		fnCSVTxs = filepath.Join(outDir, fmt.Sprintf("%s_transactions.csv", fnPrefix))
	}
	common.MustNotExist(log, fnParquetTxs)
	common.MustNotExist(log, fnCSVMeta)
	common.MustNotExist(log, fnCSVTxs)

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
	log.Infow("Loading sourcelog files...", "memUsed", common.GetMemUsageHuman())
	sourcelog, _ := common.LoadSourcelogFiles(log, sourcelogFiles)
	log.Infow("Loaded sourcelog files", "memUsed", common.GetMemUsageHuman())

	//
	// Load input files
	//
	txs, err := common.LoadTransactionCSVFiles(log, inputFiles, knownTxsFiles)
	check(err, "LoadTransactionCSVFiles")
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
	check(err, "updateInclusionStatus")

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
	// Prepare output files
	//
	cntTxWritten := writeFiles(txsSlice, fnParquetTxs, fnCSVTxs, fnCSVMeta)
	log.Infow("Finished merging!", "cntTx", printer.Sprintf("%d", cntTxWritten), "duration", time.Since(timeStart).String())
	return nil
}

func writeFiles(txs []*common.TxSummaryEntry, fnParquetTxs, fnCSVTxs, fnCSVMeta string) (cntTxWritten int) {
	writeTxCSV := fnCSVTxs != ""

	fCSVMeta, err := os.OpenFile(fnCSVMeta, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	check(err, "os.Create")
	csvHeader := strings.Join(common.TxSummaryEntryCSVHeader, ",")
	_, err = fmt.Fprintf(fCSVMeta, "%s\n", csvHeader)
	check(err, "fCSVTxs.WriteCSVHeader")

	var fCSVTxs *os.File
	if writeTxCSV {
		fCSVTxs, err = os.OpenFile(fnCSVTxs, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		check(err, "os.Create")
		_, err = fmt.Fprintf(fCSVTxs, "timestamp_ms,hash,raw_tx\n")
		check(err, "fCSVTxs.WriteCSVHeader")
	}

	// Setup parquet writer
	fw, err := local.NewLocalFileWriter(fnParquetTxs)
	check(err, "parquet.NewLocalFileWriter")
	pw, err := writer.NewParquetWriter(fw, new(common.TxSummaryEntry), 4)
	check(err, "parquet.NewParquetWriter")

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
	for _, tx := range txs {
		// Skip transactions that were included before they were received
		if tx.InclusionDelayMs <= -12_000 {
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
	log.Infow(printer.Sprintf("- wrote transactions %d / %d", cntTxWritten, cntTxTotal), "memUsed", common.GetMemUsageHuman())

	log.Info("Flushing and closing files...")
	if writeTxCSV {
		err = fCSVTxs.Close()
		check(err, "fCSVTxs.Close")
	}
	err = fCSVMeta.Close()
	check(err, "fCSVMeta.Close")
	err = pw.WriteStop()
	check(err, "pw.WriteStop")
	fw.Close()

	return cntTxWritten
}
