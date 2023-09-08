package main

import (
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
)

// mergeTransactions merges multiple transaction CSV files into transactions.parquet + metadata.csv files
func mergeTransactions(cCtx *cli.Context) error {
	outDir := cCtx.String("out")
	fnPrefix := cCtx.String("fn-prefix")
	knownTxsFiles := cCtx.StringSlice("known-txs")
	inputFiles := cCtx.Args().Slice()

	if cCtx.NArg() == 0 {
		log.Fatal("no input files specified as arguments")
	}
	log.Infow("Merge transactions", "outDir", outDir, "fnPrefix", fnPrefix, "version", version)

	err := os.MkdirAll(outDir, os.ModePerm)
	check(err, "os.MkdirAll")

	// Ensure output files are don't yet exist
	fnCSVMeta := filepath.Join(outDir, "metadata.csv")
	fnCSVTxs := filepath.Join(outDir, "transactions.csv")
	fnParquetTxs := filepath.Join(outDir, "transactions.parquet")
	if fnPrefix != "" {
		fnParquetTxs = filepath.Join(outDir, fmt.Sprintf("%s.parquet", fnPrefix))
		fnCSVMeta = filepath.Join(outDir, fmt.Sprintf("%s.csv", fnPrefix))
		fnCSVTxs = filepath.Join(outDir, fmt.Sprintf("%s_transactions.csv", fnPrefix))
	}
	common.MustNotExist(log, fnParquetTxs)
	common.MustNotExist(log, fnCSVMeta)
	common.MustNotExist(log, fnCSVTxs)
	log.Infow("Output files", "fnCSVMeta", fnCSVMeta, "fnParquetTxs", fnParquetTxs, "fnCSVTxs", fnCSVTxs)

	// Check input files
	for _, fn := range inputFiles {
		common.MustBeFile(log, fn)
	}

	//
	// Load input files
	//
	txs, err := common.LoadTransactionCSVFiles(log, inputFiles, knownTxsFiles)
	check(err, "LoadTransactionCSVFiles")
	log.Infow("Processed all input files", "txTotal", printer.Sprintf("%d", len(txs)), "memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()))

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
	log.Infow("Transactions sorted...", "txs", printer.Sprintf("%d", len(txsSlice)), "memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()))

	//
	// Prepare output files
	//
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
	log.Infof("Output Parquet file: %s", fnParquetTxs)
	fw, err := local.NewLocalFileWriter(fnParquetTxs)
	check(err, "parquet.NewLocalFileWriter")
	pw, err := writer.NewParquetWriter(fw, new(common.TxSummaryEntry), 4)
	check(err, "parquet.NewParquetWriter")

	// Parquet config: https://parquet.apache.org/docs/file-format/configurations/
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 1024 * 1024           // 1M

	// Parquet compression: must be gzip for compatibility with both Clickhouse and S3 Select
	pw.CompressionType = parquet.CompressionCodec_GZIP

	//
	// Write output files
	//
	log.Info("Writing output files...")
	cntTxWritten := 0
	cntTxTotal := len(txsSlice)
	for _, tx := range txsSlice {
		// Write to parquet
		if err = pw.Write(tx); err != nil {
			log.Errorw("parquet.Write", "error", err)
		}

		// Write to transactions CSV
		if _, err = fmt.Fprintf(fCSVTxs, "%d,%s,%s\n", tx.Timestamp, tx.Hash, tx.RawTxHex()); err != nil {
			log.Errorw("fCSVTxs.WriteString", "error", err)
		}

		// Write to summary CSV
		csvRow := strings.Join(tx.ToCSVRow(), ",")
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
