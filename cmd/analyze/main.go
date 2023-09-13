package main

import (
	"fmt"
	"os"

	"github.com/flashbots/mempool-dumpster/common"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

var (
	version = "dev" // is set during build process
	debug   = os.Getenv("DEBUG") == "1"

	// Helpers
	log *zap.SugaredLogger

	defaultSourceComparisons = cli.NewStringSlice(
		fmt.Sprintf("%s-%s", common.SourceTagBloxroute, common.SourceTagLocal),
		fmt.Sprintf("%s-%s", common.SourceTagChainbound, common.SourceTagLocal),
		fmt.Sprintf("%s-%s", common.SourceTagBloxroute, common.SourceTagChainbound),
		fmt.Sprintf("%s-%s", common.SourceTagChainbound, common.SourceTagBloxroute),
	)

	// CLI flags
	commonFlags = []cli.Flag{
		&cli.StringFlag{ //nolint:exhaustruct
			Name:  "out",
			Value: "",
			Usage: "output filename",
		},
		&cli.StringSliceFlag{ //nolint:exhaustruct
			Name:  "tx-blacklist",
			Value: &cli.StringSlice{},
			Usage: "metadata CSV/ZIP input files with transactions to ignore in analysis",
		},
		&cli.StringSliceFlag{ //nolint:exhaustruct
			Name:  "tx-whitelist",
			Value: &cli.StringSlice{},
			Usage: "metadata CSV/ZIP input files to only use transactions in there for analysis",
		},
		&cli.StringSliceFlag{ //nolint:exhaustruct
			Name:  "cmp",
			Value: defaultSourceComparisons,
			Usage: "compare these sources",
		},
	}
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
		Name:  "analyze",
		Usage: "Analyze sourcelog/transaction CSV files",
		Commands: []*cli.Command{
			{
				Name:    "sourcelog",
				Aliases: []string{"s"},
				Usage:   "analyze sourcelog CSVs",
				Flags:   commonFlags,
				Action:  analyze,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func analyze(cCtx *cli.Context) error {
	fnCSVSourcelog := cCtx.String("out")
	ignoreTxsFiles := cCtx.StringSlice("tx-blacklist")
	allowedTxsFiles := cCtx.StringSlice("tx-whitelist")
	sourceComps := common.NewSourceComps(cCtx.StringSlice("cmp"))

	inputFiles := cCtx.Args().Slice()
	if cCtx.NArg() == 0 {
		log.Fatal("no input files specified as arguments")
	}

	log.Infow("Analyze sourcelog", "version", version)
	log.Infow("Comparing:", "sources", sourceComps)

	// Ensure output files are don't yet exist
	common.MustNotExist(log, fnCSVSourcelog)
	log.Infof("Output file: %s", fnCSVSourcelog)

	// Check input files
	for _, fn := range inputFiles {
		common.MustBeFile(log, fn)
	}

	// Load reference input files (i.e. transactions before the current date to remove false positives)
	ignoreTxs, err := common.LoadTxHashesFromMetadataCSVFiles(log, ignoreTxsFiles)
	check(err, "LoadTxHashesFromMetadataCSVFiles")
	if len(ignoreTxsFiles) > 0 {
		log.Infow("Processed all ignore-tx input files",
			"ignoreTxTotal", printer.Sprintf("%d", len(ignoreTxs)),
			"memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()),
		)
	}

	allowedTxs, err := common.LoadTxHashesFromMetadataCSVFiles(log, allowedTxsFiles)
	check(err, "LoadTxHashesFromMetadataCSVFiles")
	if len(allowedTxsFiles) > 0 {
		log.Infow("Processed all allowed-tx input files",
			"allowedTxTotal", printer.Sprintf("%d", len(allowedTxs)),
			"memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()),
		)
	}

	// Load input files
	sourcelog, cntProcessedRecords := common.LoadSourcelogFiles(log, inputFiles)
	log.Infow("Processed all input files",
		"txTotal", printer.Sprintf("%d", len(sourcelog)),
		"records", printer.Sprintf("%d", cntProcessedRecords),
		"memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()),
	)

	log.Info("Analyzing...")
	analyzer := NewAnalyzer(AnalyzerOpts{
		Transactions: sourcelog,
		TxBlacklist:  ignoreTxs,
		TxWhitelist:  allowedTxs,
		SourceComps:  sourceComps,
	})
	s := analyzer.Sprint()

	if fnCSVSourcelog != "" {
		writeSummary(fnCSVSourcelog, s)
	}

	fmt.Println("")
	fmt.Println(s)
	return nil
}

func writeSummary(fn, s string) {
	log.Infof("Writing summary CSV file %s ...", fn)
	f, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		log.Errorw("openFile", "error", err)
		return
	}
	defer f.Close()
	_, err = f.WriteString(s)
	if err != nil {
		log.Errorw("writeFile", "error", err)
		return
	}
}
