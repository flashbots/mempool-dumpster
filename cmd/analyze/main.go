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

	commonFlags = []cli.Flag{
		&cli.StringFlag{ //nolint:exhaustruct
			Name:  "out",
			Value: "",
			Usage: "output filename",
		},
		&cli.StringSliceFlag{ //nolint:exhaustruct
			Name:  "known-txs",
			Value: &cli.StringSlice{},
			Usage: "reference transaction input files",
		},
	}

	// Helpers
	log *zap.SugaredLogger
	// printer = message.NewPrinter(language.English)
)

func main() {
	log = common.GetLogger(debug, false)
	defer func() { _ = log.Sync() }()

	app := &cli.App{ //nolint:exhaustruct
		Name:  "analyze",
		Usage: "Analyze sourcelog/transaction CSV files",
		Commands: []*cli.Command{
			// {
			// 	Name:    "transactions",
			// 	Aliases: []string{"tx", "t"},
			// 	Usage:   "analyze transaction CSVs",
			// 	Flags:   commonFlags,
			// 	Action:  mergeTransactions,
			// },
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
	knownTxsFiles := cCtx.StringSlice("known-txs")

	inputFiles := cCtx.Args().Slice()
	if cCtx.NArg() == 0 {
		log.Fatal("no input files specified as arguments")
	}

	log.Infow("Merge sourcelog", "version", version)

	// Ensure output files are don't yet exist
	common.MustNotExist(log, fnCSVSourcelog)
	log.Infof("Output file: %s", fnCSVSourcelog)

	// Check input files
	for _, fn := range inputFiles {
		common.MustBeFile(log, fn)
	}

	// Load input files
	sourcelog, cntProcessedRecords := common.LoadSourceLogFiles(log, inputFiles)
	log.Infow("Processed all input files",
		"txTotal", printer.Sprintf("%d", len(sourcelog)),
		"records", printer.Sprintf("%d", cntProcessedRecords),
		"memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()),
	)

	// Load reference input files (i.e. transactions before the current date to remove false positives)
	prevKnownTxs := common.LoadTxHashesFromMetadataCSVFiles(log, knownTxsFiles)
	if len(knownTxsFiles) > 0 {
		log.Infow("Processed all reference input files",
			"refTxFiles", knownTxsFiles,
			"refTxTotal", printer.Sprintf("%d", len(prevKnownTxs)),
			"memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()),
		)
	}

	log.Info("Analyzing...")
	analyzer := NewAnalyzer(sourcelog, prevKnownTxs)
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
