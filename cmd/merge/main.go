// Loads many source CSV files (produced by the collector), creates summary files in CSV and Parquet, and writes a single CSV file with all raw transactions
package main

import (
	"os"

	"github.com/flashbots/mempool-dumpster/common"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	version = "dev" // is set during build process
	debug   = os.Getenv("DEBUG") == "1"

	// Helpers
	log     *zap.SugaredLogger
	printer = message.NewPrinter(language.English)

	// Flags
	commonFlags = []cli.Flag{
		&cli.StringFlag{
			Name:  "out",
			Value: "out/",
			Usage: "output directory",
		},
		&cli.StringFlag{
			Name:  "fn-prefix",
			Value: "",
			Usage: "output file prefix (i.e. date)",
		},
	}

	mergeTxFlags = []cli.Flag{
		&cli.StringSliceFlag{
			Name:  "tx-blacklist",
			Value: &cli.StringSlice{},
			Usage: "blacklisted transaction input files (i.e. to ignore txs of previous day)",
		},
		&cli.StringSliceFlag{
			Name:  "sourcelog",
			Value: &cli.StringSlice{},
			Usage: "sourcelog files (to add sources to transactions)",
		},
		&cli.StringSliceFlag{ //nolint:exhaustruct
			Name:  "check-node",
			Usage: "eth nodes for checking tx inclusion status",
		},
		&cli.BoolFlag{
			Name:  "write-tx-csv",
			Value: false,
			Usage: "write a CSV with all received transactions (timestamp_ms,hash,raw_tx)",
		},
		&cli.BoolFlag{
			Name:  "write-summary",
			Usage: "run analyzer and write summary",
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

	app := &cli.App{
		Name:  "merge",
		Usage: "Load input CSV files, deduplicate, sort and produce single output file",
		Commands: []*cli.Command{
			{
				Name:    "transactions",
				Aliases: []string{"tx", "t"},
				Usage:   "merge transaction CSVs",
				Flags:   append(commonFlags, mergeTxFlags...),
				Action:  mergeTransactions,
			},
			{
				Name:    "sourcelog",
				Aliases: []string{"s"},
				Usage:   "merge sourcelog CSVs",
				Flags:   commonFlags,
				Action:  mergeSourcelog,
			},
			{
				Name:   "trash",
				Usage:  "merge trash CSVs",
				Flags:  commonFlags,
				Action: mergeTrash,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
