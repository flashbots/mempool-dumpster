// Loads many source CSV files (produced by the collector), creates summary files in CSV and Parquet, and writes a single CSV file with all raw transactions
package cmd_merge //nolint:stylecheck

import (
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	log *zap.SugaredLogger

	numRPCWorkers = common.GetEnvInt("MERGER_RPC_WORKERS", 8)

	// Helpers
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

		&cli.StringFlag{
			Name:     "clickhouse-dsn",
			EnvVars:  []string{"CLICKHOUSE_DSN"},
			Usage:    "ClickHouse server DSN (e.g. clickhouse://user:password@host:9440/dbname?secure=true or clickhouse://default:password@clickhouse:9000/default)",
			Category: "Data Source: Clickhouse",
		},
		&cli.StringFlag{
			Name:     "date-from",
			EnvVars:  []string{"DATE_FROM"},
			Usage:    "Start date (inclusive) for data extraction (e.g. 2022-01-01 or 2022-01-01T00:00:00Z)",
			Category: "Data Source: Clickhouse",
		},
		&cli.StringFlag{
			Name:     "date-to",
			EnvVars:  []string{"DATE_TO"},
			Usage:    "End date (exclusive) for data extraction (e.g. 2022-01-02 or 2022-01-02T00:00:00Z)",
			Category: "Data Source: Clickhouse",
		},
	}
)

var Command = cli.Command{
	Name:  "merge",
	Usage: "Load input CSV files, deduplicate, sort and produce single output file",
	Subcommands: []*cli.Command{
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
