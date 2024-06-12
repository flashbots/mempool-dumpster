package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/flashbots/mempool-dumpster/collector"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/lithammer/shortuuid"
	"github.com/urfave/cli/v2"
)

var (
	version = "dev" // is set during build process

	cliFlags = []cli.Flag{
		// Collector configuration
		&cli.BoolFlag{
			Name:     "debug",
			EnvVars:  []string{"DEBUG"},
			Usage:    "enable debug logging",
			Category: "Collector Configuration",
		},
		&cli.StringFlag{
			Name:     "out",
			EnvVars:  []string{"OUT"},
			Required: true,
			Usage:    "output base directory",
			Category: "Collector Configuration",
		},
		&cli.StringFlag{
			Name:     "uid",
			EnvVars:  []string{"UID"},
			Usage:    "collector uid, part of output CSV filenames (default: random)",
			Category: "Collector Configuration",
		},
		&cli.StringFlag{
			Name:     "check-node",
			EnvVars:  []string{"CHECK_NODE"},
			Usage:    "EL node URL to check incoming transactions",
			Category: "Collector Configuration",
		},

		// Sources
		&cli.StringSliceFlag{
			Name:     "node",
			Aliases:  []string{"nodes"},
			EnvVars:  []string{"NODE", "NODES"},
			Usage:    "EL node URL(s)",
			Category: "Sources Configuration",
		},
		&cli.StringSliceFlag{
			Name:     "blx",
			EnvVars:  []string{"BLX_AUTH"},
			Usage:    "bloXroute auth-header (or auth-header@url)",
			Category: "Sources Configuration",
		},
		&cli.StringSliceFlag{
			Name:     "eden",
			EnvVars:  []string{"EDEN_AUTH"},
			Usage:    "Eden auth-header (or auth-header@url)",
			Category: "Sources Configuration",
		},
		&cli.StringSliceFlag{
			Name:     "chainbound",
			EnvVars:  []string{"CHAINBOUND_AUTH"},
			Usage:    "Chainbound API key (or api-key@url)",
			Category: "Sources Configuration",
		},

		// Tx receivers
		&cli.StringSliceFlag{
			Name:     "tx-receivers",
			EnvVars:  []string{"TX_RECEIVERS"},
			Usage:    "URL(s) to send transactions to as octet-stream over http",
			Category: "Tx Receivers Configuration",
		},
		&cli.StringSliceFlag{
			Name:     "tx-receivers-allowed-sources",
			EnvVars:  []string{"TX_RECEIVERS_ALLOWED_SOURCES"},
			Usage:    "sources of txs to send to receivers",
			Category: "Tx Receivers Configuration",
		},

		// SSE tx subscription
		&cli.StringFlag{
			Name:     "api-listen-addr",
			EnvVars:  []string{"API_ADDR"},
			Usage:    "API listen address (host:port)",
			Category: "Tx Receivers Configuration",
		},
	}
)

func main() {
	app := &cli.App{
		Name:    "mempool-dumpster/collector",
		Usage:   "Collect mempool transactions from various sources",
		Version: version,
		Flags:   cliFlags,
		Action:  runCollector,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func runCollector(cCtx *cli.Context) error {
	var (
		debug                   = cCtx.Bool("debug")
		outDir                  = cCtx.String("out")
		uid                     = cCtx.String("uid")
		checkNodeURI            = cCtx.String("check-node")
		nodeURIs                = cCtx.StringSlice("node")
		blxAuth                 = cCtx.StringSlice("blx")
		edenAuth                = cCtx.StringSlice("eden")
		chainboundAuth          = cCtx.StringSlice("chainbound")
		receivers               = cCtx.StringSlice("tx-receivers")
		receiversAllowedSources = cCtx.StringSlice("tx-receivers-allowed-sources")
		apiListenAddr           = cCtx.String("api-listen-addr")
	)

	// Logger setup
	log := common.GetLogger(debug, false)
	defer func() { _ = log.Sync() }()

	if uid == "" {
		uid = shortuuid.New()[:6]
	}

	if len(nodeURIs) == 0 && len(blxAuth) == 0 && len(edenAuth) == 0 && len(chainboundAuth) == 0 {
		log.Fatal("No nodes, bloxroute, or eden token set (use -nodes <url1>,<url2> / -blx-token <token> / -eden-token <token>)")
	}

	log.Infow("Starting mempool-collector", "version", version, "outDir", outDir, "uid", uid)

	aliases := common.SourceAliasesFromEnv()
	if len(aliases) > 0 {
		log.Infow("Using source aliases:", "aliases", aliases)
	}

	// Start service components
	opts := collector.CollectorOpts{
		Log:                     log,
		UID:                     uid,
		OutDir:                  outDir,
		CheckNodeURI:            checkNodeURI,
		Nodes:                   nodeURIs,
		BloxrouteAuth:           blxAuth,
		EdenAuth:                edenAuth,
		ChainboundAuth:          chainboundAuth,
		Receivers:               receivers,
		ReceiversAllowedSources: receiversAllowedSources,
		APIListenAddr:           apiListenAddr,
	}

	collector.Start(&opts)

	// Wwait for termination signal
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	<-exit
	log.Info("bye")
	return nil
}
