package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flashbots/mempool-dumpster/api"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/urfave/cli/v2"
)

var (
	version = "dev" // is set during build process

	cliFlags = []cli.Flag{
		&cli.BoolFlag{
			Name:     "debug",
			EnvVars:  []string{"DEBUG"},
			Usage:    "enable debug logging",
			Category: "Collector Configuration",
		},

		&cli.StringFlag{
			Name:     "api-listen-addr",
			EnvVars:  []string{"API_ADDR"},
			Value:    "localhost:8060",
			Usage:    "API listen address (host:port)",
			Category: "Tx Receivers Configuration",
		},
	}
)

func main() {
	app := &cli.App{
		Name:    "dev",
		Usage:   "dev stuff",
		Version: version,
		Flags:   cliFlags,
		Action:  runDev,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func runDev(cCtx *cli.Context) error {
	var (
		debug         = cCtx.Bool("debug")
		apiListenAddr = cCtx.String("api-listen-addr")
	)

	// Logger setup
	log := common.GetLogger(debug, false)
	defer func() { _ = log.Sync() }()

	if apiListenAddr == "" {
		log.Fatal("api-listen-addr is required")
	}

	server := api.New(&api.HTTPServerConfig{
		Log:        log,
		ListenAddr: apiListenAddr,
	})
	server.RunInBackground()

	// Send dummy txs all X seconds
	txRLP := "0x02f873018305643b840f2c19f08503f8bfbbb2832ab980940ed1bcc400acd34593451e76f854992198995f52808498e5b12ac080a051eb99ae13fd1ace55dd93a4b36eefa5d34e115cd7b9fd5d0ffac07300cbaeb2a0782d9ad12490b45af932d8c98cb3c2fd8c02cdd6317edb36bde2df7556fa9132"
	_, tx, err := common.ParseTxRLP(int64(1693785600337), txRLP)
	if err != nil {
		return err
	}
	ctx := context.TODO()

	go func() {
		for {
			time.Sleep(2 * time.Second)
			tx := common.TxIn{
				T:      time.Now().UTC(),
				Tx:     tx,
				Source: "dummy",
			}

			err = server.SendTx(ctx, &tx)
			if err != nil {
				log.Errorw("failed to send tx", "err", err)
			}
		}
	}()

	// Wwait for termination signal
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	<-exit
	log.Info("bye")
	return nil
}
