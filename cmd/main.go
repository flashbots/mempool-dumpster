package main

import (
	"log"
	"os"

	cmd_analyze "github.com/flashbots/mempool-dumpster/cmd/analyze"
	cmd_collect "github.com/flashbots/mempool-dumpster/cmd/collect"
	cmd_merge "github.com/flashbots/mempool-dumpster/cmd/merge"
	cmd_website "github.com/flashbots/mempool-dumpster/cmd/website"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Version: common.Version,
		Commands: []*cli.Command{
			&cmd_website.Command,
			&cmd_collect.Command,
			&cmd_analyze.Command,
			&cmd_merge.Command,
		},
		HideVersion: false,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
