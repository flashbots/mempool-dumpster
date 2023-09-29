// Package collector contains the mempool collector service
package collector

import (
	"os"
	"strings"

	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/zap"
)

type CollectorOpts struct {
	Log          *zap.SugaredLogger
	UID          string
	Nodes        []string
	OutDir       string
	CheckNodeURI string

	BloxrouteAuthToken string
	EdenAuthToken      string
	ChainboundAPIKey   string
}

// Start kicks off all the service components in the background
func Start(opts *CollectorOpts) {
	processor := NewTxProcessor(TxProcessorOpts{
		Log:          opts.Log,
		OutDir:       opts.OutDir,
		UID:          opts.UID,
		CheckNodeURI: opts.CheckNodeURI,
	})
	go processor.Start()

	for _, node := range opts.Nodes {
		conn := NewNodeConnection(opts.Log, node, processor.txC)
		conn.StartInBackground()
	}

	if opts.BloxrouteAuthToken != "" {
		connectBloxroute(opts.Log, opts.BloxrouteAuthToken, blxDefaultURL, processor.txC)
	}

	// allow multiple bloxroute subscriptions (i.e. websockets + grpc. temporary, poc)
	blxAuthStrings := os.Getenv("BLX_AUTH") // header@host,header@host,...
	if blxAuthStrings != "" {
		for _, connString := range strings.Split(blxAuthStrings, ",") {
			parts := strings.Split(connString, "@")
			if len(parts) != 2 {
				opts.Log.Fatalw("Invalid bloxroute connection string", "connString", connString)
			}
			connectBloxroute(opts.Log, parts[0], parts[1], processor.txC)
		}
	}

	if opts.EdenAuthToken != "" {
		blxOpts := BlxNodeOpts{ //nolint:exhaustruct
			Log:        opts.Log,
			AuthHeader: opts.EdenAuthToken,
			IsEden:     true,
		}
		blxConn := NewBlxNodeConnection(blxOpts, processor.txC)
		go blxConn.Start()
	}

	if opts.ChainboundAPIKey != "" {
		opts := ChainboundNodeOpts{ //nolint:exhaustruct
			Log:    opts.Log,
			APIKey: opts.ChainboundAPIKey,
		}
		chainboundConn := NewChainboundNodeConnection(opts, processor.txC)
		go chainboundConn.Start()
	}
}

func connectBloxroute(log *zap.SugaredLogger, authHeader, url string, txC chan TxIn) {
	blxOpts := BlxNodeOpts{ //nolint:exhaustruct
		Log:        log,
		AuthHeader: authHeader,
		URL:        url, // URL is taken from ENV vars
	}

	// start Websocket or gRPC subscription depending on URL
	if common.IsWebsocketProtocol(blxOpts.URL) {
		blxConn := NewBlxNodeConnection(blxOpts, txC)
		go blxConn.Start()
	} else {
		blxConn := NewBlxNodeConnectionGRPC(blxOpts, txC)
		go blxConn.Start()
	}
}
