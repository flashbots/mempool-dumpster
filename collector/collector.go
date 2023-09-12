// Package collector contains the mempool collector service
package collector

import (
	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/zap"
)

type CollectorOpts struct {
	Log            *zap.SugaredLogger
	UID            string
	Nodes          []string
	OutDir         string
	WriteSourcelog bool

	BloxrouteAuthToken string
	EdenAuthToken      string
	ChainboundAPIKey   string
}

// Start kicks off all the service components in the background
func Start(opts *CollectorOpts) {
	processor := NewTxProcessor(opts.Log, opts.OutDir, opts.UID, opts.WriteSourcelog)
	go processor.Start()

	for _, node := range opts.Nodes {
		conn := NewNodeConnection(opts.Log, node, processor.txC)
		go conn.Start()
	}

	if opts.BloxrouteAuthToken != "" {
		blxOpts := BlxNodeOpts{ //nolint:exhaustruct
			Log:        opts.Log,
			AuthHeader: opts.BloxrouteAuthToken,
			URL:        blxDefaultURL, // URL is taken from ENV vars
		}

		// start Websocket or gRPC subscription depending on URL
		if common.IsWebsocketProtocol(blxOpts.URL) {
			blxConn := NewBlxNodeConnection(blxOpts, processor.txC)
			go blxConn.Start()
		} else {
			blxConn := NewBlxNodeConnectionGRPC(blxOpts, processor.txC)
			go blxConn.Start()
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
