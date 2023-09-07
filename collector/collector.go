// Package collector contains the mempool collector service
package collector

import (
	"go.uber.org/zap"
)

type CollectorOpts struct {
	Log                *zap.SugaredLogger
	UID                string
	Nodes              []string
	OutDir             string
	WriteSourcelog     bool
	BloxrouteAuthToken string
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
