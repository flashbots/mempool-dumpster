// Package collector contains the mempool collector service
package collector

import (
	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/zap"
)

type CollectorOpts struct {
	Log          *zap.SugaredLogger
	UID          string
	Nodes        []string
	OutDir       string
	CheckNodeURI string

	BloxrouteAuth  []string
	EdenAuth       []string
	ChainboundAuth []string
}

// Start kicks off all the service components in the background
func Start(opts *CollectorOpts) {
	processor := NewTxProcessor(TxProcessorOpts{
		Log:          opts.Log,
		UID:          opts.UID,
		OutDir:       opts.OutDir,
		CheckNodeURI: opts.CheckNodeURI,
	})
	go processor.Start()

	// Regular nodes
	for _, node := range opts.Nodes {
		conn := NewNodeConnection(opts.Log, node, processor.txC)
		conn.StartInBackground()
	}

	// Bloxroute
	for _, auth := range opts.BloxrouteAuth {
		token, url := common.GetAuthTokenAndURL(auth)
		startBloxrouteConnection(BlxNodeOpts{
			TxC:        processor.txC,
			Log:        opts.Log,
			AuthHeader: token,
			URL:        url,
		})
	}

	// Eden
	for _, auth := range opts.EdenAuth {
		token, url := common.GetAuthTokenAndURL(auth)
		startEdenConnection(EdenNodeOpts{
			TxC:        processor.txC,
			Log:        opts.Log,
			AuthHeader: token,
			URL:        url,
		})
	}

	// Chainbound
	for _, auth := range opts.ChainboundAuth {
		token, url := common.GetAuthTokenAndURL(auth)
		chainboundConn := NewChainboundNodeConnection(ChainboundNodeOpts{
			TxC:    processor.txC,
			Log:    opts.Log,
			APIKey: token,
			URL:    url,
		})
		go chainboundConn.Start()
	}
}
