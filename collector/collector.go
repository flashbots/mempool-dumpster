// Package collector contains the mempool collector service
package collector

import (
	"go.uber.org/zap"
)

// Start kicks off all the service components in the background
func Start(log *zap.SugaredLogger, nodes []string, outDir, uid, bloxrouteAuthToken string, srcStats bool) {
	processor := NewTxProcessor(log, outDir, uid, srcStats)
	go processor.Start()

	for _, node := range nodes {
		conn := NewNodeConnection(log, node, processor.txC)
		go conn.Start()
	}

	if bloxrouteAuthToken != "" {
		blxConn := NewBlxNodeConnection(log, bloxrouteAuthToken, processor.txC)
		go blxConn.Start()
	}
}
