// Package collector contains the mempool collector service
package collector

import (
	"go.uber.org/zap"
)

// Start kicks off all the service components in the background
func Start(log *zap.SugaredLogger, nodes []string, outDir string) {
	processor := NewTxProcessor(log, outDir)
	go processor.Start()

	for _, node := range nodes {
		conn := NewNodeConnection(log, node, processor.txC)
		go conn.Start()
	}
}
