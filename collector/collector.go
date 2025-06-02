// Package collector contains the mempool collector service
package collector

import (
	"net/http"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/flashbots/mempool-dumpster/api"
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

	Receivers               []string
	ReceiversAllowedSources []string

	APIListenAddr     string
	MetricsListenAddr string
}

// Start kicks off all the service components in the background
func Start(opts *CollectorOpts) {
	// Start API first
	var apiServer *api.Server
	if opts.APIListenAddr != "" {
		apiServer = api.New(&api.HTTPServerConfig{
			Log:        opts.Log,
			ListenAddr: opts.APIListenAddr,
		})
		go apiServer.RunInBackground()
	}

	// Start metrics server
	if opts.MetricsListenAddr != "" {
		metricsMux := http.NewServeMux()
		metricsMux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			metrics.WritePrometheus(w, true)
		})
		metricsServer := &http.Server{
			Addr:              opts.MetricsListenAddr,
			ReadHeaderTimeout: 5 * time.Second,
			Handler:           metricsMux,
		}
		go func() {
			opts.Log.Infow("Starting metrics server", "listenAddr", opts.MetricsListenAddr)
			err := metricsServer.ListenAndServe()
			if err != nil {
				opts.Log.Fatal("Failed to start metrics server", zap.Error(err))
			}
		}()
	}

	// Initialize the transaction processor, which is the brain of the collector
	processor := NewTxProcessor(TxProcessorOpts{
		Log:                     opts.Log,
		UID:                     opts.UID,
		OutDir:                  opts.OutDir,
		CheckNodeURI:            opts.CheckNodeURI,
		HTTPReceivers:           opts.Receivers,
		ReceiversAllowedSources: opts.ReceiversAllowedSources,
	})

	// If API server is running, add it as a TX receiver
	if apiServer != nil {
		processor.receivers = append(processor.receivers, apiServer)
	}

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
