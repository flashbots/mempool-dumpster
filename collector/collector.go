// Package collector contains the mempool collector service
package collector

import (
	"net/http"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/flashbots/mempool-dumpster/api"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

type CollectorOpts struct {
	Log      *zap.SugaredLogger
	UID      string
	Location string // location of the collector, will be stored in sourcelogs
	Nodes    []string
	OutDir   string

	CheckNodeURI  string
	ClickhouseDSN string

	BloxrouteAuth  []string
	EdenAuth       []string
	ChainboundAuth []string

	Receivers                []string
	ReceiversAllowedSources  []string
	ReceiversAllowAllSources bool // if true, allows all sources for receivers

	APIListenAddr     string
	MetricsListenAddr string
	EnablePprof       bool // if true, enables pprof on the metrics server
}

type Collector struct {
	opts      *CollectorOpts
	log       *zap.SugaredLogger
	processor *TxProcessor
}

func New(opts CollectorOpts) *Collector {
	return &Collector{
		opts: &opts,
		log:  opts.Log,
	}
}

// Start kicks off all the service components in the background
func (c *Collector) Start() {
	// Start API and metrics servers (if enabled)
	c.StartMetricsServer()
	apiServer := c.StartAPIServer()

	// Initialize the transaction processor, which is the brain of the collector
	c.processor = NewTxProcessor(TxProcessorOpts{
		Log:                      c.log,
		UID:                      c.opts.UID,
		Location:                 c.opts.Location,
		OutDir:                   c.opts.OutDir,
		CheckNodeURI:             c.opts.CheckNodeURI,
		ClickhouseDSN:            c.opts.ClickhouseDSN,
		HTTPReceivers:            c.opts.Receivers,
		ReceiversAllowedSources:  c.opts.ReceiversAllowedSources,
		ReceiversAllowAllSources: c.opts.ReceiversAllowAllSources,
		APIServer:                apiServer,
	})

	// Start the transaction processor, which kicks off background goroutines
	c.processor.Start()

	// Connect to regular nodes
	for _, node := range c.opts.Nodes {
		conn := NewNodeConnection(c.log, node, c.processor.txC)
		conn.StartInBackground()
	}

	// Connect to Bloxroute
	for _, auth := range c.opts.BloxrouteAuth {
		token, url := common.GetAuthTokenAndURL(auth)
		startBloxrouteConnection(BlxNodeOpts{
			TxC:        c.processor.txC,
			Log:        c.log,
			AuthHeader: token,
			URL:        url,
		})
	}

	// Connect to Eden
	for _, auth := range c.opts.EdenAuth {
		token, url := common.GetAuthTokenAndURL(auth)
		startEdenConnection(EdenNodeOpts{
			TxC:        c.processor.txC,
			Log:        c.log,
			AuthHeader: token,
			URL:        url,
		})
	}

	// Connect to Chainbound
	for _, auth := range c.opts.ChainboundAuth {
		token, url := common.GetAuthTokenAndURL(auth)
		chainboundConn := NewChainboundNodeConnection(ChainboundNodeOpts{
			TxC:    c.processor.txC,
			Log:    c.log,
			APIKey: token,
			URL:    url,
		})
		go chainboundConn.Start()
	}
}

func (c *Collector) StartAPIServer() *api.Server {
	if c.opts.APIListenAddr == "" {
		return nil
	}
	apiServer := api.New(&api.HTTPServerConfig{
		Log:        c.log,
		ListenAddr: c.opts.APIListenAddr,
	})
	go apiServer.RunInBackground()
	return apiServer
}

func (c *Collector) StartMetricsServer() {
	if c.opts.MetricsListenAddr == "" {
		return
	}
	mux := chi.NewRouter()

	// Add regular routes
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics.WritePrometheus(w, true)
	})
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Enable pprof if requested
	if c.opts.EnablePprof {
		mux.Mount("/debug", middleware.Profiler())
	}

	metricsServer := &http.Server{
		Addr:              c.opts.MetricsListenAddr,
		ReadHeaderTimeout: 5 * time.Second,
		Handler:           mux,
	}
	go func() {
		c.log.Infow("Starting metrics server", "listenAddr", c.opts.MetricsListenAddr, "pprofEnabled", c.opts.EnablePprof)
		err := metricsServer.ListenAndServe()
		if err != nil {
			c.log.Fatal("Failed to start metrics server", zap.Error(err))
		}
	}()
}

// Shutdown stops the collector and flush all pending transactions
// func (c *Collector) Shutdown() {
// 	if c.processor == nil {
// 		return
// 	}
// 	c.processor.Shutdown()
// }
