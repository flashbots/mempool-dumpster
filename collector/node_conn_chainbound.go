package collector

// Plug into Chainbound fiber as mempool data source (via websocket stream):
// https://fiber.chainbound.io/docs/usage/getting-started/

import (
	"context"
	"sync"
	"time"

	fiber "github.com/chainbound/fiber-go"
	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/zap"
)

const watchdogTimeout = 1 * time.Minute

type ChainboundNodeOpts struct {
	TxC       chan common.TxIn
	Log       *zap.SugaredLogger
	APIKey    string
	URL       string // optional override, default: ChainboundDefaultURL
	SourceTag string // optional override, default: "Chainbound"
}

type ChainboundNodeConnection struct {
	log        *zap.SugaredLogger
	apiKey     string
	url        string
	srcTag     string
	fiberC     chan *fiber.TransactionWithSender
	txC        chan common.TxIn
	backoffSec int

	watchdog      *time.Timer
	watchdogMu    sync.Mutex
	watchdogStopC chan struct{}
}

func NewChainboundNodeConnection(opts ChainboundNodeOpts) *ChainboundNodeConnection {
	url := opts.URL
	if url == "" {
		url = chainboundDefaultURL
	}

	srcTag := opts.SourceTag
	if srcTag == "" {
		srcTag = common.SourceTagChainbound
	}

	return &ChainboundNodeConnection{
		log:           opts.Log.With("src", srcTag),
		apiKey:        opts.APIKey,
		url:           url,
		srcTag:        srcTag,
		fiberC:        make(chan *fiber.TransactionWithSender),
		txC:           opts.TxC,
		backoffSec:    initialBackoffSec,
		watchdogStopC: make(chan struct{}),
	}
}

func (cbc *ChainboundNodeConnection) resetWatchdog() {
	cbc.watchdogMu.Lock()
	defer cbc.watchdogMu.Unlock()

	if cbc.watchdog == nil {
		cbc.watchdog = time.NewTimer(watchdogTimeout)
	} else {
		if !cbc.watchdog.Stop() {
			select {
			case <-cbc.watchdog.C:
			default:
			}
		}
		cbc.watchdog.Reset(watchdogTimeout)
	}
}

func (cbc *ChainboundNodeConnection) startWatchdog() {
	cbc.resetWatchdog()

	go func() {
		for {
			select {
			case <-cbc.watchdog.C:
				cbc.log.Warn("watchdog timeout: no transactions received for 1 minute, reconnecting...")
				cbc.reconnect()
				return
			case <-cbc.watchdogStopC:
				cbc.log.Debug("watchdog stopped")
				return
			}
		}
	}()
}

func (cbc *ChainboundNodeConnection) shutdownWatchdog() {
	cbc.watchdogMu.Lock()
	defer cbc.watchdogMu.Unlock()

	if cbc.watchdog != nil {
		cbc.watchdog.Stop()
	}

	select {
	case cbc.watchdogStopC <- struct{}{}:
	default:
	}
}

func (cbc *ChainboundNodeConnection) Start() {
	cbc.log.Debug("chainbound stream starting...")
	cbc.fiberC = make(chan *fiber.TransactionWithSender)
	cbc.watchdogStopC = make(chan struct{})
	go cbc.connect()
	cbc.startWatchdog()

	for fiberTx := range cbc.fiberC {
		cbc.resetWatchdog()
		cbc.txC <- common.TxIn{
			T:      time.Now().UTC(),
			Tx:     fiberTx.Transaction,
			Source: cbc.srcTag,
		}
	}

	cbc.log.Error("chainbound stream closed")
}

func (cbc *ChainboundNodeConnection) reconnect() {
	cbc.shutdownWatchdog()

	// Close the existing fiber channel to break out of the loop in Start()
	close(cbc.fiberC)

	backoffDuration := time.Duration(cbc.backoffSec) * time.Second
	cbc.log.Infof("reconnecting to chainbound in %s sec ...", backoffDuration.String())
	time.Sleep(backoffDuration)

	// increase backoff timeout for next try
	cbc.backoffSec *= 2
	if cbc.backoffSec > maxBackoffSec {
		cbc.backoffSec = maxBackoffSec
	}

	cbc.Start()
}

func (cbc *ChainboundNodeConnection) connect() {
	cbc.log.Infow("connecting...", "uri", cbc.url)

	config := fiber.NewConfig().SetIdleTimeout(10 * time.Second).SetHealthCheckInterval(10 * time.Second)

	client := fiber.NewClientWithConfig(chainboundDefaultURL, cbc.apiKey, config)

	// Connect
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		cbc.log.Errorw("failed to connect to chainbound, reconnecting in a bit...", "error", err)
		go cbc.reconnect()
		return
	}

	// Only close the client when the connection was successful
	defer client.Close()

	cbc.log.Infow("connection successful", "uri", cbc.url)
	cbc.backoffSec = initialBackoffSec

	// First make a sink channel on which to receive the transactions
	// This is a blocking call, so it needs to run in a Goroutine
	err := client.SubscribeNewTxs(nil, cbc.fiberC)
	if err != nil {
		cbc.log.Errorw("chainbound subscription error", "error", err)
		go cbc.reconnect()
		return
	}
}
