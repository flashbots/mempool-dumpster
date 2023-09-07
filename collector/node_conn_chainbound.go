package collector

// Plug into Chainbound fiber as mempool data source (via websocket stream):
// https://fiber.chainbound.io/docs/usage/getting-started/

import (
	"context"
	"time"

	fiber "github.com/chainbound/fiber-go"
	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/zap"
)

type ChainboundNodeOpts struct {
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
	fiberC     chan *fiber.Transaction
	txC        chan TxIn
	backoffSec int
}

func NewChainboundNodeConnection(opts ChainboundNodeOpts, txC chan TxIn) *ChainboundNodeConnection {
	url := opts.URL
	if url == "" {
		url = chainboundDefaultURL
	}

	srcTag := opts.SourceTag
	if srcTag == "" {
		srcTag = common.ChainboundTag
	}

	return &ChainboundNodeConnection{
		log:        opts.Log.With("src", srcTag),
		apiKey:     opts.APIKey,
		url:        url,
		srcTag:     srcTag,
		fiberC:     make(chan *fiber.Transaction),
		txC:        txC,
		backoffSec: initialBackoffSec,
	}
}

func (cbc *ChainboundNodeConnection) Start() {
	go cbc.connect()

	for fiberTx := range cbc.fiberC {
		nativeTx := fiberTx.ToNative()
		cbc.txC <- TxIn{time.Now().UTC(), nativeTx, cbc.srcTag}
	}
}

func (cbc *ChainboundNodeConnection) reconnect() {
	time.Sleep(time.Duration(cbc.backoffSec) * time.Second)

	// increase backoff timeout
	cbc.backoffSec *= 2
	if cbc.backoffSec > maxBackoffSec {
		cbc.backoffSec = maxBackoffSec
	}

	cbc.connect()
}

func (cbc *ChainboundNodeConnection) connect() {
	cbc.log.Infow("connecting...", "uri", cbc.url)

	client := fiber.NewClient(chainboundDefaultURL, cbc.apiKey)
	defer client.Close()

	// Connect
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		cbc.log.Errorw("failed to connect to chainbound", "error", err)
		go cbc.reconnect()
		return
	}

	cbc.log.Infow("connection successful", "uri", cbc.url)
	cbc.backoffSec = initialBackoffSec // reset backoff timeout

	// First make a sink channel on which to receive the transactions
	// This is a blocking call, so it needs to run in a Goroutine
	if err := client.SubscribeNewTxs(nil, cbc.fiberC); err != nil {
		cbc.log.Errorw("failed to connect to chainbound", "error", err)
		go cbc.reconnect()
		return
	}
}
