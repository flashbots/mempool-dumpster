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
	txC        chan common.TxIn
	backoffSec int
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
		log:        opts.Log.With("src", srcTag),
		apiKey:     opts.APIKey,
		url:        url,
		srcTag:     srcTag,
		txC:        opts.TxC,
		backoffSec: initialBackoffSec,
	}
}

func (cbc *ChainboundNodeConnection) Start() {
	cbc.log.Debug("chainbound stream starting...")

	for {
		// (Re)create incoming-tx channel
		ch := make(chan *fiber.TransactionWithSender)

		// Fire off connect (will close fiberC on error)
		go cbc.connect(ch)

		// Forward transactions to collector
		for fiberTx := range ch {
			cbc.txC <- common.TxIn{
				T:      time.Now().UTC(),
				Tx:     fiberTx.Transaction,
				Source: cbc.srcTag,
			}
		}

		backoffDuration := time.Duration(cbc.backoffSec) * time.Second
		cbc.log.Infof("chainbound stream closed, reconnecting in %s sec ...", backoffDuration.String())
		time.Sleep(backoffDuration)
		cbc.backoffSec *= 2
		if cbc.backoffSec > maxBackoffSec {
			cbc.backoffSec = maxBackoffSec
		}
	}
}

func (cbc *ChainboundNodeConnection) connect(ch chan *fiber.TransactionWithSender) {
	cbc.log.Infow("connecting...", "uri", cbc.url)

	config := fiber.NewConfig().SetIdleTimeout(10 * time.Second).SetHealthCheckInterval(10 * time.Second)

	client := fiber.NewClientWithConfig(chainboundDefaultURL, cbc.apiKey, config)

	// Connect
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		cbc.log.Errorw("failed to connect to chainbound", "error", err)
		close(ch)
		return
	}

	// Only close the client when the connection was successful
	defer client.Close()

	cbc.log.Infow("connection successful", "uri", cbc.url)
	cbc.backoffSec = initialBackoffSec

	// First make a sink channel on which to receive the transactions
	// This is a blocking call, so it needs to run in a Goroutine
	err := client.SubscribeNewTxs(nil, ch)
	if err != nil {
		cbc.log.Errorw("chainbound subscription error", "error", err)
		close(ch)
		return
	}
}
