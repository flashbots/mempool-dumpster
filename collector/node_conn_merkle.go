package collector

// Plug into merkle as mempool data source (via websocket stream):
// https://docs.merkle.io/transaction-network/what-is-transaction-network

import (
	"time"

	"github.com/flashbots/mempool-dumpster/common"
	"github.com/merkle3/merkle-sdk-go/merkle"
	"go.uber.org/zap"
)

type MerkleNodeOpts struct {
	TxC       chan TxIn
	Log       *zap.SugaredLogger
	APIKey    string
	SourceTag string // optional override, default: "merkle"
}

type MerkleNodeConection struct {
	log        *zap.SugaredLogger
	sdk        *merkle.MerkleSDK
	srcTag     string
	txC        chan TxIn
	backoffSec int
}

func NewMerkleNodeConnection(opts MerkleNodeOpts) *MerkleNodeConection {
	srcTag := opts.SourceTag
	if srcTag == "" {
		srcTag = common.SourceTagMerkle
	}

	sdk := merkle.New()

	sdk.SetApiKey(opts.APIKey)

	return &MerkleNodeConection{
		log:        opts.Log.With("src", srcTag),
		sdk:        sdk,
		srcTag:     srcTag,
		txC:        opts.TxC,
		backoffSec: initialBackoffSec,
	}
}

func (nc *MerkleNodeConection) Start() {
	nc.connect()
}

func (nc *MerkleNodeConection) reconnect() {
	backoffDuration := time.Duration(nc.backoffSec) * time.Second
	nc.log.Infof("reconnecting to %s in %s sec ...", nc.srcTag, backoffDuration.String())
	time.Sleep(backoffDuration)

	// increase backoff timeout for next try
	nc.backoffSec *= 2
	if nc.backoffSec > maxBackoffSec {
		nc.backoffSec = maxBackoffSec
	}

	nc.connect()
}

func (nc *MerkleNodeConection) connect() {
	nc.log.Info("connecting...")
	txs, err := nc.sdk.Transactions().Stream(merkle.EthereumMainnet)

	for {
		select {
		case e := <-err:
			nc.log.Errorw("merkle subscription error", "error", e)
			nc.reconnect()

		case tx := <-txs:
			nc.txC <- TxIn{
				T:      time.Now().UTC(),
				Tx:     tx,
				Source: nc.srcTag,
			}
		}
	}
}
