package collector

// Plug into Chainbound fiber as mempool data source (via websocket stream):
// https://fiber.chainbound.io/docs/usage/getting-started/

import (
	"time"

	"github.com/ethereum/go-ethereum/core/types"
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
	log    *zap.SugaredLogger
	sdk    *merkle.MerkleSDK
	srcTag string
	txC    chan TxIn
}

func NewMerkleNodeConnection(opts MerkleNodeOpts) *MerkleNodeConection {
	srcTag := opts.SourceTag
	if srcTag == "" {
		srcTag = common.SourceTagMerkle
	}

	sdk := merkle.New()

	sdk.SetApiKey(opts.APIKey)

	return &MerkleNodeConection{
		log:    opts.Log.With("src", srcTag),
		sdk:    sdk,
		srcTag: srcTag,
		txC:    opts.TxC,
	}
}

func (cbc *MerkleNodeConection) Start() {
	cbc.log.Debug("merkle stream starting...")
	go cbc.connect()
	cbc.log.Error("merkle stream closed")
}

func (cbc *MerkleNodeConection) connect() {
	cbc.log.Infow("connecting...")

	txs, err := cbc.sdk.Transactions().Stream(merkle.EthereumMainnet) // pass a chain id

	for {
		select {
		case e := <-err:
			cbc.log.Errorw("merkle subscription error", "error", e)
		case _tx := <-txs:
			// process the transaction
			go func(tx *types.Transaction) {
				cbc.txC <- TxIn{
					T:      time.Now().UTC(),
					Tx:     tx,
					Source: cbc.srcTag,
				}
			}(_tx)
		}
	}
}
