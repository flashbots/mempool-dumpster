package collector

// Plug into bloxroute as mempool data source (via websocket stream)
//
// bloXroute needs an API key from "professional" plan or above
// - https://docs.bloxroute.com/streams/newtxs-and-pendingtxs

import (
	"context"
	"encoding/hex"
	"strings"
	"time"

	bloxroute "github.com/bloXroute-Labs/bloxroute-sdk-go"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/zap"
)

type BlxNodeOpts struct {
	Log        *zap.SugaredLogger
	AuthHeader string
	URL        string // optional override, default: blxDefaultURL
	SourceTag  string // optional override, default: "blx" (common.BloxrouteTag)
}

type BlxNodeConnection struct {
	log    *zap.SugaredLogger
	srcTag string
	txC    chan TxIn
	c      *bloxroute.Client
}

func NewBlxNodeConnection(opts BlxNodeOpts, txC chan TxIn) *BlxNodeConnection {
	log := opts.Log
	if opts.URL == "" {
		opts.URL = blxDefaultURL
	}

	srcTag := opts.SourceTag
	if srcTag == "" {
		srcTag = common.SourceTagBloxroute
	}

	bloxRouteCfg := &bloxroute.Config{
		AuthHeader: opts.AuthHeader,
	}

	// There are three options from Bloxroute
	// First, Cloud-API with endpoint: https://docs.bloxroute.com/introduction/cloud-api-ips
	// Second, with the gateway, we have two options: websocket and gRPC
	switch opts.URL {
	case blxCloudAPIWsURL:
		bloxRouteCfg.WSCloudAPIURL = opts.URL
	default:
		if common.IsWebsocketProtocol(opts.URL) {
			bloxRouteCfg.WSGatewayURL = opts.URL
		} else {
			bloxRouteCfg.GRPCGatewayURL = opts.URL
		}
	}

	c, err := bloxroute.NewClient(context.Background(), bloxRouteCfg)
	if err != nil {
		log.Fatalw("Failed to init a new Bloxroute client", "err", err)
	}

	return &BlxNodeConnection{
		log:    opts.Log.With("src", srcTag),
		srcTag: srcTag,
		txC:    txC,
		c:      c,
	}
}

func (nc *BlxNodeConnection) Start() {
	err := nc.c.OnNewTx(context.Background(), &bloxroute.NewTxParams{
		Include: []string{"raw_tx"},
	}, func(ctx context.Context, err error, result *bloxroute.NewTxNotification) {
		if err != nil {
			nc.log.Errorw("Failed to execute the callback function", "err", err)
			return
		}
		if result == nil || result.RawTx == "" {
			nc.log.Errorw("Transaction from Bloxroute is empty")
			return
		}

		binaryTx, err := hex.DecodeString(strings.TrimPrefix(result.RawTx, "0x"))
		if err != nil {
			nc.log.Errorw("Failed to decode raw tx", "err", err)
			return
		}

		var tx types.Transaction
		err = tx.UnmarshalBinary(binaryTx)
		if err != nil {
			nc.log.Errorw("Failed to unmarshal tx", "error", err, "rlp", result.RawTx)
			return
		}

		nc.txC <- TxIn{time.Now().UTC(), &tx, nc.srcTag}
	})
	if err != nil {
		err := nc.c.UnsubscribeFromNewTxs()
		if err != nil {
			nc.log.Fatalw("Failed to unsubscribe the Bloxroute data feed")
		}
		nc.log.Fatalw("Failed to listen to new txs", "err", err)
	}
}
