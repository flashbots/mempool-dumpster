package collector

// Plug into bloxroute or eden as mempool data source (via websocket stream)
//
// bloXroute needs an API key from "professional" plan or above
// - https://docs.bloxroute.com/streams/newtxs-and-pendingtxs
//
// eden: https://docs.edennetwork.io/eden-rpc/speed-rpc

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type BlxNodeOpts struct {
	Log        *zap.SugaredLogger
	AuthHeader string
	IsEden     bool
	URL        string // optional override, default: blxDefaultURL
	SourceTag  string // optional override, default: "blx" (common.BloxrouteTag)
}

type BlxNodeConnection struct {
	log        *zap.SugaredLogger
	authHeader string
	url        string
	isEden     bool
	srcTag     string
	txC        chan TxIn
	backoffSec int
}

func NewBlxNodeConnection(opts BlxNodeOpts, txC chan TxIn) *BlxNodeConnection {
	url := opts.URL
	if url == "" {
		url = blxDefaultURL
	}

	srcTag := opts.SourceTag
	if srcTag == "" {
		srcTag = common.BloxrouteTag
	}

	return &BlxNodeConnection{
		log:        opts.Log.With("src", srcTag),
		authHeader: opts.AuthHeader,
		url:        url,
		isEden:     opts.IsEden,
		srcTag:     srcTag,
		txC:        txC,
		backoffSec: initialBackoffSec,
	}
}

func (nc *BlxNodeConnection) Start() {
	nc.connect()
}

func (nc *BlxNodeConnection) reconnect() {
	time.Sleep(time.Duration(nc.backoffSec) * time.Second)

	// increase backoff timeout
	nc.backoffSec *= 2
	if nc.backoffSec > maxBackoffSec {
		nc.backoffSec = maxBackoffSec
	}

	nc.connect()
}

func (nc *BlxNodeConnection) connect() {
	nc.log.Infow("connecting...", "uri", nc.url)
	dialer := websocket.DefaultDialer
	wsSubscriber, resp, err := dialer.Dial(nc.url, http.Header{"Authorization": []string{nc.authHeader}})
	if err != nil {
		nc.log.Errorw("failed to connect to bloxroute", "error", err)
		go nc.reconnect()
		return
	}
	defer wsSubscriber.Close()
	defer resp.Body.Close()

	subRequest := `{"id": 1, "method": "subscribe", "params": ["newTxs", {"include": ["raw_tx"]}]}`
	if nc.isEden {
		subRequest = `{"jsonrpc": "2.0", "id": 1, "method": "subscribe", "params": ["rawTxs"]}`
	}
	err = wsSubscriber.WriteMessage(websocket.TextMessage, []byte(subRequest))
	if err != nil {
		nc.log.Errorw("failed to subscribe to bloxroute", "error", err)
		go nc.reconnect()
		return
	}

	nc.log.Infow("connection successful", "uri", nc.url)
	nc.backoffSec = initialBackoffSec // reset backoff timeout

	for {
		_, nextNotification, err := wsSubscriber.ReadMessage()
		if err != nil {
			// Handle websocket errors, by closing and reconnecting. Errors seen previously:
			// - "websocket: close 1006 (abnormal closure): unexpected EOF"
			if strings.Contains(err.Error(), "failed parsing the authorization header") {
				nc.log.Errorw("invalid bloxroute auth header", "error", err)
			} else {
				nc.log.Errorw("failed to read message, reconnecting", "error", err)
			}

			go nc.reconnect()
			return
		}

		// fmt.Println("got message", string(nextNotification))
		var rlp string
		if nc.isEden {
			var txMsg common.EdenRawTxMsg
			err = json.Unmarshal(nextNotification, &txMsg)
			if err != nil {
				nc.log.Errorw("failed to unmarshal message", "error", err)
				continue
			}
			rlp = txMsg.Params.Result.RLP
		} else {
			var txMsg common.BlxRawTxMsg
			err = json.Unmarshal(nextNotification, &txMsg)
			if err != nil {
				nc.log.Errorw("failed to unmarshal message", "error", err)
				continue
			}
			rlp = txMsg.Params.Result.RawTx
		}

		if len(rlp) == 0 {
			continue
		}

		// nc.log.Debugw("got tx", "rawtx", rlp)
		rawtx, err := hex.DecodeString(strings.TrimPrefix(rlp, "0x"))
		if err != nil {
			nc.log.Errorw("failed to decode raw tx", "error", err)
			continue
		}

		var tx types.Transaction
		err = tx.UnmarshalBinary(rawtx)
		if err != nil {
			nc.log.Errorw("failed to unmarshal tx", "error", err, "rlp", rlp)
			continue
		}

		nc.txC <- TxIn{time.Now().UTC(), &tx, nc.srcTag}
	}
}
