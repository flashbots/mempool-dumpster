package collector

// Plug into bloxroute as mempool data source (via websocket stream):
// https://docs.bloxroute.com/streams/newtxs-and-pendingtxs
//
// Needs a bloXroute API key from "professional" plan or above

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

const (
	initialBackoffSec = 5
	maxBackoffSec     = 120
)

type BlxNodeOpts struct {
	Log        *zap.SugaredLogger
	AuthHeader string
	URL        string // optional override, default: blxDefaultURL
	SourceTag  string // optional override, default: "blx" (common.BloxrouteTag)
}

type BlxNodeConnection struct {
	log        *zap.SugaredLogger
	authHeader string
	url        string
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

			// TODO: exponential backoff?
			go nc.reconnect()
			return
		}

		var txMsg common.BlxRawTxMsg
		err = json.Unmarshal(nextNotification, &txMsg)
		if err != nil {
			nc.log.Errorw("failed to unmarshal message", "error", err)
			continue
		}

		if len(txMsg.Params.Result.RawTx) == 0 {
			continue
		}

		nc.log.Debugw("got blx tx", "rawtx", txMsg.Params.Result.RawTx)
		rawtx, err := hex.DecodeString(txMsg.Params.Result.RawTx[2:])
		if err != nil {
			nc.log.Errorw("failed to decode raw tx", "error", err)
			continue
		}

		var tx types.Transaction
		err = tx.UnmarshalBinary(rawtx)
		if err != nil {
			nc.log.Errorw("failed to unmarshal tx", "error", err)
			continue
		}

		nc.txC <- TxIn{time.Now().UTC(), &tx, nc.srcTag}
	}
}
