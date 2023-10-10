package collector

// Plug into bloxroute as mempool data source (via websocket stream)
//
// bloXroute needs an API key from "professional" plan or above
// - https://docs.bloxroute.com/streams/newtxs-and-pendingtxs
//

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	pb "github.com/bloXroute-Labs/gateway/v2/protobuf"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BlxNodeOpts struct {
	TxC        chan TxIn
	Log        *zap.SugaredLogger
	AuthHeader string
	URL        string // optional override, default: blxDefaultURL
	SourceTag  string // optional override, default: "blx" (common.BloxrouteTag)
}

// startBloxrouteConnection starts a Websocket or gRPC subscription (depending on URL) in the background
func startBloxrouteConnection(opts BlxNodeOpts) {
	if common.IsWebsocketProtocol(opts.URL) {
		blxConn := NewBlxNodeConnection(opts)
		go blxConn.Start()
	} else {
		blxConn := NewBlxNodeConnectionGRPC(opts)
		go blxConn.Start()
	}
}

type BlxNodeConnection struct {
	log        *zap.SugaredLogger
	authHeader string
	url        string
	srcTag     string
	txC        chan TxIn
	backoffSec int
}

func NewBlxNodeConnection(opts BlxNodeOpts) *BlxNodeConnection {
	url := opts.URL
	if url == "" {
		url = blxDefaultURL
	}

	srcTag := opts.SourceTag
	if srcTag == "" {
		srcTag = common.SourceTagBloxroute
	}

	return &BlxNodeConnection{
		log:        opts.Log.With("src", srcTag),
		authHeader: opts.AuthHeader,
		url:        url,
		srcTag:     srcTag,
		txC:        opts.TxC,
		backoffSec: initialBackoffSec,
	}
}

func (nc *BlxNodeConnection) Start() {
	nc.connect()
}

func (nc *BlxNodeConnection) reconnect() {
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

//nolint:dupl
func (nc *BlxNodeConnection) connect() {
	nc.log.Infow("connecting...", "uri", nc.url)
	dialer := websocket.DefaultDialer
	wsSubscriber, resp, err := dialer.Dial(nc.url, http.Header{"Authorization": []string{nc.authHeader}})
	if err != nil {
		nc.log.Errorw("failed to connect to bloxroute, reconnecting in a bit...", "error", err)
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

			go nc.reconnect()
			return
		}

		// fmt.Println("got message", string(nextNotification))
		var txMsg common.BlxRawTxMsg
		err = json.Unmarshal(nextNotification, &txMsg)
		if err != nil {
			nc.log.Errorw("failed to unmarshal message", "error", err)
			continue
		}
		rlp := txMsg.Params.Result.RawTx

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

type BlxNodeConnectionGRPC struct {
	log        *zap.SugaredLogger
	authHeader string
	url        string
	srcTag     string
	txC        chan TxIn
	backoffSec int
}

func NewBlxNodeConnectionGRPC(opts BlxNodeOpts) *BlxNodeConnectionGRPC {
	url := opts.URL
	if url == "" {
		url = blxDefaultURL
	}

	return &BlxNodeConnectionGRPC{
		log:        opts.Log.With("src", common.SourceTagBloxroute),
		authHeader: opts.AuthHeader,
		url:        url,
		srcTag:     common.SourceTagBloxroute,
		txC:        opts.TxC,
		backoffSec: initialBackoffSec,
	}
}

func (nc *BlxNodeConnectionGRPC) Start() {
	nc.connect()
}

func (nc *BlxNodeConnectionGRPC) reconnect() {
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

func (nc *BlxNodeConnectionGRPC) connect() {
	nc.log.Infow("connecting...", "uri", nc.url)

	conn, err := grpc.Dial(nc.url, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithInitialWindowSize(common.GRPCWindowSize))
	if err != nil {
		nc.log.Errorw("failed to connect to bloxroute gRPC, reconnecting in a bit...", "error", err)
		go nc.reconnect()
		return
	}

	client := pb.NewGatewayClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.NewTxs(ctx, &pb.TxsRequest{ //nolint:exhaustruct
		AuthHeader: nc.authHeader,
	})
	if err != nil {
		nc.log.Errorw("failed to invoke NewTxs stream on bloxroute gRPC client", "error", err)
		go nc.reconnect()
		return
	}

	defer func() {
		if err := stream.CloseSend(); err != nil {
			nc.log.Errorw("failed to close bloxroute gRPC stream", "error", err)
		}
	}()

	nc.log.Infow("connection successful", "uri", nc.url)
	nc.backoffSec = initialBackoffSec // reset backoff timeout

	for {
		msg, err := stream.Recv()
		if err != nil {
			nc.log.Errorw("failed to read message from gRPC stream", "error", err)
			go nc.reconnect()
			return
		}

		for _, tx := range msg.GetTx() {
			rlp := tx.GetRawTx()

			var tx types.Transaction
			err = tx.UnmarshalBinary(rlp)
			if err != nil {
				nc.log.Errorw("failed to unmarshal tx", "error", err, "rlp", rlp)
				continue
			}

			nc.txC <- TxIn{time.Now().UTC(), &tx, nc.srcTag}
		}
	}
}
