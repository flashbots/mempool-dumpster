package collector

// Plug into eden as mempool data source (via websocket stream)
//
// eden: https://docs.edennetwork.io/eden-mempool-streaming-service/overview

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	pb "github.com/eden-network/mempool-service/protobuf"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type EdenNodeOpts struct {
	TxC        chan common.TxIn
	Log        *zap.SugaredLogger
	AuthHeader string
	URL        string // optional override, default: edenDefaultURL
	SourceTag  string // optional override, default: "eden" (common.SourceTagEden)
}

// startEdenConnection starts a Websocket or gRPC subscription (depending on URL) in the background
func startEdenConnection(opts EdenNodeOpts) {
	if common.IsWebsocketProtocol(opts.URL) {
		edenConn := NewEdenNodeConnection(opts)
		go edenConn.Start()
	} else {
		edenConn := NewEdenNodeConnectionGRPC(opts)
		go edenConn.Start()
	}
}

type EdenNodeConnection struct {
	log        *zap.SugaredLogger
	authHeader string
	url        string
	srcTag     string
	txC        chan common.TxIn
	backoffSec int
}

func NewEdenNodeConnection(opts EdenNodeOpts) *EdenNodeConnection {
	url := opts.URL
	if url == "" {
		url = edenDefaultURL
	}

	srcTag := opts.SourceTag
	if srcTag == "" {
		srcTag = common.SourceTagEden
	}

	return &EdenNodeConnection{
		log:        opts.Log.With("src", srcTag),
		authHeader: opts.AuthHeader,
		url:        url,
		srcTag:     srcTag,
		txC:        opts.TxC,
		backoffSec: initialBackoffSec,
	}
}

func (nc *EdenNodeConnection) Start() {
	nc.connect()
}

func (nc *EdenNodeConnection) reconnect() {
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
func (nc *EdenNodeConnection) connect() {
	nc.log.Infow("connecting...", "uri", nc.url)
	dialer := websocket.DefaultDialer
	wsSubscriber, resp, err := dialer.Dial(nc.url, http.Header{"Authorization": []string{nc.authHeader}})
	if err != nil {
		nc.log.Errorw("failed to connect to eden, reconnecting in a bit...", "error", err)
		go nc.reconnect()
		return
	}
	defer wsSubscriber.Close()
	defer resp.Body.Close()

	subRequest := `{"jsonrpc": "2.0", "id": 1, "method": "subscribe", "params": ["rawTxs"]}`
	err = wsSubscriber.WriteMessage(websocket.TextMessage, []byte(subRequest))
	if err != nil {
		nc.log.Errorw("failed to subscribe to eden", "error", err)
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
				nc.log.Errorw("invalid eden auth header", "error", err)
			} else {
				nc.log.Errorw("failed to read message, reconnecting", "error", err)
			}

			go nc.reconnect()
			return
		}

		// fmt.Println("got message", string(nextNotification))
		var txMsg common.EdenRawTxMsg
		err = json.Unmarshal(nextNotification, &txMsg) //nolint:musttag
		if err != nil {
			nc.log.Errorw("failed to unmarshal message", "error", err)
			continue
		}

		rlp := txMsg.Params.Result.RLP
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

		nc.txC <- common.TxIn{
			T:      time.Now().UTC(),
			Tx:     &tx,
			Source: nc.srcTag,
		}
	}
}

type EdenNodeConnectionGRPC struct {
	log        *zap.SugaredLogger
	authHeader string
	url        string
	srcTag     string
	txC        chan common.TxIn
	backoffSec int
}

func NewEdenNodeConnectionGRPC(opts EdenNodeOpts) *EdenNodeConnectionGRPC {
	url := opts.URL
	if url == "" {
		url = edenDefaultURL
	}

	return &EdenNodeConnectionGRPC{
		log:        opts.Log.With("src", common.SourceTagEden),
		authHeader: opts.AuthHeader,
		url:        url,
		srcTag:     common.SourceTagEden,
		txC:        opts.TxC,
		backoffSec: initialBackoffSec,
	}
}

func (nc *EdenNodeConnectionGRPC) Start() {
	nc.connect()
}

func (nc *EdenNodeConnectionGRPC) reconnect() {
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

func (nc *EdenNodeConnectionGRPC) connect() {
	nc.log.Infow("connecting...", "uri", nc.url)

	conn, err := grpc.Dial(nc.url, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithInitialWindowSize(common.GRPCWindowSize))
	if err != nil {
		nc.log.Errorw("failed to connect to eden gRPC, reconnecting in a bit...", "error", err)
		go nc.reconnect()
		return
	}

	client := pb.NewStreamServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.StreamRawTransactions(ctx, &pb.StreamRawTransactionsRequest{ //nolint:exhaustruct
		AuthHeader: nc.authHeader,
	})
	if err != nil {
		nc.log.Errorw("failed to invoke StreamRawTransactions stream on eden gRPC client", "error", err)
		go nc.reconnect()
		return
	}

	defer func() {
		if err := stream.CloseSend(); err != nil {
			nc.log.Errorw("failed to close eden gRPC stream", "error", err)
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

		rlp := msg.GetRlp()

		var tx types.Transaction
		err = tx.UnmarshalBinary(rlp)
		if err != nil {
			nc.log.Errorw("failed to unmarshal tx", "error", err, "rlp", rlp)
			continue
		}

		nc.txC <- common.TxIn{
			T:      time.Now().UTC(),
			Tx:     &tx,
			Source: nc.srcTag,
		}
	}
}
