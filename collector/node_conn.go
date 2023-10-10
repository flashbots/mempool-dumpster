package collector

import (
	"context"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethclient/gethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/zap"
)

type NodeConnection struct {
	log        *zap.SugaredLogger
	uri        string
	uriTag     string // identifier of tx source (i.e. "infura", "alchemy", "ws://localhost:8546")
	txC        chan TxIn
	isAlchemy  bool
	backoffSec int
}

func NewNodeConnection(log *zap.SugaredLogger, nodeURI string, txC chan TxIn) *NodeConnection {
	srcAlias := common.TxSourcName(nodeURI)
	return &NodeConnection{
		log:        log.With("src", srcAlias),
		uri:        nodeURI,
		uriTag:     srcAlias,
		txC:        txC,
		isAlchemy:  strings.Contains(nodeURI, "alchemy.com/"),
		backoffSec: initialBackoffSec,
	}
}

func (nc *NodeConnection) StartInBackground() {
	go nc.connect()
}

func (nc *NodeConnection) reconnect() {
	backoffDuration := time.Duration(nc.backoffSec) * time.Second
	nc.log.Infof("reconnecting in %s sec ...", backoffDuration.String())
	time.Sleep(backoffDuration)

	// increase backoff timeout for next try
	nc.backoffSec *= 2
	if nc.backoffSec > maxBackoffSec {
		nc.backoffSec = maxBackoffSec
	}

	nc.connect()
}

func (nc *NodeConnection) connect() {
	var err error
	var sub *rpc.ClientSubscription
	localC := make(chan *types.Transaction)

	if nc.isAlchemy {
		sub, err = nc.connectAlchemy(localC)
	} else {
		sub, err = nc.connectGeneric(localC)
	}

	if err != nil {
		nc.log.Errorw("failed to connect, reconnecting in a bit...", "error", err)
		go nc.reconnect()
		return
	}

	for {
		select {
		case err := <-sub.Err():
			nc.log.Errorw("subscription error, reconnecting...", "error", err)
			go nc.reconnect()
		case tx := <-localC:
			nc.txC <- TxIn{time.Now().UTC(), tx, nc.uriTag}
		}
	}
}

func (nc *NodeConnection) connectGeneric(txC chan *types.Transaction) (*rpc.ClientSubscription, error) {
	nc.log.Infow("connecting...", "uri", nc.uri)
	rpcClient, err := rpc.Dial(nc.uri)
	if err != nil {
		return nil, err
	}

	sub, err := gethclient.New(rpcClient).SubscribeFullPendingTransactions(context.Background(), txC)
	if err != nil {
		return nil, err
	}

	nc.log.Infow("connection successful", "uri", nc.uri)
	return sub, nil
}

// connectAlchemy connects to Alchemy's pendingTransactions subscription (warning -- burns _a lot_ of CU credits)
func (nc *NodeConnection) connectAlchemy(txC chan *types.Transaction) (*rpc.ClientSubscription, error) {
	nc.log.Infow("connecting...", "uri", nc.uri)
	client, err := ethclient.Dial(nc.uri)
	if err != nil {
		return nil, err
	}

	sub, err := client.Client().Subscribe(context.Background(), "eth", txC, "alchemy_pendingTransactions")
	if err != nil {
		return nil, err
	}

	nc.log.Infow("connection successful", "uri", nc.uri)
	return sub, nil
}
