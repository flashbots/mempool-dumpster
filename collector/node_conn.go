package collector

import (
	"context"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethclient/gethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"go.uber.org/zap"
)

type NodeConnection struct {
	log       *zap.SugaredLogger
	uri       string
	txC       chan TxIn
	isAlchemy bool
}

func NewNodeConnection(log *zap.SugaredLogger, nodeURI string, txC chan TxIn) *NodeConnection {
	isAlchemy := strings.Contains(nodeURI, "alchemy.com/")
	if isAlchemy {
		log = log.With("conn", "alchemy")
	} else {
		log = log.With("conn", "generic")
	}

	return &NodeConnection{
		log:       log, //.With("module", "node_connection", "uri", nodeURI),
		uri:       nodeURI,
		txC:       txC,
		isAlchemy: isAlchemy,
	}
}

func (nc *NodeConnection) Start() {
	log := nc.log.With("uri", nc.uri)
	txC := make(chan *types.Transaction)

	sub, err := nc.connect(txC)
	if err != nil {
		log.Fatalln(err)
	}

	for {
		select {
		case err := <-sub.Err():
			log.Errorw("subscription error", "error", err)

			// reconnect
			for {
				log.Info("reconnecting...")
				sub, err = nc.connect(txC)
				if err == nil {
					log.Info("reconnected successfully")
					break
				}
				log.Errorw("failed to reconnect, retrying in a few seconds...", "error", err)
				time.Sleep(5 * time.Second)
			}
		case tx := <-txC:
			nc.txC <- TxIn{nc.uri, tx, time.Now().UTC()}
		}
	}
}

func (nc *NodeConnection) connect(txC chan *types.Transaction) (*rpc.ClientSubscription, error) {
	if nc.isAlchemy {
		return nc.connectAlchemy(txC)
	} else {
		return nc.connectGeneric(txC)
	}
}

func (nc *NodeConnection) connectGeneric(txC chan *types.Transaction) (*rpc.ClientSubscription, error) {
	nc.log.Infow("connecting to node...", "uri", nc.uri)
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

func (nc *NodeConnection) connectAlchemy(txC chan *types.Transaction) (*rpc.ClientSubscription, error) {
	nc.log.Infow("connecting to node...", "uri", nc.uri)
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
