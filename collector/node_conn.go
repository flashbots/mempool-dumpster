package collector

import (
	"context"
	"log"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient/gethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"go.uber.org/zap"
)

type NodeConnection struct {
	log *zap.SugaredLogger
	uri string
	txC chan TxIn
}

func NewNodeConnection(log *zap.SugaredLogger, nodeURI string, txC chan TxIn) *NodeConnection {
	return &NodeConnection{
		log: log, //.With("module", "node_connection", "uri", nodeURI),
		uri: nodeURI,
		txC: txC,
	}
}

func (nc *NodeConnection) Start() {
	nc.log.Infow("Connecting to node...", "uri", nc.uri)

	rpcClient, err := rpc.Dial(nc.uri)
	if err != nil {
		log.Fatalln(err)
	}

	txC := make(chan *types.Transaction)
	_, err = gethclient.New(rpcClient).SubscribeFullPendingTransactions(context.Background(), txC)
	if err != nil {
		log.Fatalln(err)
	}

	for tx := range txC {
		nc.txC <- TxIn{nc.uri, tx, time.Now().UTC()}
	}
}
