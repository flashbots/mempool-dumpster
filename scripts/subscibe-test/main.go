package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethclient/gethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/flashbots/mempool-dumpster/collector"
	"github.com/flashbots/mempool-dumpster/common"
)

// var url = "ws://localhost:8546"
var url = os.Getenv("URL")

func pcheck(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	MainEden()
	// MainBlx()
	// MainChainbound()
}

func MainGeneric() {
	txC := make(chan common.TxIn)
	log := common.GetLogger(true, false)
	nc := collector.NewNodeConnection(log, url, txC)
	nc.StartInBackground()
	for tx := range txC {
		log.Infow("received tx", "tx", tx.Tx.Hash())
	}
}

func MainBlx() {
	txC := make(chan common.TxIn)
	log := common.GetLogger(true, false)
	token, url := common.GetAuthTokenAndURL(os.Getenv("BLX_AUTH"))
	blxOpts := collector.BlxNodeOpts{
		TxC:        txC,
		Log:        log,
		AuthHeader: token,
		URL:        url,
	}
	nc := collector.NewBlxNodeConnectionGRPC(blxOpts)
	go nc.Start()
	for tx := range txC {
		log.Infow("received tx", "tx", tx.Tx.Hash(), "src", tx.Source)
	}
}

func MainEden() {
	txC := make(chan common.TxIn)
	log := common.GetLogger(true, false)
	token, url := common.GetAuthTokenAndURL(os.Getenv("EDEN_AUTH"))
	blxOpts := collector.EdenNodeOpts{
		TxC:        txC,
		Log:        log,
		AuthHeader: token,
		URL:        url,
	}
	nc := collector.NewEdenNodeConnection(blxOpts)
	go nc.Start()
	for tx := range txC {
		log.Infow("received tx", "tx", tx.Tx.Hash(), "src", tx.Source)
	}
}

func MainChainbound() {
	txC := make(chan common.TxIn)
	log := common.GetLogger(true, false)
	opts := collector.ChainboundNodeOpts{ //nolint:exhaustruct
		TxC:    txC,
		Log:    log,
		APIKey: os.Getenv("CHAINBOUND_AUTH"),
	}
	nc := collector.NewChainboundNodeConnection(opts)
	go nc.Start()
	for tx := range txC {
		log.Infow("received tx", "tx", tx.Tx.Hash(), "src", tx.Source)
	}
}

func MainRaw() {
	fmt.Println("connecting to node...", "uri", url)
	rpcClient, err := rpc.Dial(url)
	pcheck(err)

	txC := make(chan *types.Transaction)
	_, err = gethclient.New(rpcClient).SubscribeFullPendingTransactions(context.Background(), txC)
	pcheck(err)

	fmt.Println("connected")
	for tx := range txC {
		fmt.Println("tx", tx.Hash())
	}
}

// MainAlchemy uses alchemy: https://docs.alchemy.com/reference/newpendingtransactions https://docs.alchemy.com/reference/alchemy-pendingtransactions
func MainAlchemy() {
	fmt.Println("connecting to node...", "uri", url)
	txC := make(chan *types.Transaction)
	client, err := ethclient.Dial(url)
	pcheck(err)
	sub, err := client.Client().Subscribe(context.Background(), "eth", txC, "alchemy_pendingTransactions")
	pcheck(err)

	fmt.Println("connected")

	for {
		select {
		case err := <-sub.Err():
			panic(err)
		case tx := <-txC:
			fmt.Println(tx.Hash()) // pointer to event log
		}
	}
}
