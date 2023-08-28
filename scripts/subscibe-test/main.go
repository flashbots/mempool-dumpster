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
	mainGeneric()
	_, _, _ = pcheck, mainRaw, mainAlchemy //nolint:dogsled
}

func mainGeneric() {
	txC := make(chan collector.TxIn)
	log := common.GetLogger(true, false)
	nc := collector.NewNodeConnection(log, url, txC)
	go nc.Start()
	for tx := range txC {
		log.Infow("received tx", "tx", tx.Tx.Hash())
	}
}

func mainRaw() {
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

// alchemy: https://docs.alchemy.com/reference/newpendingtransactions https://docs.alchemy.com/reference/alchemy-pendingtransactions
func mainAlchemy() {
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
