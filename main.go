package main

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient/gethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"log"
	"os"
	"sync"
	"time"
)

func handleHash(ctx context.Context, rpcClient *rpc.Client, hash common.Hash, wg *sync.WaitGroup) {
	defer wg.Done() // Signal that this goroutine is done when this function returns
	now := time.Now()
	var rawtx string
	b := backoff.NewExponentialBackOff()

	// the client might not know the raw tx yet. so we back off and retry (for up to 15 minutes).
	err := backoff.Retry(func() error {
		// TODO: use debug_getRawTransaction for reth
		err := rpcClient.CallContext(ctx, &rawtx, "eth_getRawTransactionByHash", hash)
		if err != nil {
			return backoff.Permanent(err)
		}
		if rawtx == "0x" {
			return fmt.Errorf("data not ready")
		}
		return nil
	}, b)
	if err != nil {
		// If there is still an error after the retries, we log it and continue
		log.Printf("Failed to get raw transaction for hash %s: %v\n", hash.String(), err)
		return
	}
	fmt.Printf("%d.%09d,%s,%s\n", now.Unix(), now.UnixNano() % 1e9, hash.String(), rawtx)
}


func main() {
        if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <node endpoint>\n", os.Args[0])
        }
	rpcClient, err := rpc.Dial(os.Args[1])
	if err != nil {
		log.Fatalln(err)
	}

	ctx := context.Background()
	hashes := make(chan common.Hash)
	_, err = gethclient.New(rpcClient).SubscribePendingTransactions(ctx, hashes)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		rpcClient.Close()
	}()

	var wg sync.WaitGroup // WaitGroup to wait for all goroutines to finish
	for hash := range hashes {
		wg.Add(1) // Increment WaitGroup counter
		go handleHash(ctx, rpcClient, hash, &wg) // Start a goroutine for each transaction
	}
	wg.Wait() // Wait for all goroutines to finish
}
