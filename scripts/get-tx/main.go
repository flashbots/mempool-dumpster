package main

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/rpc"
)

func main() {
	client, err := rpc.Dial("http://localhost:8545")
	if err != nil {
		panic(err)
	}

	var rawtx string
	err = client.CallContext(context.Background(), &rawtx, "eth_getRawTransactionByHash", "0xdb0dafc982622a962ec33442dbc154105db82ec39160ea0c01f0fe8bf29a341e")
	if err != nil {
		panic(err)
	}
	fmt.Println(rawtx)
}
