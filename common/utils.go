// Package common contains common functions and variables used by various scripts and services
package common

import (
	"runtime"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var Printer = message.NewPrinter(language.English)

func GetMemUsageMb() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / 1024 / 1024
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s := Printer.Sprintf("Alloc = %d MiB, tTotalAlloc = %d MiB, Sys = %d MiB, tNumGC = %d", m.Alloc/1024/1024, m.TotalAlloc/1024/1024, m.Sys/1024/1024, m.NumGC)
	log.Info(s)
}

func RLPDecode(rlpBytes []byte) (*types.Transaction, error) {
	var tx types.Transaction
	err := rlp.DecodeBytes(rlpBytes, &tx)
	if err != nil {
		err = tx.UnmarshalBinary(rlpBytes)
	}
	return &tx, err
}

func RLPStringToTx(rlpHex string) (*types.Transaction, error) {
	rawtx, err := hexutil.Decode(rlpHex)
	if err != nil {
		return nil, err
	}
	return RLPDecode(rawtx)
}

func TxToRLPString(tx *types.Transaction) (string, error) {
	b, err := tx.MarshalBinary()
	if err != nil {
		return "", err
	}
	return hexutil.Encode(b), nil
}
