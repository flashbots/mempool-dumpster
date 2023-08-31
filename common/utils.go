// Package common contains common functions and variables used by various scripts and services
package common

import (
	"os"
	"runtime"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var Printer = message.NewPrinter(language.English)

func GetEnv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

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

func IntDiffPercentFmt(a, b int) string {
	diff := float64(a) / float64(b)
	return Printer.Sprintf("%.2f%%", diff*100)
}

func Int64DiffPercentFmt(a, b int64) string {
	diff := float64(a) / float64(b)
	return Printer.Sprintf("%.2f%%", diff*100)
}

func SourceAliasesFromEnv() map[string]string {
	aliases := make(map[string]string)
	aliasesRaw := os.Getenv("SRC_ALIASES") // format: alias=url,alias=url
	if aliasesRaw != "" {
		entries := strings.Split(aliasesRaw, ",")
		for _, entry := range entries {
			parts := strings.Split(entry, "=")
			if len(parts) != 2 {
				continue
			}
			aliases[parts[1]] = parts[0]
		}
	}
	return aliases
}
