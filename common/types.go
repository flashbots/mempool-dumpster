package common

import (
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

type TxIn struct {
	T      time.Time
	Tx     *types.Transaction
	Source string
}

type BlxRawTxMsg struct {
	Params struct {
		Result struct {
			RawTx string
		}
	}
}

type EdenRawTxMsg struct {
	Params struct {
		Result struct {
			RLP string
		}
	}
}

type SourceComp struct {
	Source    string
	Reference string
}

func NewSourceComps(args []string) (srcComp []SourceComp) {
	srcComp = make([]SourceComp, 0)

	for _, entries := range args {
		parts := strings.Split(entries, "-")
		if len(parts) != 2 {
			continue
		}
		srcComp = append(srcComp, SourceComp{
			Source:    parts[0],
			Reference: parts[1],
		})
	}

	return
}

var DefaultSourceComparisons = []SourceComp{
	{SourceTagBloxroute, SourceTagLocal},
	{SourceTagChainbound, SourceTagLocal},
	{SourceTagBloxroute, SourceTagChainbound},
	{SourceTagBloxroute, SourceTagEden},
	{SourceTagChainbound, SourceTagEden},
}
