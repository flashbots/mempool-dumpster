package collector

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

type TxIn struct {
	T      time.Time
	Tx     *types.Transaction
	Source string
}

type TxDetail struct {
	Timestamp int64  `json:"timestamp"`
	Hash      string `json:"hash"`
	RawTx     string `json:"rawTx"`
}

type SourceCounter struct {
	lock   sync.Mutex
	counts map[string]map[string]map[string]uint64 // cntType -> source -> key -> count
}

func NewSourceCounter() SourceCounter {
	return SourceCounter{ //nolint:exhaustruct
		counts: make(map[string]map[string]map[string]uint64),
	}
}

func (sc *SourceCounter) Inc(cntType, source string) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	if _, ok := sc.counts[cntType]; !ok {
		sc.counts[cntType] = make(map[string]map[string]uint64)
	}
	if _, ok := sc.counts[cntType][source]; !ok {
		sc.counts[cntType][source] = make(map[string]uint64)
	}

	sc.counts[cntType][source][cntType] += 1
}

func (sc *SourceCounter) IncKey(cntType, source, key string) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	if _, ok := sc.counts[cntType]; !ok {
		sc.counts[cntType] = make(map[string]map[string]uint64)
	}
	if _, ok := sc.counts[cntType][source]; !ok {
		sc.counts[cntType][source] = make(map[string]uint64)
	}

	sc.counts[cntType][source][key] += 1
}

func (sc *SourceCounter) Get(cntType string) map[string]map[string]uint64 {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	return sc.counts[cntType]
}

func (sc *SourceCounter) Reset() {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	sc.counts = make(map[string]map[string]map[string]uint64)
}
