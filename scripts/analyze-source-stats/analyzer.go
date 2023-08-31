package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/flashbots/mempool-dumpster/common"
)

const (
	referenceLocalSource = "local"
)

var bucketsMS = []int64{1, 5, 50, 100, 250, 500, 1000, 2000} // note: 0 would be equal timestamps

func prettyInt(i int) string {
	return printer.Sprintf("%d", i)
}

func prettyInt64(i int64) string {
	return printer.Sprintf("%d", i)
}

type Analyzer struct {
	txs map[string]map[string]int64 // [hash][src] = timestamp

	sources   []string // sorted alphabetically
	nUniqueTx int
	nAllTx    int

	nTransactionsPerSource map[string]int64
	nUniqueTxPerSource     map[string]int64

	nNotSeenLocalPerSource map[string]int64
	nOverallNotSeenLocal   int64
	// nSeenOnlyByRef map[string]int64 // todo

	nTxSeenBySingleSource int64

	timestampFirst int64
	timestampLast  int64
	timeFirst      time.Time
	timeLast       time.Time
	duration       time.Duration
}

func NewAnalyzer(transactions map[string]map[string]int64) *Analyzer {
	a := &Analyzer{ //nolint:exhaustruct
		txs:                    transactions,
		nTransactionsPerSource: make(map[string]int64),
		nUniqueTxPerSource:     make(map[string]int64),
		nNotSeenLocalPerSource: make(map[string]int64),
	}

	a.init()
	return a
}

// Init does some efficient initial data analysis and preparation for later use
func (a *Analyzer) init() {
	// unique tx
	a.nUniqueTx = len(a.txs)

	// iterate over tx to
	for _, sources := range a.txs {
		// count all tx
		a.nAllTx += len(sources)

		// number of unique tx -- special case for local+apool
		// if len(sources) == 2 {
		// 	if sources[referenceSource] != 0 && sources[referenceSource2] != 0 {
		// 		a.nUniqueTxPerSource[referenceSource] += 1
		// 		a.nTxSeenBySingleSource += 1
		// 	}
		// }

		if sources[referenceLocalSource] == 0 {
			a.nOverallNotSeenLocal += 1
		}

		// iterate over all sources for a given hash
		for src, timestamp := range sources {
			// get number of unique transactions by any single source
			if len(sources) == 1 {
				a.nUniqueTxPerSource[src] += 1
				a.nTxSeenBySingleSource += 1
			}

			// remember if this transaction was not seen by the reference source
			if sources[referenceLocalSource] == 0 {
				a.nNotSeenLocalPerSource[src] += 1
			}

			// count number of tx per source
			a.nTransactionsPerSource[src] += 1

			// find first and last timestamp
			if a.timestampFirst == 0 || timestamp < a.timestampFirst {
				a.timestampFirst = timestamp
			}
			if a.timestampLast == 0 || timestamp > a.timestampLast {
				a.timestampLast = timestamp
			}
		}
	}

	// convert timestamps to duration and UTC time
	a.duration = time.Duration(a.timestampLast-a.timestampFirst) * time.Millisecond
	a.timeFirst = time.Unix(a.timestampFirst/1000, 0).UTC()
	a.timeLast = time.Unix(a.timestampLast/1000, 0).UTC()

	// get sorted list of sources
	for src := range a.nTransactionsPerSource {
		a.sources = append(a.sources, src)
	}
	sort.Strings(a.sources)
}

func (a *Analyzer) benchmarkSourceVsLocal(src, ref string) (srcFirstBuckets map[int64]int64, totalFirstBySrc, totalSeenByBoth int) {
	srcFirstBuckets = make(map[int64]int64) // [bucket_ms] = count

	// How much earlier were transactions received by blx vs. the local node?
	for _, sources := range a.txs {
		if len(sources) == 1 {
			continue
		}

		// ensure tx was seen by both source and reference nodes
		if _, seenBySrc := sources[src]; !seenBySrc {
			continue
		}
		if _, seenByRef := sources[ref]; !seenByRef {
			continue
		}

		totalSeenByBoth += 1

		srcTS := sources[src]
		localTS := sources[ref]
		diff := srcTS - localTS

		if diff > 0 {
			totalFirstBySrc += 1
			for _, thresholdMS := range bucketsMS {
				if diff >= thresholdMS {
					srcFirstBuckets[thresholdMS] += 1
				}
			}
		}
	}

	return srcFirstBuckets, totalFirstBySrc, totalSeenByBoth
}

func (a *Analyzer) Print() {
	fmt.Println("")
	fmt.Printf("From: %s \n", a.timeFirst.String())
	fmt.Printf("To:   %s \n", a.timeLast.String())
	fmt.Printf("      (%s) \n", a.duration.String())
	fmt.Println("")
	fmt.Printf("Sources: %s \n", strings.Join(a.sources, ", "))
	fmt.Println("")
	fmt.Printf("- Transactions: %8s \n", prettyInt(a.nAllTx))
	fmt.Printf("- Unique txs:   %8s \n", prettyInt(a.nUniqueTx))

	fmt.Println("")
	fmt.Println("-------------")
	fmt.Println("Overall stats")
	fmt.Println("-------------")

	fmt.Println("")
	fmt.Printf("All transactions received: %s \n", prettyInt(a.nAllTx))
	for _, src := range a.sources { // sorted iteration
		if a.nTransactionsPerSource[src] > 0 {
			fmt.Printf("- %-8s %10s\n", src, prettyInt64(a.nTransactionsPerSource[src]))
		}
	}

	fmt.Println("")
	fmt.Printf("Exclusive tx (single source): %s / %s (%s) \n", prettyInt64(a.nTxSeenBySingleSource), prettyInt(a.nUniqueTx), common.Int64DiffPercentFmt(a.nTxSeenBySingleSource, int64(a.nUniqueTx)))
	for _, src := range a.sources {
		if a.nTransactionsPerSource[src] > 0 {
			cnt := a.nUniqueTxPerSource[src]
			fmt.Printf("- %-8s %10s\n", src, prettyInt(int(cnt)))
		}
	}

	fmt.Println("")
	fmt.Printf("Transactions not seen by local node: %s / %s (%s)\n", prettyInt64(a.nOverallNotSeenLocal), prettyInt(a.nUniqueTx), common.Int64DiffPercentFmt(a.nOverallNotSeenLocal, int64(a.nUniqueTx)))
	for _, src := range a.sources {
		if a.nTransactionsPerSource[src] > 0 && src != referenceLocalSource {
			cnt := a.nNotSeenLocalPerSource[src]
			fmt.Printf("- %-8s %10s\n", src, prettyInt64(cnt))
		}
	}

	// latency analysis for various sources:
	fmt.Println("")
	fmt.Println("------------------")
	fmt.Println("Latency comparison")
	fmt.Println("------------------")
	latencyComps := []struct{ src, ref string }{
		{common.BloxrouteTag, referenceLocalSource},
		{"apool2", referenceLocalSource},
		{"apool2", common.BloxrouteTag},
		{common.BloxrouteTag, "apool2"},
	}

	for _, comp := range latencyComps {
		srcFirstBuckets, totalFirstBySrc, _ := a.benchmarkSourceVsLocal(comp.src, comp.ref)

		fmt.Println("")
		// fmt.Printf("%s transactions received before %s: %s / %s (%s) \n", comp.src, comp.ref, prettyInt64(int64(totalFirstBySrc)), prettyInt64(int64(totalSeenByBoth)), common.Int64DiffPercentFmt(int64(totalFirstBySrc), int64(totalSeenByBoth)))
		fmt.Printf("%s transactions received before %s: %s \n", comp.src, comp.ref, prettyInt64(int64(totalFirstBySrc)))
		for _, bucketMS := range bucketsMS {
			s := fmt.Sprintf("%d ms", bucketMS)
			cnt := srcFirstBuckets[bucketMS]
			fmt.Printf(" - %-8s %8s \n", s, prettyInt64(cnt))
		}
	}
}
