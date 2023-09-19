package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/flashbots/mempool-dumpster/common"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	bucketsMS = []int64{1, 10, 50, 100, 250, 500, 1000, 5000} // note: 0 would be equal timestamps

	printer = message.NewPrinter(language.English)
)

func prettyInt(i int) string {
	return printer.Sprintf("%d", i)
}

func prettyInt64(i int64) string {
	return printer.Sprintf("%d", i)
}

type AnalyzerOpts struct {
	Transactions map[string]map[string]int64 // [hash][src] = timestamp
	TxBlacklist  map[string]bool             // optional, blacklist of txs (these will be ignored for analysis)
	TxWhitelist  map[string]bool             // optional, whitelist of txs (only these will be used for analysis)
	SourceComps  []common.SourceComp
}

type Analyzer struct {
	opts         AnalyzerOpts
	useWhitelist bool

	sources   []string // sorted alphabetically
	nUniqueTx int
	nAllTx    int

	nTransactionsPerSource map[string]int64
	nUniqueTxPerSource     map[string]int64

	nNotSeenLocalPerSource map[string]int64
	nOverallNotSeenLocal   int64

	nTxSeenBySingleSource int64

	timestampFirst int64
	timestampLast  int64
	timeFirst      time.Time
	timeLast       time.Time
	duration       time.Duration
}

func NewAnalyzer(opts AnalyzerOpts) *Analyzer {
	a := &Analyzer{ //nolint:exhaustruct
		opts:         opts,
		useWhitelist: len(opts.TxWhitelist) > 0,

		nTransactionsPerSource: make(map[string]int64),
		nUniqueTxPerSource:     make(map[string]int64),
		nNotSeenLocalPerSource: make(map[string]int64),
	}

	a.init()
	return a
}

// Init does some efficient initial data analysis and preparation for later use
func (a *Analyzer) init() {
	// iterate over tx to
	for txHash, sources := range a.opts.Transactions {
		txHashLower := strings.ToLower(txHash)
		if a.opts.TxBlacklist[txHashLower] {
			continue
		}

		if a.useWhitelist && !a.opts.TxWhitelist[txHashLower] {
			continue
		}

		// unique tx
		a.nUniqueTx += 1

		// count all tx
		a.nAllTx += len(sources)

		// count all tx that were not seen locally
		if sources[common.SourceTagLocal] == 0 {
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
			if sources[common.SourceTagLocal] == 0 {
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
	for txHash, sources := range a.opts.Transactions {
		txHashLower := strings.ToLower(txHash)
		if a.opts.TxBlacklist[txHashLower] {
			continue
		}

		if a.useWhitelist && !a.opts.TxWhitelist[txHashLower] {
			continue
		}

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
		diff := localTS - srcTS

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
	fmt.Println(a.Sprint())
}

func (a *Analyzer) Sprint() string {
	// out := fmt.Sprintln("mempool-dumpster.flashbots.net")
	// out += fmt.Sprintln("")
	out := fmt.Sprintf("From: %s \n", a.timeFirst.String())
	out += fmt.Sprintf("To:   %s \n", a.timeLast.String())
	out += fmt.Sprintf("      (%s) \n", a.duration.String())
	out += fmt.Sprintln("")
	out += fmt.Sprintf("Sources: %s \n", strings.Join(a.sources, ", "))
	// out += fmt.Sprintln("")
	// out += fmt.Sprintf("- Transactions: %9s \n", prettyInt(a.nAllTx))
	// out += fmt.Sprintf("- Unique txs:   %9s \n", prettyInt(a.nUniqueTx))

	out += fmt.Sprintln("")
	out += fmt.Sprintln("-------------")
	out += fmt.Sprintln("Overall stats")
	out += fmt.Sprintln("-------------")
	out += fmt.Sprintln("")
	out += fmt.Sprintf("Unique transactions: %s \n", prettyInt(a.nUniqueTx))

	out += fmt.Sprintln("")

	// out += fmt.Sprintf("All transactions received: %s \n", prettyInt(a.nAllTx))
	out += "Transactions received: \n"
	for _, src := range a.sources { // sorted iteration
		if a.nTransactionsPerSource[src] > 0 {
			out += fmt.Sprintf("- %-11s %10s\n", src, prettyInt64(a.nTransactionsPerSource[src]))
		}
	}

	out += fmt.Sprintln("")
	out += fmt.Sprintf("Exclusive tx (single source): %s / %s (%s) \n", prettyInt64(a.nTxSeenBySingleSource), prettyInt(a.nUniqueTx), common.Int64DiffPercentFmt(a.nTxSeenBySingleSource, int64(a.nUniqueTx)))
	for _, src := range a.sources {
		if a.nTransactionsPerSource[src] > 0 {
			cnt := a.nUniqueTxPerSource[src]
			out += fmt.Sprintf("- %-11s %10s\n", src, prettyInt(int(cnt)))
		}
	}

	out += fmt.Sprintln("")
	out += fmt.Sprintf("Transactions not seen by local node: %s / %s (%s)\n", prettyInt64(a.nOverallNotSeenLocal), prettyInt(a.nUniqueTx), common.Int64DiffPercentFmt(a.nOverallNotSeenLocal, int64(a.nUniqueTx)))
	for _, src := range a.sources {
		if a.nTransactionsPerSource[src] > 0 && src != common.SourceTagLocal {
			cnt := a.nNotSeenLocalPerSource[src]
			out += fmt.Sprintf("- %-11s %10s\n", src, prettyInt64(cnt))
		}
	}

	// latency analysis for various sources:
	out += fmt.Sprintln("")
	out += fmt.Sprintln("-----------------")
	out += fmt.Sprintln("Source comparison")
	out += fmt.Sprintln("-----------------")

	for _, comp := range a.opts.SourceComps {
		if a.nTransactionsPerSource[comp.Source] == 0 || a.nTransactionsPerSource[comp.Reference] == 0 {
			continue
		}

		srcFirstBuckets, totalFirstBySrc, totalSeenByBoth := a.benchmarkSourceVsLocal(comp.Source, comp.Reference)
		out += fmt.Sprintln("")
		// out += fmt.Sprintf("%s transactions received before %s: %s / %s (%s) \n", comp.Source, comp.Reference, prettyInt64(int64(totalFirstBySrc)), prettyInt64(int64(totalSeenByBoth)), common.Int64DiffPercentFmt(int64(totalFirstBySrc), int64(totalSeenByBoth)))
		out += fmt.Sprintf("%s transactions received before %s: %s / %s (%s)\n", comp.Source, comp.Reference, prettyInt(totalFirstBySrc), prettyInt(totalSeenByBoth), common.Int64DiffPercentFmt(int64(totalFirstBySrc), int64(totalSeenByBoth)))
		for _, bucketMS := range bucketsMS {
			s := fmt.Sprintf("%d ms", bucketMS)
			cnt := srcFirstBuckets[bucketMS]
			out += fmt.Sprintf("- %-8s %10s   (%7s) \n", s, prettyInt64(cnt), common.Int64DiffPercentFmt(cnt, int64(totalFirstBySrc)))
		}
	}

	return out
}
