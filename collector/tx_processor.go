package collector

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type TxProcessor struct {
	log    *zap.SugaredLogger
	txC    chan TxIn // important that value is sent in here, otherwise there are memory race conditions
	uid    string
	outDir string

	outFiles     map[int64]*os.File // batches for 10 min intervals (key is lower 10min timestamp)
	outFilesLock sync.RWMutex

	txn     map[ethcommon.Hash]time.Time
	txnLock sync.RWMutex

	txCnt atomic.Uint64

	srcCntFirst     map[string]uint64
	srcCntFirstLock sync.RWMutex

	srcCntAll     map[string]uint64
	srcCntAllLock sync.RWMutex
}

func NewTxProcessor(log *zap.SugaredLogger, outDir, uid string) *TxProcessor {
	return &TxProcessor{ //nolint:exhaustruct
		log:         log, // .With("uid", uid),
		txC:         make(chan TxIn, 100),
		uid:         uid,
		outDir:      outDir,
		outFiles:    make(map[int64]*os.File),
		txn:         make(map[ethcommon.Hash]time.Time),
		srcCntFirst: make(map[string]uint64),
		srcCntAll:   make(map[string]uint64),
	}
}

func (p *TxProcessor) Start() {
	// Ensure output directory exists
	err := os.MkdirAll(p.outDir, os.ModePerm)
	if err != nil {
		p.log.Error(err)
		return
	}

	p.log.Debug("Waiting for transactions...")

	// start the txn map cleaner background task
	go p.cleanupBackgroundTask()

	// start listening for transactions coming in through the channel
	for txIn := range p.txC {
		p.processTx(txIn)
	}
}

func (p *TxProcessor) processTx(txIn TxIn) {
	txHash := txIn.Tx.Hash()
	log := p.log.With("tx_hash", txHash.Hex())
	log.Debug("processTx")

	// count all transactions per source
	p.srcCntAllLock.Lock()
	p.srcCntAll[txIn.URITag]++
	p.srcCntAllLock.Unlock()

	// process transactions only once
	p.txnLock.RLock()
	_, ok := p.txn[txHash]
	p.txnLock.RUnlock()
	if ok {
		log.Debug("transaction already processed")
		return
	}

	p.txCnt.Inc()

	// count first transactions per source (i.e. who delivers a given tx first)
	p.srcCntFirstLock.Lock()
	p.srcCntFirst[txIn.URITag]++
	p.srcCntFirstLock.Unlock()

	// create tx rlp
	rlpHex, err := common.TxToRLPString(txIn.Tx)
	if err != nil {
		log.Errorw("failed to encode rlp", "error", err)
		return
	}

	// build the summary
	txDetail := TxDetail{
		Timestamp: txIn.T.UnixMilli(),
		Hash:      txHash.Hex(),
		RawTx:     rlpHex,
	}

	// Write to CSV file
	f, isCreated, err := p.getOutputCSVFile(txIn.T.Unix())
	if err != nil {
		log.Errorw("getOutputCSVFile", "error", err)
		return
	}

	if isCreated {
		p.log.Infof("new file created: %s", f.Name())
	}

	_, err = fmt.Fprintf(f, "%d,%s,%s\n", txDetail.Timestamp, txDetail.Hash, txDetail.RawTx)
	if err != nil {
		log.Errorw("fmt.Fprintf", "error", err)
		return
	}

	// Remember that this transaction was processed
	p.txnLock.Lock()
	p.txn[txHash] = txIn.T
	p.txnLock.Unlock()
}

func (p *TxProcessor) getOutputCSVFile(timestamp int64) (f *os.File, isCreated bool, err error) {
	// bucketTS := timestamp / secPerDay * secPerDay // down-round timestamp to start of bucket
	sec := int64(bucketMinutes * 60)
	bucketTS := timestamp / sec * sec // timestamp down-round to start of bucket
	t := time.Unix(bucketTS, 0).UTC()

	// return if file already open
	p.outFilesLock.RLock()
	f, ok := p.outFiles[bucketTS]
	p.outFilesLock.RUnlock()
	if ok {
		return f, false, nil
	}

	// open file for writing
	dir := filepath.Join(p.outDir, t.Format(time.DateOnly), "transactions")
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		p.log.Error(err)
		return nil, false, err
	}

	fn := filepath.Join(dir, p.getFilename(bucketTS))
	f, err = os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		p.log.Errorw("os.Create", "error", err)
		return nil, false, err
	}

	// add to open file list
	p.outFilesLock.Lock()
	p.outFiles[bucketTS] = f
	p.outFilesLock.Unlock()
	return f, true, nil
}

func (p *TxProcessor) getFilename(timestamp int64) string {
	t := time.Unix(timestamp, 0).UTC()
	return fmt.Sprintf("txs_%s_%s.csv", t.Format("2006-01-02-15-04"), p.uid)
}

func (p *TxProcessor) cleanupBackgroundTask() {
	for {
		time.Sleep(time.Minute)

		// Remove old transactions from cache
		cachedBefore := len(p.txn)
		p.txnLock.Lock()
		for k, v := range p.txn {
			if time.Since(v) > txCacheTime {
				delete(p.txn, k)
			}
		}
		p.txnLock.Unlock()

		// Remove old files from cache
		filesBefore := len(p.outFiles)
		p.outFilesLock.Lock()
		for timestamp, file := range p.outFiles {
			usageSec := bucketMinutes * 60 * 2
			if time.Now().UTC().Unix()-timestamp > int64(usageSec) { // remove all handles from 2x usage seconds ago
				p.log.Infow("closing file", "timestamp", timestamp, "filename", p.getFilename(timestamp))
				delete(p.outFiles, timestamp)
				_ = file.Close()
			}
		}
		p.outFilesLock.Unlock()

		// Get memory stats
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		// Print stats
		p.log.Infow("stats",
			"txcache_before", common.Printer.Sprint(cachedBefore),
			"txcache_after", common.Printer.Sprint(len(p.txn)),
			"txcache_removed", common.Printer.Sprint(cachedBefore-len(p.txn)),
			"files_before", filesBefore,
			"files_after", len(p.outFiles),
			"goroutines", common.Printer.Sprint(runtime.NumGoroutine()),
			"alloc_mb", m.Alloc/1024/1024,
			"num_gc", common.Printer.Sprint(m.NumGC),
			"tx_per_min", common.Printer.Sprint(p.txCnt.Load()),
		)

		// print and reset stats about who got a tx first
		srcStatsLog := p.log
		p.srcCntFirstLock.Lock()
		for k, v := range p.srcCntFirst {
			srcStatsLog = srcStatsLog.With(k, common.Printer.Sprint(v))
			p.srcCntFirst[k] = 0
		}
		p.srcCntFirstLock.Unlock()
		srcStatsLog.Info("source_stats_first")

		// print and reset stats about overall number of tx per source
		srcStatsLog = p.log
		p.srcCntAllLock.Lock()
		for k, v := range p.srcCntAll {
			srcStatsLog = srcStatsLog.With(k, common.Printer.Sprint(v))
			p.srcCntAll[k] = 0
		}
		p.srcCntAllLock.Unlock()
		srcStatsLog.Info("source_stats_all")

		// reset overall counter
		p.txCnt.Store(0)
	}
}
