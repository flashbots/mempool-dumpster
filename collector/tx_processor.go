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
	uid    string
	outDir string
	txC    chan TxIn // note: it's important that the value is sent in here instead of a pointer, otherwise there are memory race conditions

	outFiles         map[int64]*os.File
	outFilesSrcStats map[int64]*os.File
	outFilesLock     sync.RWMutex

	txn     map[ethcommon.Hash]time.Time
	txnLock sync.RWMutex

	txCnt atomic.Uint64

	srcCntFirst     map[string]uint64
	srcCntFirstLock sync.RWMutex

	srcCntAll     map[string]uint64
	srcCntUnique  map[string]map[string]bool
	srcCntAllLock sync.RWMutex

	recSourcelog bool // whether to record source stats (a CSV file with timestamp_ms,hash,source)
}

func NewTxProcessor(log *zap.SugaredLogger, outDir, uid string, writeSourcelog bool) *TxProcessor {
	return &TxProcessor{ //nolint:exhaustruct
		log: log, // .With("uid", uid),
		txC: make(chan TxIn, 100),
		uid: uid,

		outDir:           outDir,
		outFiles:         make(map[int64]*os.File),
		outFilesSrcStats: make(map[int64]*os.File),

		txn:          make(map[ethcommon.Hash]time.Time),
		srcCntFirst:  make(map[string]uint64),
		srcCntAll:    make(map[string]uint64),
		srcCntUnique: make(map[string]map[string]bool),
		recSourcelog: writeSourcelog,
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
	p.srcCntAll[txIn.Source]++
	if p.srcCntUnique[txIn.Source] == nil {
		p.srcCntUnique[txIn.Source] = make(map[string]bool)
	}
	p.srcCntUnique[txIn.Source][txHash.Hex()] = true
	p.srcCntAllLock.Unlock()

	// get output file handles
	fTx, fSrcStats, isCreated, err := p.getOutputCSVFiles(txIn.T.Unix())
	if err != nil {
		log.Errorw("getOutputCSVFiles", "error", err)
		return
	} else if isCreated {
		p.log.Infof("new file created: %s", fTx.Name())
		if p.recSourcelog {
			p.log.Infof("new file created: %s", fSrcStats.Name())
		}
	}

	// record source stats
	if p.recSourcelog {
		_, err = fmt.Fprintf(fSrcStats, "%d,%s,%s\n", txIn.T.UnixMilli(), txHash.Hex(), txIn.Source)
		if err != nil {
			log.Errorw("fmt.Fprintf", "error", err)
			return
		}
	}

	// process transactions only once
	p.txnLock.RLock()
	_, ok := p.txn[txHash]
	p.txnLock.RUnlock()
	if ok {
		log.Debug("transaction already processed")
		return
	}

	// Total unique tx count
	p.txCnt.Inc()

	// count first transactions per source (i.e. who delivers a given tx first)
	p.srcCntFirstLock.Lock()
	p.srcCntFirst[txIn.Source]++
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

	_, err = fmt.Fprintf(fTx, "%d,%s,%s\n", txDetail.Timestamp, txDetail.Hash, txDetail.RawTx)
	if err != nil {
		log.Errorw("fmt.Fprintf", "error", err)
		return
	}

	// Remember that this transaction was processed
	p.txnLock.Lock()
	p.txn[txHash] = txIn.T
	p.txnLock.Unlock()
}

// getOutputCSVFiles returns two file handles - one for the transactions and one for source stats, if needed - and a boolean indicating whether the file was created
func (p *TxProcessor) getOutputCSVFiles(timestamp int64) (fTx, fSrcStats *os.File, isCreated bool, err error) {
	// bucketTS := timestamp / secPerDay * secPerDay // down-round timestamp to start of bucket
	sec := int64(bucketMinutes * 60)
	bucketTS := timestamp / sec * sec // timestamp down-round to start of bucket
	t := time.Unix(bucketTS, 0).UTC()

	// files may already be opened
	var fTxOk, fSrcStatsOk bool
	p.outFilesLock.RLock()
	fTx, fTxOk = p.outFiles[bucketTS]
	fSrcStats, fSrcStatsOk = p.outFilesSrcStats[bucketTS]
	p.outFilesLock.RUnlock()

	if !fTxOk {
		// open transaction file for writing
		dir := filepath.Join(p.outDir, t.Format(time.DateOnly), "transactions")
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			p.log.Error(err)
			return nil, nil, false, err
		}

		fn := filepath.Join(dir, p.getFilename("txs", bucketTS))
		fTx, err = os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			p.log.Errorw("os.Create", "error", err)
			return nil, nil, false, err
		}
	}

	if p.recSourcelog && !fSrcStatsOk {
		// open sourcelog for writing
		dir := filepath.Join(p.outDir, t.Format(time.DateOnly), "sourcelog")
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			p.log.Error(err)
			return nil, nil, false, err
		}

		fn := filepath.Join(dir, p.getFilename("src", bucketTS))
		fSrcStats, err = os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			p.log.Errorw("os.Create", "error", err)
			return nil, nil, false, err
		}
	}

	// if one file was opened, record it now
	if !fTxOk || (p.recSourcelog && !fSrcStatsOk) {
		p.outFilesLock.Lock()
		p.outFiles[bucketTS] = fTx
		p.outFilesSrcStats[bucketTS] = fSrcStats
		p.outFilesLock.Unlock()
		isCreated = true
	}
	return fTx, fSrcStats, isCreated, nil
}

func (p *TxProcessor) getFilename(prefix string, timestamp int64) string {
	t := time.Unix(timestamp, 0).UTC()
	if prefix != "" {
		prefix += "_"
	}
	return fmt.Sprintf("%s%s_%s.csv", prefix, t.Format("2006-01-02_15-04"), p.uid)
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
				p.log.Infow("closing file", "timestamp", timestamp, "filename", file.Name())
				delete(p.outFiles, timestamp)
				_ = file.Close()
			}
		}
		for timestamp, file := range p.outFilesSrcStats {
			usageSec := bucketMinutes * 60 * 2
			if time.Now().UTC().Unix()-timestamp > int64(usageSec) { // remove all handles from 2x usage seconds ago
				p.log.Infow("closing file", "timestamp", timestamp, "filename", file.Name())
				delete(p.outFilesSrcStats, timestamp)
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
		srcStatsAllLog := p.log
		srcStatsUniqueLog := p.log
		p.srcCntAllLock.Lock()
		for k, v := range p.srcCntAll {
			srcStatsAllLog = srcStatsAllLog.With(k, common.Printer.Sprint(v))
			p.srcCntAll[k] = 0
		}
		for k, v := range p.srcCntUnique {
			srcStatsUniqueLog = srcStatsUniqueLog.With(k, common.Printer.Sprint(len(v)))
			p.srcCntUnique[k] = make(map[string]bool)
		}
		p.srcCntAllLock.Unlock()

		srcStatsAllLog.Info("source_stats_all")
		srcStatsUniqueLog.Info("source_stats_unique")

		// reset overall counter
		p.txCnt.Store(0)
	}
}
