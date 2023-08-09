package collector

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type TxProcessor struct {
	log    *zap.SugaredLogger
	txC    chan TxIn // important that value is sent in here, otherwise there are memory race conditions
	outDir string

	outFiles     map[int64]*os.File // batches for 10 min intervals (key is lower 10min timestamp)
	outFilesLock sync.RWMutex

	txn     map[common.Hash]time.Time
	txnLock sync.RWMutex
	txCnt   atomic.Uint64
}

func NewTxProcessor(log *zap.SugaredLogger, outDir string) *TxProcessor {
	return &TxProcessor{ //nolint:exhaustruct
		log:      log,
		txC:      make(chan TxIn, 100),
		outDir:   outDir,
		outFiles: make(map[int64]*os.File),
		txn:      make(map[common.Hash]time.Time),
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
		go p.processTx(txIn)
	}
}

func (p *TxProcessor) getOutputCSVFile(timestamp int64) (*os.File, error) {
	sec := int64(bucketMinutes * 60)
	bucketTS := timestamp / sec * sec // down-round timestamp to last 10 minutes
	t := time.Unix(bucketTS, 0).UTC()

	// return if already open
	p.outFilesLock.RLock()
	f, ok := p.outFiles[bucketTS]
	p.outFilesLock.RUnlock()
	if ok {
		return f, nil
	}

	// open file for writing
	dir := filepath.Join(p.outDir, t.Format(time.DateOnly), "transactions")
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		p.log.Error(err)
		return nil, err
	}

	_fn := fmt.Sprintf("txs-%s.csv", t.Format("2006-01-02-15-04"))
	fn := filepath.Join(dir, _fn)
	f, err = os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		p.log.Errorw("os.Create", "error", err)
		return nil, err
	}

	// add to open file list
	p.outFilesLock.Lock()
	p.outFiles[bucketTS] = f
	p.outFilesLock.Unlock()
	return f, nil
}

func (p *TxProcessor) processTx(txIn TxIn) {
	txHash := txIn.tx.Hash()
	log := p.log.With("tx_hash", txHash.Hex())
	log.Debug("processTx")

	// process transactions only once
	p.txnLock.RLock()
	_, ok := p.txn[txHash]
	p.txnLock.RUnlock()
	if ok {
		log.Debug("transaction already processed")
		return
	}

	p.txCnt.Inc()

	// prepare rlp rawtx
	buf := new(bytes.Buffer)
	err := txIn.tx.EncodeRLP(buf)
	if err != nil {
		log.Errorw("failed to encode rlp", "error", err)
		return
	}

	// build the summary
	txDetail := TxDetail{
		Timestamp: txIn.t.UnixMilli(),
		Hash:      txHash.Hex(),
		RawTx:     hexutil.Encode(buf.Bytes()),
	}

	// Write to CSV file
	f, err := p.getOutputCSVFile(txIn.t.Unix())
	if err != nil {
		log.Errorw("getOutputCSVFile", "error", err)
		return
	}

	_, err = fmt.Fprintf(f, "%d,%s,%s\n", txDetail.Timestamp, txDetail.Hash, txDetail.RawTx)
	if err != nil {
		log.Errorw("fmt.Fprintf", "error", err)
		return
	}

	// Remember that this transaction was processed
	p.txnLock.Lock()
	p.txn[txHash] = txIn.t
	p.txnLock.Unlock()
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
		for k, f := range p.outFiles {
			usageSec := bucketMinutes * 60 * 2
			if time.Now().UTC().Unix()-k > int64(usageSec) { // remove all handles from 2x usage seconds ago
				_fn := fmt.Sprintf("txs-%s.csv", time.Unix(k, 0).Format("2006-01-02-15-04"))
				p.log.Infow("closing file", "timestamp", k, "filename", _fn)
				delete(p.outFiles, k)
				_ = f.Close()
			}
		}
		p.outFilesLock.Unlock()

		// Print stats
		p.log.Infow("stats",
			"txcache_before", cachedBefore,
			"txcache_after", len(p.txn),
			"txcache_removed", cachedBefore-len(p.txn),
			"files_before", filesBefore,
			"files_after", len(p.outFiles),
			"goroutines", runtime.NumGoroutine(),
			"tx_per_min", p.txCnt.Load())
		p.txCnt.Store(0)
	}
}
