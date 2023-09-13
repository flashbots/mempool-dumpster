package collector

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type TxProcessorOpts struct {
	Log          *zap.SugaredLogger
	OutDir       string
	UID          string
	CheckNodeURI string
}

type TxProcessor struct {
	log    *zap.SugaredLogger
	uid    string
	outDir string
	txC    chan TxIn // note: it's important that the value is sent in here instead of a pointer, otherwise there are memory race conditions

	outFilesLock sync.RWMutex
	outFiles     map[int64]*OutFiles

	knownTxs     map[ethcommon.Hash]time.Time
	knownTxsLock sync.RWMutex

	txCnt      atomic.Uint64
	srcMetrics SourceMetrics

	checkNodeURI string
	ethClient    *ethclient.Client
}

type OutFiles struct {
	FTxs       *os.File
	FSourcelog *os.File
	FTrash     *os.File
}

func NewTxProcessor(opts TxProcessorOpts) *TxProcessor {
	return &TxProcessor{ //nolint:exhaustruct
		log: opts.Log, // .With("uid", uid),
		txC: make(chan TxIn, 100),
		uid: opts.UID,

		outDir:   opts.OutDir,
		outFiles: make(map[int64]*OutFiles),

		knownTxs:   make(map[ethcommon.Hash]time.Time),
		srcMetrics: NewMetricsCounter(),

		checkNodeURI: opts.CheckNodeURI,
	}
}

func (p *TxProcessor) Start() {
	p.log.Info("Starting TxProcessor ...")
	var err error

	if p.checkNodeURI != "" {
		p.log.Infof("Conecting to check-node at %s ...", p.checkNodeURI)
		p.ethClient, err = ethclient.Dial(p.checkNodeURI)
		if err != nil {
			p.log.Fatal(err)
		}
	}

	// Ensure output directory exists
	err = os.MkdirAll(p.outDir, os.ModePerm)
	if err != nil {
		p.log.Fatal(err)
	}

	p.log.Info("Waiting for transactions...")

	// start the txn map cleaner background task
	go p.cleanupBackgroundTask()

	// start listening for transactions coming in through the channel
	for txIn := range p.txC {
		p.processTx(txIn)
	}
}

func (p *TxProcessor) processTx(txIn TxIn) {
	txHash := txIn.Tx.Hash()
	log := p.log.With("tx_hash", txHash.Hex()).With("source", txIn.Source)
	log.Debug("processTx")

	// count all transactions per source
	p.srcMetrics.Inc(KeyStatsAll, txIn.Source)
	p.srcMetrics.IncKey(KeyStatsUnique, txIn.Source, txIn.Tx.Hash().Hex())

	// get output file handles
	outFiles, isCreated, err := p.getOutputCSVFiles(txIn.T.Unix())
	if err != nil {
		log.Errorw("getOutputFiles", "error", err)
		return
	} else if isCreated {
		p.log.Infof("new file created: %s", outFiles.FTxs.Name())
		p.log.Infof("new file created: %s", outFiles.FSourcelog.Name())
		p.log.Infof("new file created: %s", outFiles.FTrash.Name())
	}

	// write sourcelog
	_, err = fmt.Fprintf(outFiles.FSourcelog, "%d,%s,%s\n", txIn.T.UnixMilli(), txHash.Hex(), txIn.Source)
	if err != nil {
		log.Errorw("fmt.Fprintf", "error", err)
		return
	}

	// process transactions only once
	p.knownTxsLock.RLock()
	_, ok := p.knownTxs[txHash]
	p.knownTxsLock.RUnlock()
	if ok {
		log.Debug("transaction already processed")
		return
	}

	// errNotFound := errors.New("not found")
	// check if tx was already included
	if p.ethClient != nil {
		receipt, err := p.ethClient.TransactionReceipt(context.Background(), txHash)
		if err != nil {
			if err.Error() == "not found" {
				// all good, mempool tx
			} else {
				log.Errorw("ethClient.TransactionReceipt", "error", err)
			}
		} else if receipt != nil {
			p.srcMetrics.Inc(KeyStatsTxOnChain, txIn.Source)
			log.Debugw("transaction already included", "block", receipt.BlockNumber.Uint64())
			_, err = fmt.Fprintf(outFiles.FTrash, "%d,%s,%s,%s,%s\n", txIn.T.UnixMilli(), txHash.Hex(), txIn.Source, TrashTxAlreadyOnChain, receipt.BlockNumber.String())
			if err != nil {
				log.Errorw("fmt.Fprintf", "error", err)
			}
			return
		}
	}

	// Total unique tx count
	p.txCnt.Inc()

	// count first transactions per source (i.e. who delivers a given tx first)
	p.srcMetrics.Inc(KeyStatsFirst, txIn.Source)

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

	_, err = fmt.Fprintf(outFiles.FTxs, "%d,%s,%s\n", txDetail.Timestamp, txDetail.Hash, txDetail.RawTx)
	if err != nil {
		log.Errorw("fmt.Fprintf", "error", err)
		return
	}

	// Remember that this transaction was processed
	p.knownTxsLock.Lock()
	p.knownTxs[txHash] = txIn.T
	p.knownTxsLock.Unlock()
}

// getOutputCSVFiles returns two file handles - one for the transactions and one for source stats, if needed - and a boolean indicating whether the file was created
func (p *TxProcessor) getOutputCSVFiles(timestamp int64) (outFiles *OutFiles, isCreated bool, err error) {
	// bucketTS := timestamp / secPerDay * secPerDay // down-round timestamp to start of bucket
	sec := int64(bucketMinutes * 60)
	bucketTS := timestamp / sec * sec // timestamp down-round to start of bucket
	t := time.Unix(bucketTS, 0).UTC()

	// files may already be opened
	p.outFilesLock.RLock()
	outFiles, outFilesOk := p.outFiles[bucketTS]
	p.outFilesLock.RUnlock()

	if outFilesOk {
		return outFiles, false, nil
	}
	// open transactions output files
	dir := filepath.Join(p.outDir, t.Format(time.DateOnly), "transactions")
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, false, err
	}

	fn := filepath.Join(dir, p.getFilename("txs", bucketTS))
	fTx, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, false, err
	}

	// open sourcelog for writing
	dir = filepath.Join(p.outDir, t.Format(time.DateOnly), "sourcelog")
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, false, err
	}

	fn = filepath.Join(dir, p.getFilename("src", bucketTS))
	fSourcelog, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, false, err
	}

	// open trash for writing
	dir = filepath.Join(p.outDir, t.Format(time.DateOnly), "trash")
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, false, err
	}

	fn = filepath.Join(dir, p.getFilename("trash", bucketTS))
	fTrash, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, false, err
	}

	outFiles = &OutFiles{
		FTxs:       fTx,
		FSourcelog: fSourcelog,
		FTrash:     fTrash,
	}
	p.outFilesLock.Lock()
	p.outFiles[bucketTS] = outFiles
	p.outFilesLock.Unlock()
	return outFiles, true, nil
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
		cachedBefore := len(p.knownTxs)
		p.knownTxsLock.Lock()
		for k, v := range p.knownTxs {
			if time.Since(v) > txCacheTime {
				delete(p.knownTxs, k)
			}
		}
		p.knownTxsLock.Unlock()

		// Remove old files from cache
		filesBefore := len(p.outFiles)
		p.outFilesLock.Lock()
		for timestamp, outFiles := range p.outFiles {
			usageSec := bucketMinutes * 60 * 2
			if time.Now().UTC().Unix()-timestamp > int64(usageSec) { // remove all handles from 2x usage seconds ago
				p.log.Infow("closing output files", "timestamp", timestamp)
				delete(p.outFiles, timestamp)
				_ = outFiles.FTxs.Close()
				_ = outFiles.FSourcelog.Close()
				_ = outFiles.FTrash.Close()
			}
		}
		p.outFilesLock.Unlock()

		// Get memory stats
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		// Print stats
		p.log.Infow("stats",
			"txcache_before", common.Printer.Sprint(cachedBefore),
			"txcache_after", common.Printer.Sprint(len(p.knownTxs)),
			"txcache_removed", common.Printer.Sprint(cachedBefore-len(p.knownTxs)),
			"files_before", filesBefore,
			"files_after", len(p.outFiles),
			"goroutines", common.Printer.Sprint(runtime.NumGoroutine()),
			"alloc_mb", m.Alloc/1024/1024,
			"num_gc", common.Printer.Sprint(m.NumGC),
			"tx_per_min", common.Printer.Sprint(p.txCnt.Load()),
		)

		// print source stats
		p.srcMetrics.Logger(p.log, KeyStatsFirst, false).Info("source_stats/first")
		p.srcMetrics.Logger(p.log, KeyStatsAll, false).Info("source_stats/all")
		p.srcMetrics.Logger(p.log, KeyStatsUnique, true).Info("source_stats/unique")
		p.srcMetrics.Logger(p.log, KeyStatsTxOnChain, false).Info("source_stats/tx_onchain")

		// reset counters
		p.srcMetrics.Reset()
		p.txCnt.Store(0)
	}
}
