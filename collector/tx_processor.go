package collector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/flashbots/mempool-dumpster/api"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/flashbots/mempool-dumpster/metrics"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	receiverTimeout   = 5 * time.Second
	receiptTimeout    = time.Second
	maxReceiptLookups = 32
)

var (
	errInvalidSender      = errors.New("invalid sender")
	errBlobMissingSidecar = errors.New("missing blob sidecar")
)

type TxProcessorOpts struct {
	Log                     *zap.SugaredLogger
	OutDir                  string // if empty no files will be written
	UID                     string
	Location                string // location of the collector, will be stored in sourcelogs
	CheckNodeURI            string
	ClickhouseDSN           string
	RedisEndpoint           string
	HTTPReceivers           []string
	AdditionalReceivers     []TxReceiver
	ReceiversAllowedSources []string
	APIServer               *api.Server
}

type TxProcessor struct {
	log *zap.SugaredLogger

	uid      string
	location string

	outDir  string
	txC     chan common.TxIn // note: it's important that the value is sent in here instead of a pointer, otherwise there are memory race conditions
	txCDone chan struct{}    // closed when startTransactionReceiverLoop exits

	outFilesLock sync.RWMutex
	outFiles     map[int64]OutFiles

	knownTxs     map[string]time.Time
	knownTxsLock sync.RWMutex

	txCnt      atomic.Uint64
	srcMetrics SourceMetrics

	checkNodeURI string
	ethClient    *ethclient.Client

	receivers                []TxReceiver
	receiversAllowedSources  []string
	receiversAllowAllSources bool

	lastHealthCheckCall time.Time

	clickhouseDSN string
	clickhouse    *Clickhouse

	redisEndpoint string
	redis         *Redis
}

type OutFiles struct {
	FTxs       *os.File
	FSourcelog *os.File
	FTrash     *os.File
	users      *atomic.Int64
}

type receiptPipeline struct {
	slots chan struct{}
	mu    sync.Mutex
	tails map[string]chan struct{}
	jobs  sync.WaitGroup
}

func (r *receiptPipeline) submit(hash string, process func()) {
	r.slots <- struct{}{}

	r.mu.Lock()
	previous := r.tails[hash]
	done := make(chan struct{})
	r.tails[hash] = done
	r.mu.Unlock()

	r.jobs.Add(1)
	go func() {
		defer r.jobs.Done()
		if previous != nil {
			<-previous
		}
		process()

		r.mu.Lock()
		if r.tails[hash] == done {
			delete(r.tails, hash)
		}
		close(done)
		r.mu.Unlock()
		<-r.slots
	}()
}

func NewTxProcessor(opts TxProcessorOpts) *TxProcessor {
	// Build the list of (external) transaction receivers
	receivers := make([]TxReceiver, 0, len(opts.HTTPReceivers)+len(opts.AdditionalReceivers))
	for _, r := range opts.HTTPReceivers {
		receivers = append(receivers, NewHTTPReceiver(r))
	}
	receivers = append(receivers, opts.AdditionalReceivers...)

	// The API server is also a transaction receiver, so we add it to the list
	if opts.APIServer != nil {
		receivers = append(receivers, opts.APIServer)
	}

	return &TxProcessor{ //nolint:exhaustruct
		log:     opts.Log,
		txC:     make(chan common.TxIn, 100),
		txCDone: make(chan struct{}),

		uid:      opts.UID,
		location: opts.Location,

		outDir:   opts.OutDir,
		outFiles: make(map[int64]OutFiles),

		knownTxs:   make(map[string]time.Time),
		srcMetrics: NewMetricsCounter(),

		checkNodeURI:  opts.CheckNodeURI,
		clickhouseDSN: opts.ClickhouseDSN,
		redisEndpoint: opts.RedisEndpoint,

		receivers:                receivers,
		receiversAllowedSources:  opts.ReceiversAllowedSources,
		receiversAllowAllSources: len(opts.ReceiversAllowedSources) == 1 && opts.ReceiversAllowedSources[0] == "all",
	}
}

func (p *TxProcessor) Shutdown() {
	p.log.Info("Shutting down TxProcessor ...")
	p.stopTransactionReceiverLoop()
	if p.redis != nil {
		if err := p.redis.Close(); err != nil {
			p.log.Errorw("failed to close Redis", "error", err)
		}
	}
	if p.clickhouse != nil {
		p.clickhouse.FlushCurrentBatches()
	}
}

func (p *TxProcessor) Start() {
	p.log.Info("Starting TxProcessor ...")
	var err error

	if p.clickhouseDSN != "" {
		p.log.Info("Connecting to Clickhouse...")
		p.clickhouse, err = NewClickhouse(ClickhouseOpts{
			Log: p.log,
			DSN: p.clickhouseDSN,
		})
		if err != nil {
			p.log.Fatalw("failed to connect to Clickhouse", "error", err)
		}
		p.log.Info("Connected to Clickhouse!")
	}

	if p.redisEndpoint != "" {
		p.log.Info("Connecting to Redis...")
		p.redis, err = NewRedis(p.log, p.redisEndpoint)
		if err != nil {
			p.log.Fatalw("failed to connect to Redis", "error", err)
		}
		p.log.Info("Connected to Redis!")
	}

	if p.checkNodeURI != "" {
		p.log.Infof("Connecting to check-node at %s ...", p.checkNodeURI)
		p.ethClient, err = ethclient.Dial(p.checkNodeURI)
		if err != nil {
			p.log.Fatal(err)
		}
	}

	// Ensure output directory exists (only if outDir is set)
	if p.outDir != "" {
		err = os.MkdirAll(p.outDir, os.ModePerm)
		if err != nil {
			p.log.Fatal(err)
		}
	}

	// start the txn map cleaner background task
	go p.startHousekeeper()

	// start listening for transactions coming in through the channel
	go p.startTransactionReceiverLoop()

	time.Sleep(100 * time.Millisecond) // give some time for the goroutine to start
	p.log.Info("TxProcessor started successfully")
}

func (p *TxProcessor) stopTransactionReceiverLoop() {
	close(p.txC)
	<-p.txCDone
}

func (p *TxProcessor) startTransactionReceiverLoop() {
	defer close(p.txCDone)
	var receipts *receiptPipeline
	if p.ethClient != nil {
		receipts = &receiptPipeline{
			slots: make(chan struct{}, maxReceiptLookups),
			tails: make(map[string]chan struct{}),
		}
		defer receipts.jobs.Wait()
	}
	p.log.Info("Waiting for transactions...")
	for txIn := range p.txC {
		if txIn.Tx == nil {
			continue
		}
		p.processTx(txIn, receipts)
	}
}

func (p *TxProcessor) sendTxToReceivers(txIn common.TxIn) {
	txAllowed := p.receiversAllowAllSources || slices.Contains(p.receiversAllowedSources, txIn.Source)
	if !txAllowed || len(p.receivers) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), receiverTimeout)
	defer cancel()

	metrics.ObserveTxDispatchAge(txIn.T)
	var wg sync.WaitGroup
	for _, r := range p.receivers {
		wg.Add(1)
		go func(r TxReceiver) {
			defer wg.Done()
			err := r.SendTx(ctx, &txIn)
			if err != nil {
				p.log.Errorw("failed to send tx", "error", err)
			}
		}(r)
	}
	wg.Wait()
}

func (p *TxProcessor) processTx(txIn common.TxIn, receipts *receiptPipeline) {
	tx := txIn.Tx
	txHashLower := strings.ToLower(tx.Hash().Hex())
	log := p.log.With("tx_hash", txHashLower).With("source", txIn.Source)
	log.Debug("processTx")

	metrics.IncTxReceived(txIn.Source)

	// Count every transaction per source
	p.srcMetrics.Inc(KeyStatsAll, txIn.Source)
	p.srcMetrics.IncKey(KeyStatsUnique, txIn.Source, tx.Hash().Hex())

	// Get output file handles (only if outDir is set)
	var outFiles OutFiles
	var err error
	if p.outDir != "" {
		var isCreated bool
		outFiles, isCreated, err = p.getOutputCSVFiles(txIn.T.Unix())
		if err != nil {
			log.Errorw("getOutputFiles", "error", err)
			return
		}
		defer outFiles.users.Dec()
		if isCreated {
			p.log.Infof("new file created: %s", outFiles.FTxs.Name())
			p.log.Infof("new file created: %s", outFiles.FSourcelog.Name())
			p.log.Infof("new file created: %s", outFiles.FTrash.Name())
		}

		// write sourcelog
		_, err = fmt.Fprintf(outFiles.FSourcelog, "%d,%s,%s\n", txIn.T.UnixMilli(), txHashLower, txIn.Source)
		if err != nil {
			log.Errorw("fmt.Fprintf", "error", err)
			return
		}
	}

	if p.clickhouse != nil {
		p.clickhouse.AddSourceLog(txIn.T, txHashLower, txIn.Source, p.location)
	}

	if receipts == nil {
		p.processCandidate(txIn, outFiles, txHashLower, log)
		return
	}
	if p.isKnown(txHashLower) {
		return
	}
	if outFiles.users != nil {
		outFiles.users.Inc()
	}
	receipts.submit(txHashLower, func() {
		if outFiles.users != nil {
			defer outFiles.users.Dec()
		}
		p.processCandidate(txIn, outFiles, txHashLower, log)
	})
}

func (p *TxProcessor) isKnown(hash string) bool {
	p.knownTxsLock.RLock()
	defer p.knownTxsLock.RUnlock()
	_, known := p.knownTxs[hash]
	return known
}

func (p *TxProcessor) processCandidate(txIn common.TxIn, outFiles OutFiles, hash string, log *zap.SugaredLogger) {
	if p.isKnown(hash) {
		return
	}
	if err := p.validateTx(txIn); err != nil {
		p.writeInvalidTx(outFiles.FTrash, txIn, err)
		metrics.IncTxReceivedTrash(txIn.Source)
		p.srcMetrics.Inc(KeyStatsTxTrash, txIn.Source)
		return
	}

	go p.sendTxToReceivers(txIn)

	if p.ethClient != nil {
		if receipt := p.lookupReceipt(txIn, log); receipt != nil {
			p.srcMetrics.Inc(KeyStatsTxOnChain, txIn.Source)
			p.srcMetrics.Inc(KeyStatsTxTrash, txIn.Source)
			metrics.IncTxReceivedTrash(txIn.Source)
			log.Debugw("transaction already included", "block", receipt.BlockNumber)
			p.writeTrash(outFiles.FTrash, txIn, common.TrashTxAlreadyOnChain, receipt.BlockNumber.String())
			return
		}
	}

	if p.archiveTx(txIn, outFiles, hash, log) {
		p.knownTxsLock.Lock()
		p.knownTxs[hash] = txIn.T
		p.knownTxsLock.Unlock()
	}
}

func (p *TxProcessor) lookupReceipt(txIn common.TxIn, log *zap.SugaredLogger) *types.Receipt {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), receiptTimeout)
	defer cancel()

	receipt, err := p.ethClient.TransactionReceipt(ctx, txIn.Tx.Hash())
	outcome := "not_found"
	switch {
	case err == nil:
		outcome = "included"
	case errors.Is(err, ethereum.NotFound):
	default:
		receipt = nil // a response can be partially decoded before an error
		outcome = "error"
		if errors.Is(err, context.DeadlineExceeded) {
			outcome = "timeout"
		}
		log.Errorw("ethClient.TransactionReceipt", "error", err)
	}
	metrics.ObserveReceiptLookup(start, outcome)
	return receipt
}

func (p *TxProcessor) archiveTx(txIn common.TxIn, outFiles OutFiles, txHashLower string, log *zap.SugaredLogger) bool {
	// Add tx hash to Redis asynchronously via background workers.
	// Protect will check this to filter txs coming back through.
	if p.redis != nil {
		p.redis.AddTx(txHashLower)
	}

	// Add transaction to Clickhouse
	if p.clickhouse != nil {
		err := p.clickhouse.AddTransaction(txIn) // send to Clickhouse
		if err != nil {
			log.Errorw("failed to add transaction to Clickhouse", "error", err)
		}
	}

	// Total unique tx count
	p.txCnt.Inc()

	// count first transactions per source (i.e. who delivers a given tx first)
	p.srcMetrics.Inc(KeyStatsFirst, txIn.Source)
	metrics.IncTxReceivedFirst(txIn.Source)

	// write the transaction file (only if outDir is set)
	if p.outDir != "" {
		// create tx rlp
		rlpHex, err := common.TxToRLPString(txIn.Tx)
		if err != nil {
			log.Errorw("failed to encode rlp", "error", err)
			return false
		}

		_, err = fmt.Fprintf(outFiles.FTxs, "%d,%s,%s\n", txIn.T.UnixMilli(), txHashLower, rlpHex)
		if err != nil {
			log.Errorw("fmt.Fprintf", "error", err)
			return false
		}
	}

	return true
}

func (p *TxProcessor) writeTrash(fTrash *os.File, txIn common.TxIn, message, notes string) {
	if fTrash == nil {
		return // skip writing if file handle is nil (no-write mode)
	}

	txHashLower := strings.ToLower(txIn.Tx.Hash().Hex())
	_, err := fmt.Fprintf(fTrash, "%d,%s,%s,%s,%s\n", txIn.T.UnixMilli(), txHashLower, txIn.Source, message, notes)
	if err != nil {
		p.log.With("tx_hash", txHashLower).With("source", txIn.Source).Errorw("fmt.Fprintf", "error", err)
	}
}

func (p *TxProcessor) writeInvalidTx(fTrash *os.File, txIn common.TxIn, err error) {
	if fTrash == nil || err == nil {
		return // skip writing if file handle is nil (no-write mode) or no error
	}

	var message, notes string
	switch {
	case errors.Is(err, common.ErrChainIDNotSet):
		message = "chainId not set"
	case errors.Is(err, errInvalidSender):
		message = common.TrashTxSignatureError
	case errors.Is(err, txpool.ErrNegativeValue):
		message = "negative value"
	case errors.Is(err, core.ErrFeeCapVeryHigh):
		message = "extremely large gasFeeCap"
	case errors.Is(err, core.ErrTipVeryHigh):
		message = "extremely large gasTipCap"
	case errors.Is(err, core.ErrTipAboveFeeCap):
		message = "gasFeeCap lower than gasTipCap"
	case errors.Is(err, errBlobMissingSidecar):
		message = "invalid blob transaction"
	default:
		message = "validation error"
		notes = err.Error()
	}
	p.writeTrash(fTrash, txIn, message, notes)
}

func (p *TxProcessor) validateTx(txIn common.TxIn) error { // inspired by https://github.com/flashbots/suave-geth/blob/dd3875eccde5b11feb621f10d9aae6417c98bdb0/core/txpool/txpool.go#L600
	tx := txIn.Tx
	txHashLower := strings.ToLower(tx.Hash().Hex())
	log := p.log.With("tx_hash", txHashLower).With("source", txIn.Source)

	if tx.ChainId().Sign() <= 0 {
		log.Debugf("error: invalid chainID %w", tx.ChainId())
		return common.ErrChainIDNotSet
	}

	// Make sure the transaction is signed properly.
	if _, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx); err != nil {
		log.Debugw("error: transaction signature incorrect")
		return errors.Join(errInvalidSender, err)
	}

	if tx.Value().Sign() < 0 {
		log.Debugw("error: transaction with negative value")
		return txpool.ErrNegativeValue
	}

	// Sanity check for extremely large numbers
	if tx.GasFeeCap().BitLen() > 256 {
		log.Debugw("error: transaction gasFeeCap extremely large")
		return core.ErrFeeCapVeryHigh
	}

	if tx.GasTipCap().BitLen() > 256 {
		log.Debugw("error: gasTipCap extremely large")
		return core.ErrTipVeryHigh
	}

	// Ensure gasFeeCap is greater than or equal to gasTipCap.
	if tx.GasFeeCapIntCmp(tx.GasTipCap()) < 0 {
		log.Debugw("error: transaction gasFeeCap lower than gasTipCap")
		return core.ErrTipAboveFeeCap
	}

	// Ensure blob txs are correctly formed
	if err := p.validateBlobTx(tx); err != nil {
		log.Debugw("error: invalid blob transaction", "reason", err)
		return err
	}

	// all good
	return nil
}

// validateBlobTx ensures that a blob tx is capable of being consumed
// by our system.  Namely, the blob tx should be in the "full" PooledTransactions
// network representation with the full sidecar available.
func (p *TxProcessor) validateBlobTx(tx *types.Transaction) error {
	if tx.Type() != types.BlobTxType {
		return nil
	}

	if tx.BlobTxSidecar() == nil {
		return errBlobMissingSidecar
	}

	return nil
}

// getOutputCSVFiles returns two file handles - one for the transactions and one for source stats, if needed - and a boolean indicating whether the file was created
func (p *TxProcessor) getOutputCSVFiles(timestamp int64) (outFiles OutFiles, isCreated bool, err error) {
	// bucketTS := timestamp / secPerDay * secPerDay // down-round timestamp to start of bucket
	sec := int64(bucketMinutes * 60)
	bucketTS := timestamp / sec * sec // timestamp down-round to start of bucket
	t := time.Unix(bucketTS, 0).UTC()

	// files may already be opened
	p.outFilesLock.RLock()
	outFiles, outFilesOk := p.outFiles[bucketTS]
	if outFilesOk {
		outFiles.users.Inc()
	}
	p.outFilesLock.RUnlock()

	if outFilesOk {
		return outFiles, false, nil
	}
	// open transactions output files
	dir := filepath.Join(p.outDir, t.Format(time.DateOnly), "transactions")
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return OutFiles{}, false, err
	}

	fn := filepath.Join(dir, p.getFilename("txs", bucketTS))
	fTx, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return OutFiles{}, false, err
	}

	// open sourcelog for writing
	dir = filepath.Join(p.outDir, t.Format(time.DateOnly), "sourcelog")
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return OutFiles{}, false, err
	}

	fn = filepath.Join(dir, p.getFilename("src", bucketTS))
	fSourcelog, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return OutFiles{}, false, err
	}

	// open trash for writing
	dir = filepath.Join(p.outDir, t.Format(time.DateOnly), "trash")
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return OutFiles{}, false, err
	}

	fn = filepath.Join(dir, p.getFilename("trash", bucketTS))
	fTrash, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return OutFiles{}, false, err
	}

	outFiles = OutFiles{
		FTxs:       fTx,
		FSourcelog: fSourcelog,
		FTrash:     fTrash,
		users:      atomic.NewInt64(1),
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

// startHousekeeper is an endless loop to clean up old transactions from the cache, log information, ping healthchecks.io, etc.
func (p *TxProcessor) startHousekeeper() {
	for {
		// Call healthchecks.io
		go p.healthCheckCall()

		// Wait one minute in between runs
		time.Sleep(time.Minute)

		// Remove old transactions from cache
		p.knownTxsLock.Lock()
		cachedBefore := len(p.knownTxs)
		for k, v := range p.knownTxs {
			if time.Since(v) > txCacheTime {
				delete(p.knownTxs, k)
			}
		}
		cachedAfter := len(p.knownTxs)
		p.knownTxsLock.Unlock()

		// Remove old files from cache
		p.outFilesLock.Lock()
		filesBefore := len(p.outFiles)
		for timestamp, outFiles := range p.outFiles {
			usageSec := bucketMinutes * 60 * 2
			if outFiles.users.Load() == 0 && time.Now().UTC().Unix()-timestamp > int64(usageSec) { // remove all handles from 2x usage seconds ago
				p.log.Infow("closing output files", "timestamp", timestamp)
				delete(p.outFiles, timestamp)
				_ = outFiles.FTxs.Close()
				_ = outFiles.FSourcelog.Close()
				_ = outFiles.FTrash.Close()
			}
		}
		filesAfter := len(p.outFiles)
		p.outFilesLock.Unlock()

		// Get memory stats
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		// Print stats
		p.log.Infow("stats",
			"txcache_before", common.Printer.Sprint(cachedBefore),
			"txcache_after", common.Printer.Sprint(cachedAfter),
			"txcache_removed", common.Printer.Sprint(cachedBefore-cachedAfter),
			"files_before", filesBefore,
			"files_after", filesAfter,
			"goroutines", common.Printer.Sprint(runtime.NumGoroutine()),
			"alloc_mb", m.Alloc/1024/1024,
			"num_gc", common.Printer.Sprint(m.NumGC),
			"tx_per_min", common.Printer.Sprint(p.txCnt.Load()),
		)

		// print source stats
		p.srcMetrics.Logger(p.log, KeyStatsFirst, false).Info("source_stats/first")
		p.srcMetrics.Logger(p.log, KeyStatsAll, false).Info("source_stats/all")
		p.srcMetrics.Logger(p.log, KeyStatsUnique, true).Info("source_stats/unique")
		p.srcMetrics.Logger(p.log, KeyStatsTxTrash, false).Info("source_stats/trash")

		// reset counters
		p.srcMetrics.Reset()
		p.txCnt.Store(0)
	}
}

func (p *TxProcessor) healthCheckCall() {
	if healthChecksIOURL == "" {
		return
	}

	if time.Since(p.lastHealthCheckCall) < time.Minute*5 {
		return
	}

	resp, err := http.Get(healthChecksIOURL) //nolint:gosec
	if err != nil {
		p.log.Errorw("[HealthCheckCall] ERROR", "error", err)
		return
	}
	resp.Body.Close()
}
