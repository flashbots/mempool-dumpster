package collector

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/flashbots/mempool-dumpster/metrics"
	"go.uber.org/zap"
)

var ErrNoDSN = fmt.Errorf("Clickhouse DSN is required")

type ClickhouseOpts struct {
	Log *zap.SugaredLogger

	DSN        string
	DisableTLS bool // if true, disables TLS verification for Clickhouse connections
}

type SourceLogEntry struct {
	ReceivedAt time.Time
	Hash       string
	Source     string
	Location   string
}

type Clickhouse struct {
	opts ClickhouseOpts

	log  *zap.SugaredLogger
	conn driver.Conn

	currentTxBatch []common.TxSummaryEntry // Batch of transactions to be inserted
	// currentTxBatchLock sync.Mutex

	currentSourcelogBatch []SourceLogEntry // Batch of source logs to be inserted
	// currentSourcelogBatchLock sync.Mutex
}

// NewClickhouse creates a new Clickhouse instance with a connection to the database.
func NewClickhouse(opts ClickhouseOpts) (*Clickhouse, error) {
	ch := &Clickhouse{
		log:                   opts.Log,
		opts:                  opts,
		currentTxBatch:        make([]common.TxSummaryEntry, 0, clickhouseBatchSize),
		currentSourcelogBatch: make([]SourceLogEntry, 0, clickhouseBatchSize),
	}
	if ch.opts.DSN == "" {
		return nil, ErrNoDSN
	}

	err := ch.connect()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// connect establishes a connection to the Clickhouse database using secure native protocol.
func (ch *Clickhouse) connect() error {
	ctx := context.Background()
	var err error

	// Parse the DSN to extract address and authentication details
	options, err := clickhouse.ParseDSN(ch.opts.DSN)
	if err != nil {
		metrics.IncClickhouseError()
		return fmt.Errorf("failed to parse Clickhouse DSN: %w", err)
	}

	var chTLS *tls.Config
	if !ch.opts.DisableTLS {
		chTLS = &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec
		}
	}

	ch.conn, err = clickhouse.Open(&clickhouse.Options{
		Addr: options.Addr,
		Auth: options.Auth,
		TLS:  chTLS,
		Debugf: func(format string, v ...interface{}) {
			ch.log.Infof("Clickhouse debug: "+format, v...)
		},
	})
	if err != nil {
		return err
	}

	if err := ch.conn.Ping(ctx); err != nil {
		metrics.IncClickhouseError()
		ch.log.Errorw("Failed to connect to Clickhouse", "error", err)
		return err
	}

	return nil
}

// AddTransaction adds a transaction to the Clickhouse batch. If the batch size exceeds the configured limit, it sends the batch to Clickhouse.
func (ch *Clickhouse) AddTransaction(tx common.TxIn) error {
	txSummary, _, err := common.ParseTx(tx.T.UnixMilli(), tx.Tx)
	if err != nil {
		metrics.IncClickhouseError()
		return fmt.Errorf("failed to parse transaction: %w", err)
	}

	// First, check if the current batch is full, in which case we need to send it to Clickhouse
	// ch.currentTxBatchLock.Lock()
	// defer ch.currentTxBatchLock.Unlock()
	if len(ch.currentTxBatch) >= clickhouseBatchSize {
		// Create a copy of the batche and save it to Clickhouse (with retries)
		txs := make([]common.TxSummaryEntry, clickhouseBatchSize)
		copy(txs, ch.currentTxBatch)
		go ch.saveTxs(txs)

		// Reset the current batche
		ch.currentTxBatch = make([]common.TxSummaryEntry, 0, clickhouseBatchSize)
	}

	// Add item to batches
	ch.currentTxBatch = append(ch.currentTxBatch, txSummary)
	return nil
}

// saveTxs saves the current batch of transactions to Clickhouse, with retries. Expected to be run in a background goroutine.
func (ch *Clickhouse) saveTxs(txs []common.TxSummaryEntry) {
	batch, err := ch.conn.PrepareBatch(context.Background(), "INSERT INTO transactions")
	if err != nil {
		metrics.IncClickhouseError()
		ch.log.Errorw("Failed to prepare Clickhouse batch insert", "error", err)
		return
	}

	for _, tx := range txs {
		err := batch.Append(
			tx.Timestamp,
			tx.Hash,
			tx.ChainID,
			tx.TxType,
			tx.From,
			tx.To,
			tx.Value,
			tx.Nonce,
			tx.Gas,
			tx.GasPrice,
			tx.GasTipCap,
			tx.GasFeeCap,
			tx.DataSize,
			tx.Data4Bytes,
			tx.RawTxHex(),
		)
		if err != nil {
			metrics.IncClickhouseError()
			ch.log.Errorw("Failed to append transaction to Clickhouse batch", "error", err, "txHash", tx.Hash)
		}
	}

	ch.sendBatchWithRetries("transactions", batch)
}

// AddSourceLog adds a source log to the Clickhouse batch. If the batch size exceeds the configured limit, it sends the batch to Clickhouse.
func (ch *Clickhouse) AddSourceLog(timeReceived time.Time, hash, source, location string) error {
	// ch.currentSourcelogBatchLock.Lock()
	// defer ch.currentSourcelogBatchLock.Unlock()
	if len(ch.currentSourcelogBatch) >= clickhouseBatchSize {
		// Time to save the batch. Create a copy of the batches and send it off to save to Clickhouse (with retries)
		sourcelogs := make([]SourceLogEntry, clickhouseBatchSize)
		copy(sourcelogs, ch.currentSourcelogBatch)
		go ch.saveSourcelogs(sourcelogs)

		// Reset the current batche
		ch.currentSourcelogBatch = make([]SourceLogEntry, 0, clickhouseBatchSize)
	}

	// Add item to batches
	ch.currentSourcelogBatch = append(ch.currentSourcelogBatch, SourceLogEntry{
		ReceivedAt: timeReceived,
		Hash:       hash,
		Source:     source,
		Location:   location,
	})

	return nil
}

// saveTxs saves the current batch of transactions to Clickhouse, with retries. Expected to be run in a background goroutine.
func (ch *Clickhouse) saveSourcelogs(sourcelogs []SourceLogEntry) {
	batch, err := ch.conn.PrepareBatch(context.Background(), "INSERT INTO sourcelogs")
	if err != nil {
		metrics.IncClickhouseError()
		ch.log.Errorw("Failed to prepare Clickhouse batch insert", "error", err)
		return
	}

	for _, log := range sourcelogs {
		err := batch.Append(
			log.ReceivedAt,
			log.Hash,
			log.Source,
			log.Location,
		)
		if err != nil {
			metrics.IncClickhouseError()
			ch.log.Errorw("Failed to append source log to Clickhouse batch", "error", err, "logHash", log.Hash)
			return
		}
	}

	ch.sendBatchWithRetries("sourcelogs", batch)
}

func (ch *Clickhouse) sendBatchWithRetries(name string, batch driver.Batch) {
	retryCount := 0

	timeStarted := time.Now()
	ch.log.Debugw("Starting Clickhouse batch save", "name", name, "size", batch.Rows())

	for {
		// Save batch
		err := batch.Send()
		if err == nil {
			// Successfully sent the batch
			timeElapsed := time.Since(timeStarted)
			ch.log.Infow("Successfully saved Clickhouse batch", "name", name, "size", batch.Rows(), "retryCount", retryCount, "timeElapsedMs", timeElapsed.Milliseconds())
			metrics.IncClickhouseBatchSaveSuccess()
			metrics.AddClickhouseBatchSaveDurationMilliseconds(name, timeElapsed.Milliseconds())
			metrics.AddClickhouseEntriesSaved(name, batch.Rows())
			return
		}

		// There was an error saving the batch. Log the error and possibly retry.
		metrics.IncClickhouseErrorBatchSave()
		ch.log.Errorw("Failed to save Clickhouse batch", "name", name, "error", err)

		retryCount++
		if retryCount >= clickhouseSaveRetries {
			metrics.IncClickhouseBatchSaveGiveup()
			ch.log.Errorw("Max retries reached, giving up on Clickhouse batch", "name", name, "retryCount", retryCount)
			return
		}

		sleepTime := time.Duration(retryCount*3) * time.Second
		ch.log.Infow("Retrying to save Clickhouse batch", "name", name, "retryCount", retryCount, "sleepTime", sleepTime)
		time.Sleep(sleepTime)
		metrics.IncClickhouseBatchSaveRetries()
	}
}
