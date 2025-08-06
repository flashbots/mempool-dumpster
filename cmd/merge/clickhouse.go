package cmd_merge //nolint:stylecheck

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/zap"
)

var ErrNoDSN = fmt.Errorf("Clickhouse DSN is required")

type ClickhouseOpts struct {
	Log *zap.SugaredLogger
	DSN string
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
}

// NewClickhouse creates a new Clickhouse instance with a connection to the database.
func NewClickhouse(opts ClickhouseOpts) (*Clickhouse, error) {
	ch := &Clickhouse{
		log:  opts.Log,
		opts: opts,
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
		return fmt.Errorf("failed to parse Clickhouse DSN: %w", err)
	}

	options.Debugf = func(format string, v ...interface{}) {
		ch.log.Infof("Clickhouse debug: "+format, v...)
	}

	ch.conn, err = clickhouse.Open(options)
	if err != nil {
		return err
	}

	if err := ch.conn.Ping(ctx); err != nil {
		ch.log.Errorw("Failed to connect to Clickhouse", "error", err)
		return err
	}

	return nil
}

// loadTransactions retrieves transactions from the Clickhouse database within the specified time range (timeStart inclusive, timeEnd exclusive).
func (ch *Clickhouse) loadTransactions(timeStart, timeEnd time.Time) (txs map[string]*common.TxSummaryEntry, err error) {
	ctx := context.Background()
	rows, err := ch.conn.Query(ctx, `SELECT
		min(received_at), hash, chain_id, tx_type, from, to, value, nonce, gas, gas_price, gas_tip_cap, gas_fee_cap, data_size, data_4bytes, any(raw_tx)
	FROM transactions WHERE received_at >= ? AND received_at < ?
	GROUP BY (hash, chain_id, tx_type, from, to, value, nonce, gas, gas_price, gas_tip_cap, gas_fee_cap, data_size, data_4bytes)
	SETTINGS max_threads = 8,
			max_block_size = 65536,
			group_by_two_level_threshold = 100000`, timeStart, timeEnd)
	if err != nil {
		return nil, err
	}

	txs = make(map[string]*common.TxSummaryEntry)
	for rows.Next() {
		entry := common.TxSummaryEntry{}
		if err := rows.Scan(&entry.Timestamp, &entry.Hash, &entry.ChainID, &entry.TxType, &entry.From, &entry.To, &entry.Value, &entry.Nonce, &entry.Gas, &entry.GasPrice, &entry.GasTipCap, &entry.GasFeeCap, &entry.DataSize, &entry.Data4Bytes, &entry.RawTx); err != nil {
			return nil, err
		}
		txs[entry.Hash] = &entry
	}

	return txs, nil
}
