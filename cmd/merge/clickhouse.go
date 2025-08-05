package cmd_merge //nolint:stylecheck

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/flashbots/mempool-dumpster/metrics"
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
		metrics.IncClickhouseError()
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
		metrics.IncClickhouseError()
		ch.log.Errorw("Failed to connect to Clickhouse", "error", err)
		return err
	}

	return nil
}
