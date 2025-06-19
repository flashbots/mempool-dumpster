package collector

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
)

var ErrNoDSN = fmt.Errorf("Clickhouse DSN is required")

type ClickhouseOpts struct {
	DSN string
	Log *zap.SugaredLogger
}

type Clickhouse struct {
	opts ClickhouseOpts

	log  *zap.SugaredLogger
	conn driver.Conn
}

// NewClickhouse creates a new Clickhouse instance with a connection to the database.
func NewClickhouse(opts ClickhouseOpts) (*Clickhouse, error) {
	ch := &Clickhouse{log: opts.Log, opts: opts}
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

	ch.conn, err = clickhouse.Open(&clickhouse.Options{
		Addr: options.Addr,
		Auth: options.Auth,
		Debugf: func(format string, v ...interface{}) {
			ch.log.Infof("Clickhouse debug: "+format, v...)
		},
	})
	if err != nil {
		return err
	}

	if err := ch.conn.Ping(ctx); err != nil {
		ch.log.Errorw("Failed to connect to Clickhouse", "error", err)
		return err
	}

	return nil
}
