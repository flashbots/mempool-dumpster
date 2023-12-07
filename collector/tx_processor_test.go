package collector

import (
	"context"
	"testing"
	"time"

	"github.com/flashbots/mempool-dumpster/common"
)

// var testLog = common.GetLogger(true, false)

// func TestBuilderAliases(t *testing.T) {
// 	tempDir := t.TempDir()
// 	txp := NewTxProcessor(testLog, tempDir, "test1")
// 	require.Equal(t, "collector", "collector")
// }

type MockTxReceiver struct {
	ReceivedTx *TxIn
}

func (r *MockTxReceiver) SendTx(ctx context.Context, tx *TxIn) error {
	r.ReceivedTx = tx
	return nil
}

func TestTxProcessor_sendTxToReceivers(t *testing.T) {
	receiver := MockTxReceiver{ReceivedTx: nil}

	processor := NewTxProcessor(TxProcessorOpts{
		Log:                     common.GetLogger(true, false),
		OutDir:                  "",
		UID:                     "",
		CheckNodeURI:            "",
		HTTPReceivers:           nil,
		ReceiversAllowedSources: []string{"allowed"},
	})
	processor.receivers = append(processor.receivers, &receiver)

	tx := TxIn{
		T:      time.Now(),
		Tx:     nil,
		Source: "not-allowed",
	}
	processor.sendTxToReceivers(tx)

	if receiver.ReceivedTx != nil {
		t.Errorf("expected nil, got %v", receiver.ReceivedTx)
	}

	tx.Source = "allowed"
	processor.sendTxToReceivers(tx)
	if receiver.ReceivedTx == nil {
		t.Errorf("expected tx, got nil")
	}
}
