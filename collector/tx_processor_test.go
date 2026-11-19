package collector

import (
	"context"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// var testLog = common.GetLogger(true, false)

// func TestBuilderAliases(t *testing.T) {
// 	tempDir := t.TempDir()
// 	txp := NewTxProcessor(testLog, tempDir, "test1")
// 	require.Equal(t, "collector", "collector")
// }

type MockTxReceiver struct {
	ReceivedTx *common.TxIn
}

type recordingTxReceiver chan ethcommon.Hash

func (r recordingTxReceiver) SendTx(ctx context.Context, tx *common.TxIn) error {
	select {
	case r <- tx.Tx.Hash():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type stalledReceiptService struct {
	hash    ethcommon.Hash
	started chan struct{}
	release chan struct{}
	receipt *types.Receipt
}

func (s *stalledReceiptService) GetTransactionReceipt(ctx context.Context, hash ethcommon.Hash) (*types.Receipt, error) {
	if hash == s.hash {
		select {
		case s.started <- struct{}{}:
		default:
		}
		select {
		case <-s.release:
			return s.receipt, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, nil
}

func startProcessorWithStalledReceipt(t *testing.T, tx *types.Transaction, opts TxProcessorOpts) (*TxProcessor, *stalledReceiptService) {
	t.Helper()
	service := &stalledReceiptService{
		hash:    tx.Hash(),
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	server := rpc.NewServer()
	t.Cleanup(server.Stop)
	require.NoError(t, server.RegisterName("eth", service))
	client := ethclient.NewClient(rpc.DialInProc(server))
	t.Cleanup(client.Close)

	processor := NewTxProcessor(opts)
	processor.ethClient = client
	go processor.startTransactionReceiverLoop()
	return processor, service
}

func (s *stalledReceiptService) waitForRequest(t *testing.T) {
	t.Helper()
	select {
	case <-s.started:
	case <-time.After(time.Second):
		t.Fatal("receipt lookup did not start")
	}
}

func TestTxProcessorForwardsWhileReceiptLookupStalls(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	txs := make([]*types.Transaction, 40)
	want := make([]ethcommon.Hash, len(txs))
	for i := range txs {
		txs[i] = rpcReceiverTestTransaction(t, key, uint64(i))
		want[i] = txs[i].Hash()
	}
	firstTx := txs[0]
	receiver := make(recordingTxReceiver, len(txs))
	processor, service := startProcessorWithStalledReceipt(t, firstTx, TxProcessorOpts{
		Log:                     zap.NewNop().Sugar(),
		AdditionalReceivers:     []TxReceiver{receiver},
		ReceiversAllowedSources: []string{"all"},
	})
	defer processor.Shutdown()
	defer close(service.release)

	processor.txC <- common.TxIn{T: time.Now(), Tx: firstTx, Source: "first"}
	service.waitForRequest(t)
	for _, tx := range txs[1:] {
		processor.txC <- common.TxIn{T: time.Now(), Tx: tx, Source: "second"}
	}

	got := make([]ethcommon.Hash, 0, len(want))
	deadline := time.After(500 * time.Millisecond)
	for range want {
		select {
		case hash := <-receiver:
			got = append(got, hash)
		case <-deadline:
			t.Fatalf("forwarded %d of %d transactions while the first receipt lookup was stalled", len(got), len(want))
		}
	}
	require.ElementsMatch(t, want, got)
}

func TestTxProcessorArchivesAfterReceiptLookupTimesOut(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	tx := rpcReceiverTestTransaction(t, key, 0)
	processor, service := startProcessorWithStalledReceipt(t, tx, TxProcessorOpts{Log: zap.NewNop().Sugar()})
	defer close(service.release)

	receivedAt := time.Now()
	processor.txC <- common.TxIn{T: receivedAt, Tx: tx, Source: "first"}
	service.waitForRequest(t)

	stopped := make(chan struct{})
	go func() {
		processor.Shutdown()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("processor did not drain while the check node was stalled")
	}
	require.Equal(t, receivedAt, processor.knownTxs[tx.Hash().Hex()])
}

func TestTxProcessorPreservesRepeatSightingsOfIncludedTransactions(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	tx := rpcReceiverTestTransaction(t, key, 0)
	receiver := make(recordingTxReceiver, 1)
	outDir := t.TempDir()
	processor, service := startProcessorWithStalledReceipt(t, tx, TxProcessorOpts{
		Log:                     zap.NewNop().Sugar(),
		OutDir:                  outDir,
		AdditionalReceivers:     []TxReceiver{receiver},
		ReceiversAllowedSources: []string{"allowed"},
	})

	service.receipt = &types.Receipt{TxHash: tx.Hash(), BlockNumber: big.NewInt(1), Logs: []*types.Log{}}
	receivedAt := time.Now()
	processor.txC <- common.TxIn{T: receivedAt, Tx: tx, Source: "other"}
	service.waitForRequest(t)
	processor.txC <- common.TxIn{T: receivedAt, Tx: tx, Source: "allowed"}
	sourceLog := filepath.Join(outDir, receivedAt.UTC().Format(time.DateOnly), "sourcelog", "*.csv")
	seen := false
	deadline := time.Now().Add(500 * time.Millisecond)
	for !seen && time.Now().Before(deadline) {
		paths, err := filepath.Glob(sourceLog)
		require.NoError(t, err)
		for _, path := range paths {
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			seen = strings.Contains(string(data), ",allowed\n")
		}
		if !seen {
			time.Sleep(time.Millisecond)
		}
	}
	close(service.release)
	processor.Shutdown()
	require.True(t, seen, "the second sighting's source log must be written before the first receipt returns")

	select {
	case hash := <-receiver:
		require.Equal(t, tx.Hash(), hash)
	case <-time.After(time.Second):
		t.Fatal("the allowed source's observation was not forwarded")
	}
	require.Empty(t, processor.knownTxs)
	require.Len(t, processor.outFiles, 1)
	for _, files := range processor.outFiles {
		sources, err := os.ReadFile(files.FSourcelog.Name())
		require.NoError(t, err)
		require.Contains(t, string(sources), ",other\n")
		require.Contains(t, string(sources), ",allowed\n")
		trash, err := os.ReadFile(files.FTrash.Name())
		require.NoError(t, err)
		require.Contains(t, string(trash), ",other,")
		require.Contains(t, string(trash), ",allowed,")
	}
}

func (r *MockTxReceiver) SendTx(ctx context.Context, tx *common.TxIn) error {
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
		AdditionalReceivers:     []TxReceiver{&receiver},
		ReceiversAllowedSources: []string{"allowed"},
	})

	tx := common.TxIn{
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
