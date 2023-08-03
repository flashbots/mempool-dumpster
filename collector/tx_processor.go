package collector

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"
)

type TxProcessor struct {
	log    *zap.SugaredLogger
	txC    chan TxIn
	outDir string

	txn     map[common.Hash]time.Time
	txnLock sync.RWMutex
}

func NewTxProcessor(log *zap.SugaredLogger, outDir string) *TxProcessor {
	return &TxProcessor{
		log:     log, //.With("module", "tx_processor"),
		txC:     make(chan TxIn, 100),
		outDir:  outDir,
		txn:     make(map[common.Hash]time.Time),
		txnLock: sync.RWMutex{},
	}
}

func (nc *TxProcessor) Start() {
	nc.log.Info("Waiting for transactions...")

	// start the txn map cleaner background task
	go nc.cleanTxnMap()

	// start listening for transactions coming in through the channel
	for txIn := range nc.txC {
		go nc.processTx(&txIn)
	}
}

func (nc *TxProcessor) processTx(txIn *TxIn) {
	txHash := txIn.tx.Hash()
	log := nc.log.With("tx_hash", txHash.Hex())
	log.Info("processTx")

	// process transactions only once
	nc.txnLock.RLock()
	_, ok := nc.txn[txHash]
	nc.txnLock.RUnlock()
	if ok {
		log.Infof("transaction already processed")
		return
	}

	// prepare rlp rawtx
	buf := new(bytes.Buffer)
	err := txIn.tx.EncodeRLP(buf)
	if err != nil {
		log.Errorw("failed to encode rlp", "error", err)
		return
	}

	// prepare signature values
	v, r, s := txIn.tx.RawSignatureValues()

	// prepare 'from' address, fails often because of unsupported tx type
	from, err := types.Sender(types.NewEIP155Signer(txIn.tx.ChainId()), txIn.tx)
	if err != nil {
		log.Debugw("failed to get sender", "error", err)
	}

	// prepare 'to' address
	to := ""
	if txIn.tx.To() != nil {
		to = txIn.tx.To().Hex()
	}

	// build the summary
	txSummary := TxSummaryJSON{
		Timestamp: txIn.t.UnixMilli(),
		Hash:      txHash.Hex(),
		RawTx:     hexutil.Encode(buf.Bytes()),

		ChainID:   txIn.tx.ChainId().String(),
		From:      from.Hex(),
		To:        to,
		Value:     txIn.tx.Value().String(),
		Nonce:     txIn.tx.Nonce(),
		Gas:       txIn.tx.Gas(),
		GasPrice:  txIn.tx.GasPrice().String(),
		GasTipCap: txIn.tx.GasTipCap().String(),
		GasFeeCap: txIn.tx.GasFeeCap().String(),

		DataSize: int64(len(txIn.tx.Data())),
		V:        v.String(),
		R:        r.String(),
		S:        s.String(),
	}

	// write json to file
	if nc.outDir != "" {
		// prepare path and ensure it exists
		dir := filepath.Join(nc.outDir, txIn.t.Format(datePath), "transactions")
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			log.Error(err)
			return
		}

		// final filename
		fn := filepath.Join(dir, txHash.Hex()+".json")

		// TODO: check if file already exists, in which case either overwrite or skip

		// write json to file
		log.Infof("writing to: %s", fn)
		content, err := json.MarshalIndent(txSummary, "", "  ")
		if err != nil {
			log.Errorw("json.MarshalIndent", "error", err)
			return
		}
		err = os.WriteFile(fn, content, 0o600)
		if err != nil {
			log.Errorw("os.WriteFile", "error", err)
			return
		}
	}

	// todo: write json to S3

	// Remember that this transaction was processed
	nc.txnLock.Lock()
	nc.txn[txHash] = txIn.t
	nc.txnLock.Unlock()
}

func (nc *TxProcessor) cleanTxnMap() {
	for {
		time.Sleep(time.Minute)

		// Check now and remove any old transactions
		nBefore := len(nc.txn)
		nc.txnLock.Lock()
		for k, v := range nc.txn {
			if time.Since(v) > txCacheTime {
				delete(nc.txn, k)
			}
		}
		nc.txnLock.Unlock()
		nc.log.Infow("cleanTxnMap", "n_before", nBefore, "n_after", len(nc.txn), "n_removed", nBefore-len(nc.txn), "goroutines", runtime.NumGoroutine())
	}
}
