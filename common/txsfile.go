package common

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"
)

func LoadTransactionCSVFiles(log *zap.SugaredLogger, files []string) (txs map[string]*TxEnvelope) { //nolint:gocognit
	cntProcessedFiles := 0
	txs = make(map[string]*TxEnvelope)
	for _, filename := range files {
		log.Infof("Loading %s ...", filename)
		cntProcessedFiles += 1
		cntTxInFileTotal := 0
		cntTxInFileNew := 0

		readFile, err := os.Open(filename)
		if err != nil {
			log.Errorw("os.Open", "error", err, "file", filename)
			return
		}
		defer readFile.Close()

		fileReader := bufio.NewReader(readFile)
		for {
			l, err := fileReader.ReadString('\n')
			if len(l) == 0 && err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Errorw("fileReader.ReadString", "error", err)
				break
			}

			if len(l) < 66 {
				// log.Errorw("invalid line", "line", l)
				continue
			}

			l = strings.Trim(l, "\n")
			items := strings.Split(l, ",") // timestamp,hash,rlp
			if len(items) != 3 {
				log.Errorw("invalid line", "line", l)
				continue
			}

			cntTxInFileTotal += 1

			ts, err := strconv.Atoi(items[0])
			if err != nil {
				log.Errorw("strconv.Atoi", "error", err, "line", l)
				continue
			}
			txTimestamp := int64(ts)

			// Dedupe transactions, and make sure to store the lowest timestamp
			if _, ok := txs[items[1]]; ok {
				log.Debugf("Skipping duplicate tx: %s", items[1])

				if txTimestamp < txs[items[1]].Summary.Timestamp {
					txs[items[1]].Summary.Timestamp = txTimestamp
					log.Debugw("Updating timestamp for duplicate tx", "line", l)
				}

				continue
			}

			// Process this tx
			txSummary, _, err := parseTx(txTimestamp, items[1], items[2])
			if err != nil {
				log.Errorw("parseTx", "error", err, "line", l)
				continue
			}

			// Add to map
			txs[items[1]] = &TxEnvelope{items[2], &txSummary}
			cntTxInFileNew += 1
		}
		log.Infow("Processed file",
			"txInFile", Printer.Sprintf("%d", cntTxInFileTotal),
			"txNew", Printer.Sprintf("%d", cntTxInFileNew),
			"txTotal", Printer.Sprintf("%d", len(txs)),
			"memUsedMiB", Printer.Sprintf("%d", GetMemUsageMb()),
		)
		// break
	}
	return txs
}

func parseTx(timestampMs int64, hash, rawTx string) (TxSummaryEntry, *types.Transaction, error) {
	tx, err := RLPStringToTx(rawTx)
	if err != nil {
		return TxSummaryEntry{}, nil, err
	}

	from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
	if err != nil {
		// fmt.Println("Error: ", err)
		_ = err
	}
	// prepare 'to' address
	to := ""
	if tx.To() != nil {
		to = tx.To().Hex()
	}

	// prepare '4 bytes' of data (function name)
	data4Bytes := ""
	if len(tx.Data()) >= 4 {
		data4Bytes = hexutil.Encode(tx.Data()[:4])
	}

	return TxSummaryEntry{
		Timestamp: timestampMs,
		Hash:      tx.Hash().Hex(),

		ChainID:   tx.ChainId().String(),
		From:      from.Hex(),
		To:        to,
		Value:     tx.Value().String(),
		Nonce:     tx.Nonce(),
		Gas:       tx.Gas(),
		GasPrice:  tx.GasPrice().String(),
		GasTipCap: tx.GasTipCap().String(),
		GasFeeCap: tx.GasFeeCap().String(),

		DataSize:   int64(len(tx.Data())),
		Data4Bytes: data4Bytes,
	}, tx, nil
}
