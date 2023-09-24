package common

import (
	"archive/zip"
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"
)

// LoadTransactionCSVFiles loads transaction CSV files into a map[txHash]*TxSummaryEntry
// All transactions occurring in []knownTxsFiles are skipped
func LoadTransactionCSVFiles(log *zap.SugaredLogger, txInputFiles, txBlacklistFiles []string) (txs map[string]*TxSummaryEntry, err error) {
	// load previously known transaction hashes
	prevKnownTxs, err := LoadTxHashesFromMetadataCSVFiles(log, txBlacklistFiles)
	if err != nil {
		log.Errorw("LoadTxHashesFromMetadataCSVFiles", "error", err)
		return nil, err
	}
	log.Infow("Loaded previously known transactions", "txTotal", Printer.Sprintf("%d", len(prevKnownTxs)), "memUsed", GetMemUsageHuman())

	cntProcessedFiles := 0
	txs = make(map[string]*TxSummaryEntry)
	for _, filename := range txInputFiles {
		log.Infof("Loading %s ...", filename)
		cntProcessedFiles += 1

		if strings.HasSuffix(filename, ".csv") {
			readFile, err := os.Open(filename)
			if err != nil {
				log.Errorw("os.Open", "error", err, "file", filename)
				return nil, err
			}
			defer readFile.Close()
			err = readTxFile(log, readFile, prevKnownTxs, &txs, true)
			if err != nil {
				log.Errorw("readTxFile", "error", err, "file", filename)
				return nil, err
			}
		} else if strings.HasSuffix(filename, ".csv.zip") {
			zipReader, err := zip.OpenReader(filename)
			if err != nil {
				return nil, err
			}
			defer zipReader.Close()

			for _, f := range zipReader.File {
				if !strings.HasSuffix(f.Name, ".csv") {
					continue
				}

				r, err := f.Open()
				if err != nil {
					return nil, err
				}
				defer r.Close()
				err = readTxFile(log, r, prevKnownTxs, &txs, true)
				if err != nil {
					log.Errorw("readTxFile", "error", err, "file", filename)
					return nil, err
				}
			}
		} else {
			log.Errorf("Unknown file type: %s", filename)
			return nil, ErrUnsupportedFileFormat
		}

		log.Infow("Processed file",
			"txTotal", Printer.Sprintf("%d", len(txs)),
			"memUsed", GetMemUsageHuman(),
		)
	}

	return txs, nil
}

// readTxFile reads a single transaction CSV file line-by-line
func readTxFile(log *zap.SugaredLogger, rd io.Reader, prevKnownTxs map[string]bool, txs *map[string]*TxSummaryEntry, logProgress bool) (err error) {
	cnt := 0
	fileReader := bufio.NewReader(rd)
	for {
		l, err := fileReader.ReadString('\n')
		if len(l) == 0 && err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		if len(l) < 66 {
			continue
		}

		l = strings.Trim(l, "\n")
		items := strings.Split(l, ",") // timestamp,hash,rlp
		if len(items) != 3 {
			log.Warnw("invalid line", "line", l)
			continue
		}

		ts, err := strconv.Atoi(items[0])
		if err != nil {
			log.Warnw("strconv.Atoi", "error", err, "line", l)
			continue
		}
		txTimestamp := int64(ts)
		txHash := strings.ToLower(items[1])

		// Don't store transactions that were already seen previously (in knownTxsFiles)
		if prevKnownTxs[txHash] {
			log.Debugf("Skipping tx that was already seen previously: %s", txHash)
			continue
		}

		// Dedupe transactions, and make sure to store the lowest timestamp
		if _, ok := (*txs)[txHash]; ok {
			log.Debugf("Skipping duplicate tx: %s", txHash)
			if txTimestamp < (*txs)[txHash].Timestamp {
				(*txs)[txHash].Timestamp = txTimestamp
				log.Debugw("Updating timestamp for duplicate tx", "line", l)
			}
			continue
		}

		// Process this tx
		txSummary, _, err := ParseTx(txTimestamp, items[2])
		if err != nil {
			log.Errorw("parseTx", "error", err, "line", l)
			continue
		}

		// Add to map
		(*txs)[txHash] = &txSummary

		cnt += 1
		if logProgress && cnt%100000 == 0 {
			log.Infof("- loaded %s rows", PrettyInt(cnt))
		}
	}

	return nil
}

func ParseTx(timestampMs int64, rawTxHex string) (TxSummaryEntry, *types.Transaction, error) {
	tx, err := RLPStringToTx(rawTxHex)
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

	rawTxBytes, err := tx.MarshalBinary()
	if err != nil {
		return TxSummaryEntry{}, nil, err
	}

	return TxSummaryEntry{ //nolint:exhaustruct
		Timestamp: timestampMs,
		Hash:      tx.Hash().Hex(),

		ChainID:   tx.ChainId().String(),
		From:      strings.ToLower(from.Hex()),
		To:        strings.ToLower(to),
		Value:     tx.Value().String(),
		Nonce:     fmt.Sprint(tx.Nonce()),
		Gas:       fmt.Sprint(tx.Gas()),
		GasPrice:  tx.GasPrice().String(),
		GasTipCap: tx.GasTipCap().String(),
		GasFeeCap: tx.GasFeeCap().String(),

		DataSize:   int64(len(tx.Data())),
		Data4Bytes: data4Bytes,

		RawTx:   string(rawTxBytes),
		Sources: []string{},
	}, tx, nil
}

// LoadTxHashesFromMetadataCSVFiles loads transaction hashes from metadata CSV (or .csv.zip) files into a map[txHash]bool
func LoadTxHashesFromMetadataCSVFiles(log *zap.SugaredLogger, files []string) (txs map[string]bool, err error) {
	txs = make(map[string]bool)

	for _, filename := range files {
		log.Infof("Loading tx hashes from %s ...", filename)

		rows, err := GetCSV(filename)
		if err != nil {
			log.Errorw("GetCSV", "error", err)
			return nil, err
		}

		for _, record := range rows {
			if len(record) < 2 {
				log.Errorw("invalid line", "line", record)
				continue
			}

			txHash := strings.ToLower(record[1])
			if len(txHash) < 66 {
				continue
			}
			txs[txHash] = true
		}
	}

	return txs, nil
}
