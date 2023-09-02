// Package sourcelog deals with loading and analyzing the transaction source logs.
//
// Input: CSV file(s) with the following format:
//
//	<timestamp_ms>,<tx_hash>,<source>
package sourcelog

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/zap"
)

// LoadSourceLogFiles loads all input CSV files and returns a map[hash][source] = timestampMs
func LoadSourceLogFiles(log *zap.SugaredLogger, files []string) (txs map[string]map[string]int64) { //nolint:gocognit
	txs = make(map[string]map[string]int64)

	timestampFirst, timestampLast := int64(0), int64(0)
	cntProcessedFiles := 0
	cntProcessedRecords := int64(0)

	// Collect transactions from all input files to memory
	for _, filename := range files {
		log.Infof("Processing: %s", filename)
		cntProcessedFiles += 1
		cntTxInFileTotal := 0

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

			l = strings.Trim(l, "\n")
			items := strings.Split(l, ",") // timestamp,hash,source
			if len(items) != 3 {
				log.Errorw("invalid line", "line", l)
				continue
			}

			cntTxInFileTotal += 1

			if len(l) < 66 {
				continue
			}

			ts, err := strconv.Atoi(items[0])
			if err != nil {
				log.Errorw("strconv.Atoi", "error", err, "line", l)
				continue
			}
			txTimestamp := int64(ts)
			txHash := items[1]
			txSource := common.TxSourcName(items[2])

			// that it's a valid hash
			if len(txHash) != 66 {
				log.Errorw("invalid hash length", "hash", txHash)
				continue
			}
			if _, err = hexutil.Decode(txHash); err != nil {
				log.Errorw("hexutil.Decode", "error", err, "line", l)
				continue
			}

			cntProcessedRecords += 1

			if timestampFirst == 0 || txTimestamp < timestampFirst {
				timestampFirst = txTimestamp
			}
			if txTimestamp > timestampLast {
				timestampLast = txTimestamp
			}

			// Add entry to txs map
			if _, ok := txs[txHash]; !ok {
				txs[txHash] = make(map[string]int64)
				txs[txHash][txSource] = txTimestamp
			}

			// Update timestamp if it's earlier (i.e. alchemy often sending duplicate entries, this makes sure we record the earliest timestamp)
			if txs[txHash][txSource] == 0 || txTimestamp < txs[txHash][txSource] {
				txs[txHash][txSource] = txTimestamp
			}
		}
		log.Infow("Processed file",
			"txInFile", printer.Sprintf("%d", cntTxInFileTotal),
			// "txNew", printer.Sprintf("%d", cntTxInFileNew),
			"txTotal", printer.Sprintf("%d", len(txs)),
			"memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()),
		)
	}

	log.Infow("Processed all input files",
		"files", cntProcessedFiles,
		"records", printer.Sprintf("%d", cntProcessedRecords),
		"txTotal", printer.Sprintf("%d", len(txs)),
		"memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()),
	)

	return txs
}
