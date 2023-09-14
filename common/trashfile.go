package common

import (
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"go.uber.org/zap"
)

type TrashEntry struct {
	Timestamp int64
	Hash      string
	Source    string
	Reason    string
	Notes     string
}

func (entry *TrashEntry) TrashEntryToCSVRow() string {
	fields := []string{
		strconv.Itoa(int(entry.Timestamp)),
		entry.Hash,
		entry.Source,
		entry.Reason,
		entry.Notes,
	}
	return strings.Join(fields, ",")
}

func NewTrashEntryFromCSVRow(row []string) *TrashEntry {
	if len(row) < 4 {
		return nil
	}

	ts, err := strconv.Atoi(row[0])
	if err != nil {
		return nil
	}
	txTimestamp := int64(ts)
	txHash := strings.ToLower(row[1])
	txSource := TxSourcName(row[2])
	txReason := row[3]
	txNotes := ""
	if len(row) >= 5 {
		txNotes = row[4]
	}

	// that it's a valid hash
	if len(txHash) != 66 {
		return nil
	}
	if _, err = hexutil.Decode(txHash); err != nil {
		return nil
	}

	return &TrashEntry{
		Timestamp: txTimestamp,
		Hash:      txHash,
		Source:    txSource,
		Reason:    txReason,
		Notes:     txNotes,
	}
}

// LoadTrashFiles loads sourcelog .csv (or .csv.zip) files (format: <timestamp_ms>,<tx_hash>,<source>) and returns a map[hash][source] = *TrashEntry
func LoadTrashFiles(log *zap.SugaredLogger, files []string) (txs map[string]map[string]*TrashEntry, err error) {
	txs = make(map[string]map[string]*TrashEntry)

	rows, err := GetCSVFromFiles(files)
	if err != nil {
		return txs, err
	}

	for _, items := range rows {
		if len(items) < 4 {
			continue
		}

		// discard CSV header
		if txHashLower := strings.ToLower(items[1]); len(txHashLower) < 66 {
			continue
		}

		entry := NewTrashEntryFromCSVRow(items)
		if entry == nil {
			log.Errorw("invalid line", "line", items)
			continue
		}

		// Add entry to txs map
		if _, ok := txs[entry.Hash]; !ok {
			txs[entry.Hash] = make(map[string]*TrashEntry)
			txs[entry.Hash][entry.Source] = entry
		}

		// Use earliest known timestamp (i.e. alchemy often sending duplicate entries, this makes sure we record the earliest timestamp)
		if txs[entry.Hash][entry.Source] == nil || entry.Timestamp < txs[entry.Hash][entry.Source].Timestamp {
			txs[entry.Hash][entry.Source] = entry
		}
	}

	return txs, nil
}
