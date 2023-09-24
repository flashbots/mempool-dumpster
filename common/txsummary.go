package common

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

var TxSummaryEntryCSVHeader = []string{
	"timestamp_ms",
	"hash",
	"chain_id",
	"from",
	"to",
	"value",
	"nonce",
	"gas",
	"gas_price",
	"gas_tip_cap",
	"gas_fee_cap",
	"data_size",
	"data_4bytes",
	"sources",
	"included_at_block_height",
	"included_block_timestamp_ms",
	"inclusion_delay_ms",
}

type TxSummaryEntryNoRaw struct {
	// The fields are written to CSV, and the order shouldn't change (for backwards compatibility)
	Timestamp int64  `parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Hash      string `parquet:"name=hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`

	ChainID string `parquet:"name=chainId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	From    string `parquet:"name=from, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	To      string `parquet:"name=to, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Value   string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	Nonce   string `parquet:"name=nonce, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`

	Gas       string `parquet:"name=gas, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	GasPrice  string `parquet:"name=gasPrice, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	GasTipCap string `parquet:"name=gasTipCap, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	GasFeeCap string `parquet:"name=gasFeeCap, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`

	DataSize   int64  `parquet:"name=dataSize, type=INT64"`
	Data4Bytes string `parquet:"name=data4Bytes, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	Sources []string `parquet:"name=sources, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`

	// Inclusion stats
	IncludedAtBlockHeight  int64 `parquet:"name=includedAtBlockHeight, type=INT64"`
	IncludedBlockTimestamp int64 `parquet:"name=includedBlockTimestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	InclusionDelayMs       int64 `parquet:"name=inclusionDelayMs, type=INT64"`
}

// TxSummaryEntry is a struct that represents a single transaction in the summary CSV and Parquet file
// see also https://github.com/xitongsys/parquet-go for more details on parquet tags
type TxSummaryEntry struct {
	// The fields are written to CSV, and the order shouldn't change (for backwards compatibility)
	Timestamp int64  `parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Hash      string `parquet:"name=hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`

	ChainID string `parquet:"name=chainId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	From    string `parquet:"name=from, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	To      string `parquet:"name=to, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Value   string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	Nonce   string `parquet:"name=nonce, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`

	Gas       string `parquet:"name=gas, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	GasPrice  string `parquet:"name=gasPrice, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	GasTipCap string `parquet:"name=gasTipCap, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	GasFeeCap string `parquet:"name=gasFeeCap, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`

	DataSize   int64  `parquet:"name=dataSize, type=INT64"`
	Data4Bytes string `parquet:"name=data4Bytes, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	Sources []string `parquet:"name=sources, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`

	// Inclusion stats
	IncludedAtBlockHeight  int64 `parquet:"name=includedAtBlockHeight, type=INT64"`
	IncludedBlockTimestamp int64 `parquet:"name=includedBlockTimestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	InclusionDelayMs       int64 `parquet:"name=inclusionDelayMs, type=INT64"`

	// Finally, the raw transaction (not written to CSV)
	RawTx string `parquet:"name=rawTx, type=BYTE_ARRAY, encoding=PLAIN, omitstats=true"`
}

func (t *TxSummaryEntry) HasSource(src string) bool {
	for _, s := range t.Sources {
		if s == src {
			return true
		}
	}
	return false
}

func (t *TxSummaryEntry) RawTxHex() string {
	return fmt.Sprintf("0x%x", t.RawTx)
}

func (t *TxSummaryEntry) WasIncludedBeforeReceived() bool {
	threshold := math.Abs(TxAlreadyIncludedThreshold)
	return t.IncludedAtBlockHeight > 0 && t.InclusionDelayMs <= -int64(threshold)
}

func (t *TxSummaryEntry) ToCSVRow() []string {
	return []string{
		fmt.Sprint(t.Timestamp),
		t.Hash,
		t.ChainID,
		t.From,
		t.To,
		t.Value,
		fmt.Sprint(t.Nonce),
		fmt.Sprint(t.Gas),
		t.GasPrice,
		t.GasTipCap,
		t.GasFeeCap,
		fmt.Sprint(t.DataSize),
		t.Data4Bytes,
		strings.Join(t.Sources, " "),
		fmt.Sprint(t.IncludedAtBlockHeight),
		fmt.Sprint(t.IncludedBlockTimestamp),
		fmt.Sprint(t.InclusionDelayMs),
	}
}

func (t *TxSummaryEntry) UpdateInclusionStatus(ethClient *ethclient.Client) (*types.Header, error) {
	receipt, err := ethClient.TransactionReceipt(context.Background(), common.HexToHash(t.Hash))
	if err != nil {
		if err.Error() == "not found" {
			// not yet included
			return nil, nil
		} else {
			return nil, err
		}
	} else if receipt != nil {
		// already included
		t.IncludedAtBlockHeight = receipt.BlockNumber.Int64()
	}

	// Get header for the block timestamp
	header, err := ethClient.HeaderByHash(context.Background(), receipt.BlockHash)
	if err != nil {
		return nil, err
	} else {
		t.IncludedBlockTimestamp = int64(header.Time * 1000)
		t.InclusionDelayMs = t.IncludedBlockTimestamp - t.Timestamp
	}
	return header, nil
}
