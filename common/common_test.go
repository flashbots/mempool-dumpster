package common

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

var (
	test1Hash = "0xbb59e550e4730da43af01b7ae6e1d05b1df501baa4119b8ab6a3427d9b3635b1"
	test1Rlp  = "0x02f873018305643b840f2c19f08503f8bfbbb2832ab980940ed1bcc400acd34593451e76f854992198995f52808498e5b12ac080a051eb99ae13fd1ace55dd93a4b36eefa5d34e115cd7b9fd5d0ffac07300cbaeb2a0782d9ad12490b45af932d8c98cb3c2fd8c02cdd6317edb36bde2df7556fa9132"

	test2Hash         = "0xdd00ae95e4dc13fdf92682137223d697e346852a61c268faa8806b59a8cb2c9b"
	test2RlpIncorrect = "0xb87502f8720101841dcd65008502540be40082520894b2d513b9a54a999912a57b705bcadf7e71ed595c8701bf330f70d20080c001a090f9ab3c4bed558ce05b50b28a92f39d98c8974977dd0ed925d2b5f1c77a2c40a008ea8be2f31edf3467e2553c1fbabff563a4af458716434c354c771501a6168a"
	test2RlpCorrect   = "0x02f8720101841dcd65008502540be40082520894b2d513b9a54a999912a57b705bcadf7e71ed595c8701bf330f70d20080c001a090f9ab3c4bed558ce05b50b28a92f39d98c8974977dd0ed925d2b5f1c77a2c40a008ea8be2f31edf3467e2553c1fbabff563a4af458716434c354c771501a6168a"
)

// test parseTx
func TestParseTx(t *testing.T) {
	ts := int64(1693785600337)

	//
	// check the first rlp
	///
	summary, tx, err := ParseTx(ts, test1Rlp)
	require.NoError(t, err)
	require.Equal(t, ts, summary.Timestamp)
	require.Equal(t, test1Hash, summary.Hash)
	require.Equal(t, summary.Hash, tx.Hash().Hex())
	require.Equal(t, "0xd8aa8f3be2fb0c790d3579dcf68a04701c1e33db", summary.From)
	require.Equal(t, test1Rlp, summary.RawTxHex())

	// re-encode
	rlpNew, err := TxToRLPString(tx)
	require.NoError(t, err)
	require.Equal(t, test1Rlp, rlpNew)

	//
	// check the incorrect rlp... ParseTx should fix it internally
	//
	summary, tx, err = ParseTx(ts, test2RlpIncorrect)
	require.NoError(t, err)
	require.Equal(t, test2Hash, summary.Hash)
	require.Equal(t, summary.Hash, tx.Hash().Hex())
	require.Equal(t, test2RlpCorrect, summary.RawTxHex())

	// re-encoding to rlp yields a different result
	rlpNew, err = TxToRLPString(tx)
	require.NoError(t, err)
	require.Equal(t, test2RlpCorrect, rlpNew)
}

func TestParquet(t *testing.T) {
	summary, _, err := ParseTx(int64(1693785600337), test1Rlp)
	require.NoError(t, err)

	// Create a new Parquet file
	dir := t.TempDir()
	// dir := "/tmp/"
	fn := filepath.Join(dir, "test.parquet")

	// Setup parquet writer
	fw, err := local.NewLocalFileWriter(fn)
	require.NoError(t, err)
	pw, err := writer.NewParquetWriter(fw, new(TxSummaryEntry), 4)
	require.NoError(t, err)

	// Parquet config: https://parquet.apache.org/docs/file-format/configurations/
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8K
	pw.CompressionType = parquet.CompressionCodec_GZIP

	// Write to parquet
	err = pw.Write(summary)
	require.NoError(t, err)
	err = pw.WriteStop()
	require.NoError(t, err)
	fw.Close()

	//
	// Now, read the file
	//
	fr, err := local.NewLocalFileReader(fn)
	require.NoError(t, err)
	pr, err := reader.NewParquetReader(fr, new(TxSummaryEntry), 4)
	require.NoError(t, err)

	num := int(pr.GetNumRows())
	require.Equal(t, 1, num)

	entries := make([]TxSummaryEntry, 10)
	err = pr.Read(&entries)
	require.NoError(t, err)

	pr.ReadStop()
	fr.Close()

	require.Equal(t, 1, len(entries))
	tx := entries[0]

	require.Equal(t, summary.Timestamp, tx.Timestamp)
	require.Equal(t, summary.Hash, tx.Hash)
	require.Equal(t, summary.ChainID, tx.ChainID)
	require.Equal(t, summary.From, tx.From)
	require.Equal(t, summary.To, tx.To)
	require.Equal(t, summary.Value, tx.Value)
	require.Equal(t, summary.Nonce, tx.Nonce)
	require.Equal(t, summary.Gas, tx.Gas)
	require.Equal(t, summary.GasPrice, tx.GasPrice)
	require.Equal(t, summary.GasTipCap, tx.GasTipCap)
	require.Equal(t, summary.GasFeeCap, tx.GasFeeCap)
	require.Equal(t, summary.DataSize, tx.DataSize)
	require.Equal(t, summary.Data4Bytes, tx.Data4Bytes)
	require.Equal(t, summary.RawTx, tx.RawTx)

	//
	// Double-check - parse the final rawTx
	//
	summary2, _, err := ParseTx(int64(1693785600337), test1Rlp)
	require.NoError(t, err)
	require.Equal(t, summary.Hash, summary2.Hash)
}
