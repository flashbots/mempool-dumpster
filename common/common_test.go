package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// test parseTx
func TestParseTx(t *testing.T) {
	ts := int64(1693785600337)
	hash := "0xbb59e550e4730da43af01b7ae6e1d05b1df501baa4119b8ab6a3427d9b3635b1"
	rlp := "0x02f873018305643b840f2c19f08503f8bfbbb2832ab980940ed1bcc400acd34593451e76f854992198995f52808498e5b12ac080a051eb99ae13fd1ace55dd93a4b36eefa5d34e115cd7b9fd5d0ffac07300cbaeb2a0782d9ad12490b45af932d8c98cb3c2fd8c02cdd6317edb36bde2df7556fa9132"
	summary, _, err := parseTx(ts, hash, rlp)
	require.NoError(t, err)
	require.Equal(t, ts, summary.Timestamp)
	require.Equal(t, hash, summary.Hash)
	require.Equal(t, "0xd8Aa8F3be2fB0C790D3579dcF68a04701C1e33DB", summary.From)
}
