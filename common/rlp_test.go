package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRLPCoding takes RLP encoded transactions of various types, decodes them and encodes them again into EIP-2718 typed TX envelope (https://eips.ethereum.org/EIPS/eip-2718)
func TestRLPCoding(t *testing.T) {
	rlpTests := []struct {
		hash   string
		rlpIn  string
		rlpOut string
	}{
		// Typed tx envelope
		{
			hash:   "0x30c34b78c15f082c75374849677e24c9797004395b77bd88ea01114c4d0ad371",
			rlpIn:  "0x02f868058080808094f0d9b927f64374f0b48cbe56bc6af212d52ee25a880de0b6b3a764000080c080a03b5086c500757105dbb8c61a8aefce8e496451173e1bec27460a4071522aee79a03cea79b45d6946667f914c86899a761a9c2202512203d858079ae0443e6f776d",
			rlpOut: "0x02f868058080808094f0d9b927f64374f0b48cbe56bc6af212d52ee25a880de0b6b3a764000080c080a03b5086c500757105dbb8c61a8aefce8e496451173e1bec27460a4071522aee79a03cea79b45d6946667f914c86899a761a9c2202512203d858079ae0443e6f776d",
		},

		// Legacy tx
		{
			hash:   "0x470273031fc9ed469bf820795fc7528b9f698a5d33a055eab640637880b66c08",
			rlpIn:  "0xb87802f875018201088459682f00850a3cc5ac918252089404be5b8576fc23164b9ee69577fe7857dd6be1988802c346682d9a485880c080a08679e43c770c07395663fbb7fa0d2a8ca9b9535e598c25b9794c50e664c5098ca0366a741acdb68a37df66547001cf31e0c630477f78482d3b7a5778f30c6fbfe1",
			rlpOut: "0x02f875018201088459682f00850a3cc5ac918252089404be5b8576fc23164b9ee69577fe7857dd6be1988802c346682d9a485880c080a08679e43c770c07395663fbb7fa0d2a8ca9b9535e598c25b9794c50e664c5098ca0366a741acdb68a37df66547001cf31e0c630477f78482d3b7a5778f30c6fbfe1",
		},

		// Typed tx from legacy tx
		{
			hash:   "0x470273031fc9ed469bf820795fc7528b9f698a5d33a055eab640637880b66c08",
			rlpIn:  "0x02f875018201088459682f00850a3cc5ac918252089404be5b8576fc23164b9ee69577fe7857dd6be1988802c346682d9a485880c080a08679e43c770c07395663fbb7fa0d2a8ca9b9535e598c25b9794c50e664c5098ca0366a741acdb68a37df66547001cf31e0c630477f78482d3b7a5778f30c6fbfe1",
			rlpOut: "0x02f875018201088459682f00850a3cc5ac918252089404be5b8576fc23164b9ee69577fe7857dd6be1988802c346682d9a485880c080a08679e43c770c07395663fbb7fa0d2a8ca9b9535e598c25b9794c50e664c5098ca0366a741acdb68a37df66547001cf31e0c630477f78482d3b7a5778f30c6fbfe1",
		},
	}

	for _, tt := range rlpTests {
		// decode RLP to TX
		tx, err := RLPStringToTx(tt.rlpIn)
		require.NoError(t, err)
		require.Equal(t, tt.hash, tx.Hash().Hex())

		// marshalBinary TX to RLP2
		rlpHex2, err := TxToRLPString(tx)
		require.NoError(t, err)
		require.Equal(t, tt.rlpOut, rlpHex2)

		// decode RLP2 to TX2
		tx2, err := RLPStringToTx(rlpHex2)
		require.NoError(t, err)
		require.Equal(t, tt.hash, tx2.Hash().Hex())
	}
}
