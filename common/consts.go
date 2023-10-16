package common

import "strings"

const (
	SourceTagLocal      = "local"
	SourceTagBloxroute  = "bloxroute"
	SourceTagChainbound = "chainbound"
	SourceTagMerkle     = "merkle"
	SourceTagEden       = "eden"
	SourceTagAlchemy    = "alchemy"
	SourceTagInfura     = "infura"

	// Trash tx reasons
	TrashTxAlreadyOnChain = "tx-already-onchain"

	// GRPCWindowSize is recommended window size by bloxroute-labs:
	// https://docs.bloxroute.com/streams/working-with-streams/creating-a-subscription/grpc
	GRPCWindowSize = 128 * 1024

	// TxAlreadyIncludedThreshold sets the threshold for discarding transactions (if included that many ms before received)
	TxAlreadyIncludedThreshold = 12_000
)

func TxSourcName(uri string) string {
	sourceAlias := SourceAliasesFromEnv()
	if alias, ok := sourceAlias[uri]; ok {
		return alias
	}

	if strings.Contains(uri, "alchemy.com/") {
		return SourceTagAlchemy
	}

	if strings.Contains(uri, "infura.io/") {
		return SourceTagInfura
	}

	return uri
}
