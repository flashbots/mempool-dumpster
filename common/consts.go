package common

import "strings"

const (
	SourceTagLocal      = "local"
	SourceTagBloxroute  = "bloxroute"
	SourceTagChainbound = "chainbound"
	SourceTagEden       = "eden"
)

// GRPCWindowSize is recommended window size by bloxroute-labs:
// https://docs.bloxroute.com/streams/working-with-streams/creating-a-subscription/grpc
const GRPCWindowSize = 128 * 1024

func TxSourcName(uri string) string {
	sourceAlias := SourceAliasesFromEnv()
	if alias, ok := sourceAlias[uri]; ok {
		return alias
	}

	if strings.Contains(uri, "alchemy.com/") {
		return "alchemy"
	}

	if strings.Contains(uri, "infura.io/") {
		return "infura"
	}

	return uri
}
