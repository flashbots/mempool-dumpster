package common

import "strings"

const (
	SourceTagLocal      = "local"
	SourceTagBloxroute  = "bloxroute"
	SourceTagChainbound = "chainbound"
	SourceTagEden       = "eden"
)

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
