package collector

type TxDetail struct {
	Timestamp int64  `json:"timestamp"`
	Hash      string `json:"hash"`
	RawTx     string `json:"rawTx"`
}
