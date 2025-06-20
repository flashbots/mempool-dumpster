package metrics

import (
	"fmt"

	"github.com/VictoriaMetrics/metrics"
)

var (
	txReceived      = metrics.NewCounter("tx_received_total")
	txReceivedFirst = metrics.NewCounter("tx_received_first")
	txReceivedTrash = metrics.NewCounter("tx_received_trash")

	clickhouseErrors           = metrics.NewCounter("clickhouse_errors_total")
	clickhouseErrorsBatchSave  = metrics.NewCounter("clickhouse_errors_batch_save_total")
	clickhouseBatchSaveRetries = metrics.NewCounter("clickhouse_batch_save_retries_total")
	clickhouseBatchSaveGiveup  = metrics.NewCounter("clickhouse_batch_save_giveup_total")
	clickhouseBatchSaveSuccess = metrics.NewCounter("clickhouse_batch_save_success_total")
)

const (
	TxReceivedSourceLabel      = `tx_received_total{source="%s"}`
	TxReceivedFirstSourceLabel = `tx_received_first{source="%s"}`
	TxReceivedTrashLabel       = `tx_received_trash{source="%s"}`
)

func IncTxReceived(source string) {
	txReceived.Inc()
	l := fmt.Sprintf(TxReceivedSourceLabel, source)
	metrics.GetOrCreateCounter(l).Inc()
}

func IncTxReceivedFirst(source string) {
	txReceivedFirst.Inc()
	l := fmt.Sprintf(TxReceivedFirstSourceLabel, source)
	metrics.GetOrCreateCounter(l).Inc()
}

func IncTxReceivedTrash(source string) {
	txReceivedTrash.Inc()
	l := fmt.Sprintf(TxReceivedTrashLabel, source)
	metrics.GetOrCreateCounter(l).Inc()
}

func IncClickhouseError() {
	clickhouseErrors.Inc()
}

func IncClickhouseErrorBatchSave() {
	clickhouseErrorsBatchSave.Inc()
}

func IncClickhouseBatchSaveRetries() {
	clickhouseBatchSaveRetries.Inc()
}

func IncClickhouseBatchSaveGiveup() {
	clickhouseBatchSaveGiveup.Inc()
}

func IncClickhouseBatchSaveSuccess() {
	clickhouseBatchSaveSuccess.Inc()
}
