package metrics

import (
	"fmt"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

var (
	txDispatchAge   = metrics.NewHistogram("mempool_dumpster_tx_receive_to_dispatch_seconds")
	txReceived      = metrics.NewCounter("mempool_dumpster_tx_received_total")
	txReceivedFirst = metrics.NewCounter("mempool_dumpster_tx_received_first")
	txReceivedTrash = metrics.NewCounter("mempool_dumpster_tx_received_trash")

	clickhouseErrors           = metrics.NewCounter("mempool_dumpster_clickhouse_errors_total")
	clickhouseErrorsBatchSave  = metrics.NewCounter("mempool_dumpster_clickhouse_errors_batch_save_total")
	clickhouseBatchSaveRetries = metrics.NewCounter("mempool_dumpster_clickhouse_batch_save_retries_total")
	clickhouseBatchSaveGiveup  = metrics.NewCounter("mempool_dumpster_clickhouse_batch_save_giveup_total")
	clickhouseBatchSaveSuccess = metrics.NewCounter("mempool_dumpster_clickhouse_batch_save_success_total")
)

const (
	TxReceivedSourceLabel      = `mempool_dumpster_tx_received_total{source="%s"}`
	TxReceivedFirstSourceLabel = `mempool_dumpster_tx_received_first{source="%s"}`
	TxReceivedTrashLabel       = `mempool_dumpster_tx_received_trash{source="%s"}`

	ClickhouseBatchSaveTimeLabel = `mempool_dumpster_clickhouse_batch_save_duration_milliseconds{type="%s"}`
	ClickhouseEntriesSavedLabel  = `mempool_dumpster_clickhouse_entries_saved_total{type="%s"}`
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

func ObserveTxDispatchAge(received time.Time) {
	txDispatchAge.UpdateDuration(received)
}

func ObserveReceiptLookup(start time.Time, outcome string) {
	label := fmt.Sprintf(`mempool_dumpster_receipt_lookup_duration_seconds{outcome=%q}`, outcome)
	metrics.GetOrCreateHistogram(label).UpdateDuration(start)
}

func IncTxRPCReceiverOutcome(outcome string) {
	label := fmt.Sprintf(`mempool_dumpster_tx_rpc_receiver_requests_total{outcome=%q}`, outcome)
	metrics.GetOrCreateCounter(label).Inc()
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

func AddClickhouseBatchSaveDurationMilliseconds(cntType string, durationMs int64) {
	label := fmt.Sprintf(ClickhouseBatchSaveTimeLabel, cntType)
	metrics.GetOrCreateHistogram(label).Update(float64(durationMs))
}

func AddClickhouseEntriesSaved(cntType string, cnt int) {
	label := fmt.Sprintf(ClickhouseEntriesSavedLabel, cntType)
	metrics.GetOrCreateCounter(label).Add(cnt)
}
