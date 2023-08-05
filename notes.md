Errors:

```bash
panic: runtime error: slice bounds out of range [:4] with capacity 0

goroutine 233424 [running]:
github.com/flashbots/mempool-archiver/collector.(*TxProcessor).processTx(0xc00007e640, 0xc0000a8000)
        /root/mempool-archiver/collector/tx_processor.go:94 +0x13d2
created by github.com/flashbots/mempool-archiver/collector.(*TxProcessor).Start
        /root/mempool-archiver/collector/tx_processor.go:47 +0x105
exit status 2
```

-> fixed with tx to channel as value instead of ptr