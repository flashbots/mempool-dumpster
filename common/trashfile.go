package common

// LoadTrashFiles loads sourcelog .csv (or .csv.zip) files (format: <timestamp_ms>,<tx_hash>,<source>) and returns a map[hash][source] = timestampMs
// func LoadTrashFiles(log *zap.SugaredLogger, files []string) (txs map[string]map[string]int64, cntProcessedRecords int64) { //nolint:gocognit
// 	txs = make(map[string]map[string]int64)

// 	rows, err := GetCSVFromFiles(files)
// 	if err != nil {
// 		log.Errorw("GetCSV", "error", err)
// 		return txs, cntProcessedRecords
// 	}

// 	for _, items := range rows {
// 		if len(items) != 3 {
// 			log.Errorw("invalid line", "line", items)
// 			continue
// 		}

// 		if len(items[1]) < 66 {
// 			continue
// 		}

// 		ts, err := strconv.Atoi(items[0])
// 		if err != nil {
// 			log.Errorw("strconv.Atoi", "error", err, "line", items)
// 			continue
// 		}
// 		txTimestamp := int64(ts)
// 		txHash := strings.ToLower(items[1])
// 		txSource := TxSourcName(items[2])

// 		// that it's a valid hash
// 		if len(txHash) != 66 {
// 			log.Errorw("invalid hash length", "hash", txHash)
// 			continue
// 		}
// 		if _, err = hexutil.Decode(txHash); err != nil {
// 			log.Errorw("hexutil.Decode", "error", err, "line", items)
// 			continue
// 		}

// 		cntProcessedRecords += 1

// 		// Add entry to txs map
// 		if _, ok := txs[txHash]; !ok {
// 			txs[txHash] = make(map[string]int64)
// 			txs[txHash][txSource] = txTimestamp
// 		}

// 		// Update timestamp if it's earlier (i.e. alchemy often sending duplicate entries, this makes sure we record the earliest timestamp)
// 		if txs[txHash][txSource] == 0 || txTimestamp < txs[txHash][txSource] {
// 			txs[txHash][txSource] = txTimestamp
// 		}
// 	}
// 	log.Infow("Processed file",
// 		"txTotal", Printer.Sprintf("%d", len(txs)),
// 		"memUsedMiB", Printer.Sprintf("%d", GetMemUsageMb()),
// 	)

// 	return txs, cntProcessedRecords
// }
