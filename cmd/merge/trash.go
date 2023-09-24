package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/flashbots/mempool-dumpster/common"
	"github.com/urfave/cli/v2"
)

func mergeTrash(cCtx *cli.Context) error {
	outDir := cCtx.String("out")
	fnPrefix := cCtx.String("fn-prefix")
	inputFiles := cCtx.Args().Slice()
	if cCtx.NArg() == 0 {
		log.Fatal("no input files specified as arguments")
	}

	log.Infow("Merge trash", "outDir", outDir, "fnPrefix", fnPrefix, "version", version)

	err := os.MkdirAll(outDir, os.ModePerm)
	check(err, "os.MkdirAll")

	// Ensure output files are don't yet exist
	fnOutCSV := filepath.Join(outDir, "trash.csv")
	if fnPrefix != "" {
		fnOutCSV = filepath.Join(outDir, fmt.Sprintf("%s_trash.csv", fnPrefix))
	}
	common.MustNotExist(log, fnOutCSV)
	log.Infof("Output file: %s", fnOutCSV)

	// Check input files
	for _, fn := range inputFiles {
		common.MustBeCSVFile(log, fn)
	}

	// Load input files
	log.Infof("Loading %d trash input files ...", len(inputFiles))
	trashTxs, err := common.LoadTrashFiles(log, inputFiles)
	check(err, "LoadTrashFiles")
	log.Infow("Processed all trash input files",
		"trashTxTotal", printer.Sprintf("%d", len(trashTxs)),
		"memUsed", common.GetMemUsageHuman(),
	)

	// Write output files
	log.Infof("Writing trash CSV file %s ...", fnOutCSV)
	err = writeTrashCSV(fnOutCSV, trashTxs)
	check(err, "writeSourcelogCSV")
	log.Infof("Output file written: %s", fnOutCSV)
	return nil
}

func writeTrashCSV(fn string, trash map[string]map[string]*common.TrashEntry) error {
	f, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write header
	_, err = f.WriteString("timestamp_ms,hash,source,reason,notes\n")
	if err != nil {
		return err
	}

	// save tx+source by timestamp: [timestamp][hash] = source
	cache := make(map[int64]map[string][]*common.TrashEntry)
	for hash, sources := range trash {
		for _, entry := range sources {
			if _, ok := cache[entry.Timestamp]; !ok {
				cache[entry.Timestamp] = make(map[string][]*common.TrashEntry)
			}
			cache[entry.Timestamp][hash] = append(cache[entry.Timestamp][hash], entry)
		}
	}

	// sort by timestamp
	timestamps := make([]int, 0)
	for ts := range cache {
		timestamps = append(timestamps, int(ts))
	}
	sort.Ints(timestamps)

	// write to file
	for _, ts := range timestamps {
		for _, entries := range cache[int64(ts)] {
			for _, entry := range entries {
				_, err = f.WriteString(fmt.Sprintf("%s\n", entry.TrashEntryToCSVRow()))
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
