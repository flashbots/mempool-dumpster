package main

import (
	"github.com/urfave/cli/v2"
)

func mergeTrash(cCtx *cli.Context) error {
	// outDir := cCtx.String("out")
	// fnPrefix := cCtx.String("fn-prefix")
	// inputFiles := cCtx.Args().Slice()
	// if cCtx.NArg() == 0 {
	// 	log.Fatal("no input files specified as arguments")
	// }

	// log.Infow("Merge trash", "outDir", outDir, "fnPrefix", fnPrefix, "version", version)

	// err := os.MkdirAll(outDir, os.ModePerm)
	// check(err, "os.MkdirAll")

	// // Ensure output files are don't yet exist
	// fnOutCSV := filepath.Join(outDir, "trash.csv")
	// if fnPrefix != "" {
	// 	fnOutCSV = filepath.Join(outDir, fmt.Sprintf("%s_trash.csv", fnPrefix))
	// }
	// common.MustNotExist(log, fnOutCSV)
	// log.Infof("Output file: %s", fnOutCSV)

	// // Check input files
	// for _, fn := range inputFiles {
	// 	common.MustBeFile(log, fn)
	// }

	// // Load input files
	// sourcelog, cntProcessedRecords := common.LoadSourceLogFiles(log, inputFiles)
	// log.Infow("Processed all input files",
	// 	"txTotal", printer.Sprintf("%d", len(sourcelog)),
	// 	"records", printer.Sprintf("%d", cntProcessedRecords),
	// 	"memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()),
	// )

	// // Write output files
	// log.Infof("Writing sourcelog CSV file %s ...", fnOutCSV)
	// err = writeSourcelogCSV(fnOutCSV, sourcelog)
	// check(err, "writeSourcelogCSV")
	// log.Infof("Output file written: %s", fnOutCSV)
	return nil
}

// func writeTrashCSV(fn string, sourcelog map[string]map[string]int64) error {
// 	f, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
// 	if err != nil {
// 		return err
// 	}
// 	defer f.Close()

// 	// Write header
// 	_, err = f.WriteString("timestamp_ms,hash,source\n")
// 	if err != nil {
// 		return err
// 	}

// 	// save tx+source by timestamp: [timestamp][hash] = source
// 	cache := make(map[int]map[string][]string)
// 	for hash, v := range sourcelog {
// 		for source, ts := range v {
// 			if _, ok := cache[int(ts)]; !ok {
// 				cache[int(ts)] = make(map[string][]string)
// 			}
// 			cache[int(ts)][hash] = append(cache[int(ts)][hash], source)
// 		}
// 	}

// 	// sort by timestamp
// 	timestamps := make([]int, 0)
// 	for ts := range cache {
// 		timestamps = append(timestamps, ts)
// 	}
// 	sort.Ints(timestamps)

// 	// write to file
// 	for _, ts := range timestamps {
// 		for hash, sources := range cache[ts] {
// 			for _, source := range sources {
// 				_, err = f.WriteString(fmt.Sprintf("%d,%s,%s\n", ts, hash, source))
// 				if err != nil {
// 					return err
// 				}
// 			}
// 		}
// 	}

// 	return nil
// }
