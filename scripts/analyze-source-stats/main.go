package main

/**
Source-Stats Summarizer takes the source-stats CSV files from the collector and summarizes them into a single CSV file.

Input: CSV file(s) with the following format:

	<timestamp_ms>,<tx_hash>,<source>

Output (currently):

	2023-08-30T20:34:47.253Z        INFO    Processed all input files       {"files": 22, "txTotal": "627,891", "memUsedMiB": "594"}
	2023-08-30T20:34:47.648Z        INFO    Overall tx count        {"infura": "578,606", "alchemy": "568,790", "ws://localhost:8546": "593,046", "blx": "419,725"}
	2023-08-30T20:34:47.696Z        INFO    Unique tx count {"blx": "22,403", "ws://localhost:8546": "9,962", "alchemy": "2,940", "infura": "4,658", "unique": "39,963 / 627,891"}
	2023-08-30T20:34:47.816Z        INFO    Not seen by local node  {"blx": "23,895", "infura": "9,167", "alchemy": "7,039", "notSeenByRef": "34,845 / 627,891"}

	Total unique tx: 627,891

	Transactions received:
	- alchemy: 			   568,790
	- blx:				   419,725
	- infura: 			   578,606
	- ws://localhost:8546: 593,046

	Unique tx (sole sender):
	- alchemy: 				2,940
	- blx: 					22,403
	- infura: 				4,658
	- ws://localhost:8546: 	9,962

	Transactions not seen by local node: 34,845 / 627,891
	- alchemy: 	7,039
	- blx: 		23,895
	- infura: 	9,167

more insight ideas?
- who sent first
*/

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/zap"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	version = "dev" // is set during build process

	// Default values
	defaultDebug = os.Getenv("DEBUG") == "1"

	// Flags
	printVersion = flag.Bool("version", false, "only print version")
	debugPtr     = flag.Bool("debug", defaultDebug, "print debug output")

	outDirPtr  = flag.String("out", "", "where to save output files")
	outDatePtr = flag.String("out-date", "", "date to use in output file names")
	// limit = flag.Int("limit", 0, "max number of txs to process")

	// Errors
	// errLimitReached = errors.New("limit reached")

	// Helpers
	log     *zap.SugaredLogger
	printer = message.NewPrinter(language.English)
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Use: %s -out <output_directory> <input_file1> <input_file2> <input_dir>/*.csv ... \n\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	// perhaps only print the version
	if *printVersion {
		fmt.Printf("mempool source-stats-summarizer %s\n", version)
		return
	}

	// Logger setup
	log = common.GetLogger(*debugPtr, false)
	defer func() { _ = log.Sync() }()

	// Collect input files from arguments
	files := flag.Args()
	if flag.NArg() == 0 {
		fmt.Println("No input files specified as arguments.")
		os.Exit(1)
	}

	log.Infow("Starting mempool-source-stats-summarizer", "version", version)

	aliases := common.SourceAliasesFromEnv()
	if len(aliases) > 0 {
		log.Infow("Using source aliases:", "aliases", aliases)
	}

	// writing output file is optional
	if *outDirPtr != "" {
		log.Infof("Output directory: %s", *outDirPtr)

		// Prepare output file paths, and make sure they don't exist yet
		fnOut := getOutFilename()
		if _, err := os.Stat(fnOut); !errors.Is(err, os.ErrNotExist) {
			log.Fatalf("Output file already exists: %s", fnOut)
		}

		// Ensure the output directory exists
		err := os.MkdirAll(*outDirPtr, os.ModePerm)
		if err != nil {
			log.Errorw("os.MkdirAll", "error", err)
			return
		}
	}

	summarizeSourceStats(files)
}

func getOutFilename() string {
	fnOut := filepath.Join(*outDirPtr, "source-stats.csv")
	if *outDatePtr != "" {
		fnOut = filepath.Join(*outDirPtr, fmt.Sprintf("%s_source-stats.csv", *outDatePtr))
	}
	return fnOut
}

func checkInputFiles(files []string) {
	// Ensure all input files exist and are CSVs
	for _, filename := range files {
		s, err := os.Stat(filename)
		if errors.Is(err, os.ErrNotExist) {
			log.Fatalf("Input file does not exist: %s", filename)
		} else if err != nil {
			log.Fatalf("os.Stat: %s", err)
		}

		if s.IsDir() {
			log.Fatalf("Input file is a directory: %s", filename)
		} else if filepath.Ext(filename) != ".csv" {
			log.Fatalf("Input file is not a CSV file: %s", filename)
		}
	}
}

// summarizeSourceStats parses all input CSV files into one output CSV and one output Parquet file.
func summarizeSourceStats(files []string) { //nolint:gocognit
	checkInputFiles(files)

	txs := make(map[string]map[string]int64) // [hash][srcTag]timestampMs

	timestampFirst, timestampLast := int64(0), int64(0)
	cntProcessedFiles := 0
	cntProcessedRecords := int64(0)

	// Collect transactions from all input files to memory
	for _, filename := range files {
		log.Infof("Processing: %s", filename)
		cntProcessedFiles += 1
		cntTxInFileTotal := 0

		readFile, err := os.Open(filename)
		if err != nil {
			log.Errorw("os.Open", "error", err, "file", filename)
			return
		}
		defer readFile.Close()

		fileReader := bufio.NewReader(readFile)
		for {
			l, err := fileReader.ReadString('\n')
			if len(l) == 0 && err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Errorw("fileReader.ReadString", "error", err)
				break
			}

			l = strings.Trim(l, "\n")
			items := strings.Split(l, ",") // timestamp,hash,source
			if len(items) != 3 {
				log.Errorw("invalid line", "line", l)
				continue
			}

			cntTxInFileTotal += 1

			ts, err := strconv.Atoi(items[0])
			if err != nil {
				log.Errorw("strconv.Atoi", "error", err, "line", l)
				continue
			}
			txTimestamp := int64(ts)
			txHash := items[1]
			txSrcTag := common.TxSourcName(items[2])

			// that it's a valid hash
			if len(txHash) != 66 {
				log.Errorw("invalid hash length", "hash", txHash)
				continue
			}
			if _, err = hexutil.Decode(txHash); err != nil {
				log.Errorw("hexutil.Decode", "error", err, "line", l)
				continue
			}

			cntProcessedRecords += 1

			if timestampFirst == 0 || txTimestamp < timestampFirst {
				timestampFirst = txTimestamp
			}
			if txTimestamp > timestampLast {
				timestampLast = txTimestamp
			}

			// Check for duplicate sends from a single source
			// if _, ok := txs[txHash]; ok {
			// 	// hash already exists
			// 	if _, ok := txs[txHash][txSrcTag]; ok {
			// 		// source already exists
			// 		log.Debugw("duplicate entry", "hash", txHash, "source", txSrcTag, "timestamp", txTimestamp)
			// 	}
			// }

			// Add entry to txs map
			if _, ok := txs[txHash]; !ok {
				txs[txHash] = make(map[string]int64)
				txs[txHash][txSrcTag] = txTimestamp
			}

			// Update timestamp if it's earlier (i.e. alchemy often sending duplicate entries, this makes sure we record the earliest timestamp)
			if txs[txHash][txSrcTag] == 0 || txTimestamp < txs[txHash][txSrcTag] {
				txs[txHash][txSrcTag] = txTimestamp
			}
		}
		log.Infow("Processed file",
			"txInFile", printer.Sprintf("%d", cntTxInFileTotal),
			// "txNew", printer.Sprintf("%d", cntTxInFileNew),
			"txTotal", printer.Sprintf("%d", len(txs)),
			"memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()),
		)
	}

	log.Infow("Processed all input files",
		"files", cntProcessedFiles,
		"records", printer.Sprintf("%d", cntProcessedRecords),
		"txTotal", printer.Sprintf("%d", len(txs)),
		"memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()),
	)

	// Write output file
	if *outDirPtr != "" {
		err := writeTxCSV(txs)
		if err != nil {
			log.Errorw("writeTxCSV", "error", err)
		}
	}

	// Analyze
	log.Info("Analyzing...")
	analyzer := NewAnalyzer(txs)
	analyzer.Print()

	// analyzeTxs(timestampFirst, timestampLast, cntProcessedRecords, txs)
}

func writeTxCSV(txs map[string]map[string]int64) error {
	fn := getOutFilename()

	log.Infof("Output CSV file: %s", fn)
	f, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write header
	_, err = f.WriteString("timestamp_ms,hash,source\n")
	if err != nil {
		return err
	}

	// save tx+source by timestamp: [timestamp][hash] = source
	cache := make(map[int]map[string][]string)
	for hash, v := range txs {
		for srcTag, ts := range v {
			if _, ok := cache[int(ts)]; !ok {
				cache[int(ts)] = make(map[string][]string)
			}
			cache[int(ts)][hash] = append(cache[int(ts)][hash], srcTag)
		}
	}

	// sort by timestamp
	timestamps := make([]int, 0)
	for ts := range cache {
		timestamps = append(timestamps, ts)
	}
	sort.Ints(timestamps)

	// write to file
	for _, ts := range timestamps {
		for hash, srcTags := range cache[ts] {
			for _, srcTag := range srcTags {
				_, err = f.WriteString(fmt.Sprintf("%d,%s,%s\n", ts, hash, srcTag))
				if err != nil {
					return err
				}
			}
		}
	}

	log.Infof("Output file written: %s", fn)
	return nil
}
