package main

//
// Source-Stats Summarizer takes the source-stats CSV files from the collector and summarizes them into a single CSV file.
//
// Output:
//
// date, hour, source, tx_count, tx_count_first, tx_count_unique, tx_count_unseen
//

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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

	// Ensure output directory is set
	if *outDirPtr == "" {
		flag.Usage()
		log.Fatal("-out argument is required")
	}

	// Collect input files from arguments
	files := flag.Args()
	if flag.NArg() == 0 {
		fmt.Println("No input files specified as arguments.")
		os.Exit(1)
	}

	log.Infow("Starting mempool-source-stats-summarizer", "version", version)
	log.Infof("Output directory: %s", *outDirPtr)

	// Ensure the output directory exists
	err := os.MkdirAll(*outDirPtr, os.ModePerm)
	if err != nil {
		log.Errorw("os.MkdirAll", "error", err)
		return
	}

	summarizeSourceStats(files)
}

// summarizeSourceStats parses all input CSV files into one output CSV and one output Parquet file.
func summarizeSourceStats(files []string) { //nolint:gocognit
	// Prepare output file paths, and make sure they don't exist yet
	fnStats := filepath.Join(*outDirPtr, "source-stats.csv")
	if *outDatePtr != "" {
		fnStats = filepath.Join(*outDirPtr, fmt.Sprintf("%s_source-stats.csv", *outDatePtr))
	}
	if _, err := os.Stat(fnStats); !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("Output file already exists: %s", fnStats)
	}

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

	// Process input files
	// type txMeta struct {
	// 	timestampMs int64
	// 	hash        string
	// 	srcTag      string
	// }

	txs := make(map[string]map[string]int64) // [hash][srcTag]timestampMs
	sources := make(map[string]bool)

	// Collect transactions from all input files to memory
	cntProcessedFiles := 0
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

			// todo: check items[1] is a valid hash

			cntTxInFileTotal += 1

			ts, err := strconv.Atoi(items[0])
			if err != nil {
				log.Errorw("strconv.Atoi", "error", err, "line", l)
				continue
			}
			txTimestamp := int64(ts)
			txHash := items[1]
			txSrcTag := items[2]

			// Add source to map
			sources[txSrcTag] = true

			// Add entry to txs map
			if _, ok := txs[txHash]; !ok {
				txs[txHash] = make(map[string]int64)
			}
			// hash already exists
			txs[txHash][txSrcTag] = txTimestamp
		}
		log.Infow("Processed file",
			"txInFile", printer.Sprintf("%d", cntTxInFileTotal),
			// "txNew", printer.Sprintf("%d", cntTxInFileNew),
			"txTotal", printer.Sprintf("%d", len(txs)),
			"memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()),
		)
		// break
	}

	log.Infow("Processed all input files", "files", cntProcessedFiles, "txTotal", printer.Sprintf("%d", len(txs)), "memUsedMiB", printer.Sprintf("%d", common.GetMemUsageMb()))

	// step 1: get overall tx / source
	srcCntOverallTxs := make(map[string]int64)
	for _, v := range txs {
		for srcTag := range v {
			srcCntOverallTxs[srcTag] += 1
		}
	}

	l := log
	for srcTag, cnt := range srcCntOverallTxs {
		l = l.With(srcTag, printer.Sprintf("%d", cnt))
	}
	l.Info("Overall tx count")

	// step 2: get unique tx / source
	srcCntUniqueTxs := make(map[string]int64)
	nUnique := 0
	for hash, v := range txs {
		if len(v) == 1 {
			for srcTag := range v {
				srcCntUniqueTxs[srcTag] += 1
				nUnique += 1
				_ = hash
				// fmt.Println("unique", srcTag, hash)
			}
		}
	}

	l = log
	for srcTag, cnt := range srcCntUniqueTxs {
		l = l.With(srcTag, printer.Sprintf("%d", cnt))
	}
	l.Infow("Unique tx count", "unique", printer.Sprintf("%d / %d", nUnique, len(txs)))

	// step 2: get +/- vs reference
	ref := "ws://localhost:8546"
	srcNotSeenByRef := make(map[string]int64)
	nNotSeenByRef := 0
	for hash, v := range txs {
		if _, seenByRef := v[ref]; !seenByRef {
			nNotSeenByRef += 1
			for srcTag := range v {
				srcNotSeenByRef[srcTag] += 1
				_ = hash
				// fmt.Println("unique", srcTag, hash)
			}
		}
	}

	l = log
	for srcTag, cnt := range srcNotSeenByRef {
		l = l.With(srcTag, printer.Sprintf("%d", cnt))
	}
	l.Infow("Not seen by local node", "notSeenByRef", printer.Sprintf("%d / %d", nNotSeenByRef, len(txs)))

	// log.Infof("Finished processing %s CSV files, wrote %s transactions", printer.Sprintf("%d", cntProcessedFiles), printer.Sprintf("%d", cntTxWritten))
}
