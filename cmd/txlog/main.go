package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/flashbots/mempool-dumpster/common"
	"github.com/flashbots/mempool-dumpster/txlog"
	"go.uber.org/zap"
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
	log *zap.SugaredLogger
	// printer = message.NewPrinter(language.English)
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Use: %s -out <output_directory> <input_file1> <input_file2> <input_dir>/*.csv ... \n\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	// perhaps only print the version
	if *printVersion {
		fmt.Printf("mempool-dumpster txlog %s\n", version)
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

	log.Infow("mempool-dumpster txlog", "version", version)

	// print possible aliases, for debugging
	aliases := common.SourceAliasesFromEnv()
	if len(aliases) > 0 {
		log.Infow("Using source aliases:", "aliases", aliases)
	}

	// if writing output file, ensure output directory exists but output file doesn't
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

	// Check the input files
	checkInputFiles(files)

	// Load transaction log files
	txLog := txlog.LoadTxLog(log, files)

	// Write output file
	if *outDirPtr != "" {
		err := writeTxCSV(txLog)
		if err != nil {
			log.Errorw("writeTxCSV", "error", err)
		}
	}

	// Analyze
	log.Info("Analyzing...")
	analyzer := txlog.NewAnalyzer(txLog)
	analyzer.Print()
}

func getOutFilename() string {
	fnOut := filepath.Join(*outDirPtr, "txlog.csv")
	if *outDatePtr != "" {
		fnOut = filepath.Join(*outDirPtr, fmt.Sprintf("%s_txlog.csv", *outDatePtr))
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
			log.Warnf("Input file might not be a CSV file (incorrect file ending): %s", filename)
		}
	}
}

func writeTxCSV(txs map[string]map[string]int64) error {
	fn := getOutFilename()

	log.Infof("Writing output CSV file %s ...", fn)
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
		for source, ts := range v {
			if _, ok := cache[int(ts)]; !ok {
				cache[int(ts)] = make(map[string][]string)
			}
			cache[int(ts)][hash] = append(cache[int(ts)][hash], source)
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
		for hash, sources := range cache[ts] {
			for _, source := range sources {
				_, err = f.WriteString(fmt.Sprintf("%d,%s,%s\n", ts, hash, source))
				if err != nil {
					return err
				}
			}
		}
	}

	log.Infof("Output file written: %s", fn)
	return nil
}
