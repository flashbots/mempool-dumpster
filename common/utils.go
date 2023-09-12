// Package common contains common functions and variables used by various scripts and services
package common

import (
	"archive/zip"
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"go.uber.org/zap"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	Printer                  = message.NewPrinter(language.English)
	ErrUnsupportedFileFormat = errors.New("unsupported file format")
)

func GetEnv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

func GetMemUsageMb() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / 1024 / 1024
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s := Printer.Sprintf("Alloc = %d MiB, tTotalAlloc = %d MiB, Sys = %d MiB, tNumGC = %d", m.Alloc/1024/1024, m.TotalAlloc/1024/1024, m.Sys/1024/1024, m.NumGC)
	log.Info(s)
}

func RLPDecode(rlpBytes []byte) (*types.Transaction, error) {
	var tx types.Transaction
	err := tx.UnmarshalBinary(rlpBytes)
	if err != nil {
		err = rlp.DecodeBytes(rlpBytes, &tx)
	}
	return &tx, err
}

func RLPStringToTx(rlpHex string) (*types.Transaction, error) {
	if !strings.HasPrefix(rlpHex, "0x") {
		rlpHex = "0x" + rlpHex
	}
	rawtx, err := hexutil.Decode(rlpHex)
	if err != nil {
		return nil, err
	}
	return RLPDecode(rawtx)
}

func TxToRLPString(tx *types.Transaction) (string, error) {
	b, err := tx.MarshalBinary()
	if err != nil {
		return "", err
	}
	return hexutil.Encode(b), nil
}

func IntDiffPercentFmt(a, b int) string {
	diff := float64(a) / float64(b)
	return Printer.Sprintf("%.2f%%", diff*100)
}

func Int64DiffPercentFmt(a, b int64) string {
	diff := float64(a) / float64(b)
	return Printer.Sprintf("%.2f%%", diff*100)
}

func SourceAliasesFromEnv() map[string]string {
	aliases := make(map[string]string)
	aliasesRaw := os.Getenv("SRC_ALIASES") // format: alias=url,alias=url
	if aliasesRaw != "" {
		entries := strings.Split(aliasesRaw, ",")
		for _, entry := range entries {
			parts := strings.Split(entry, "=")
			if len(parts) != 2 {
				continue
			}
			aliases[parts[1]] = parts[0]
		}
	}
	return aliases
}

func MustNotExist(log *zap.SugaredLogger, fn string) {
	if _, err := os.Stat(fn); !os.IsNotExist(err) {
		log.Fatalf("Output file already exists: %s", fn)
	}
}

func MustBeFile(log *zap.SugaredLogger, fn string) {
	s, err := os.Stat(fn)
	if errors.Is(err, os.ErrNotExist) {
		log.Fatalf("Input file does not exist: %s", fn)
	} else if err != nil {
		log.Fatalf("os.Stat: %s", err)
	}
	if s.IsDir() {
		log.Fatalf("Input file is a directory: %s", fn)
	} else if filepath.Ext(fn) != ".csv" && !strings.HasSuffix(fn, ".csv.zip") {
		log.Fatalf("Input file is not a .csv or .csv.zip file: %s", fn)
	}
}

// GetCSV returns a CSV content from a file (.csv or .csv.zip)
func GetCSV(filename string) (rows [][]string, err error) {
	rows = make([][]string, 0)

	if strings.HasSuffix(filename, ".csv") {
		r, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		csvReader := csv.NewReader(r)
		return csvReader.ReadAll()
	} else if strings.HasSuffix(filename, ".zip") { // a zip file can contain many files
		zipReader, err := zip.OpenReader(filename)
		if err != nil {
			return nil, err
		}
		defer zipReader.Close()

		for _, f := range zipReader.File {
			if !strings.HasSuffix(f.Name, ".csv") {
				continue
			}

			r, err := f.Open()
			if err != nil {
				return nil, err
			}
			defer r.Close()
			csvReader := csv.NewReader(r)
			_rows, err := csvReader.ReadAll()
			if err != nil {
				return nil, err
			}
			rows = append(rows, _rows...)
		}
		return rows, nil
	}
	return nil, ErrUnsupportedFileFormat
}

// HumanBytes returns size in the same format as AWS S3
func HumanBytes(n uint64) string {
	s := humanize.IBytes(n)
	s = strings.Replace(s, "MiB", "MB", 1)
	s = strings.Replace(s, "GiB", "GB", 1)
	s = strings.Replace(s, "KiB", "KB", 1)
	return s
}

func IsWebsocketProtocol(url string) bool {
	return strings.HasPrefix(url, "ws://") || strings.HasPrefix(url, "wss://")
}
