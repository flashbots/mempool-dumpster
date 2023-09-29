// Package common contains common functions and variables used by various scripts and services
package common

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/olekukonko/tablewriter"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	ErrUnsupportedFileFormat = errors.New("unsupported file format")

	Printer = message.NewPrinter(language.English)
	Caser   = cases.Title(language.English)
	Title   = Caser.String
)

func GetEnv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

func GetEnvInt(key string, defaultValue int) int {
	if value, ok := os.LookupEnv(key); ok {
		val, err := strconv.Atoi(value)
		if err == nil {
			return val
		}
	}
	return defaultValue
}

func GetMemUsageMb() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / 1024 / 1024
}

func GetMemUsageHuman() string {
	mb := GetMemUsageMb()
	return HumanBytes(mb * 1024 * 1024)
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

func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Floor(val*ratio) / ratio
}

func IntDiffPercentFmt(a, b int, decimals uint) string {
	return IntDiffPercentFmtC(a, b, decimals, "%%")
}

func IntDiffPercentFmtC(a, b int, decimals uint, fmtSuffix string) string {
	format := fmt.Sprintf("%%.%df%s", decimals, fmtSuffix)
	f := float64(a) / float64(b)
	f2 := roundFloat(f*100, decimals)
	return Printer.Sprintf(format, f2)
}

func Int64DiffPercentFmt(a, b int64, decimals uint) string {
	return Int64DiffPercentFmtC(a, b, decimals, "%%")
}

func Int64DiffPercentFmtC(a, b int64, decimals uint, fmtSuffix string) string {
	format := fmt.Sprintf("%%.%df%s", decimals, fmtSuffix)
	f := float64(a) / float64(b)
	f2 := roundFloat(f*100, decimals)
	return Printer.Sprintf(format, f2)
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

// HumanBytes returns size in the same format as AWS S3
func HumanBytes(n uint64) string {
	s := humanize.IBytes(n)
	s = strings.Replace(s, "KiB", "KB", 1)
	s = strings.Replace(s, "MiB", "MB", 1)
	s = strings.Replace(s, "GiB", "GB", 1)
	return s
}

func IsWebsocketProtocol(url string) bool {
	return strings.HasPrefix(url, "ws://") || strings.HasPrefix(url, "wss://")
}

func PrettyInt(i int) string {
	return Printer.Sprintf("%d", i)
}

func PrettyInt64(i int64) string {
	return Printer.Sprintf("%d", i)
}

func FmtDateDay(t time.Time) string {
	return t.Format("2006-01-02")
}

func FmtDateDayTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

func FmtDuration(d time.Duration) string {
	x, _ := time.ParseDuration("1s")
	s := d.Round(x).String()
	s = strings.Replace(s, "h", "h ", 1)
	s = strings.Replace(s, "m", "m ", 1)
	s = strings.Replace(s, "s", "s ", 1)
	return strings.Trim(s, " ")
}

func SetupMarkdownTableWriter(table *tablewriter.Table) {
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")
}
