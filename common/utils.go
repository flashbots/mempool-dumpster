// Package common contains common functions and variables used by various scripts and services
package common

import (
	"runtime"

	"github.com/ethereum/go-ethereum/log"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var Printer = message.NewPrinter(language.English)

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
