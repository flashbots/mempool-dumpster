package main

import (
	"os"

	"github.com/olekukonko/tablewriter"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	printer = message.NewPrinter(language.English)
	caser   = cases.Title(language.English)
	title   = caser.String
)

func writeSummary(fn, s string) {
	log.Infof("Writing summary CSV file %s ...", fn)
	f, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		log.Errorw("openFile", "error", err)
		return
	}
	defer f.Close()
	_, err = f.WriteString(s)
	if err != nil {
		log.Errorw("writeFile", "error", err)
		return
	}
}

func setupTableWriter(table *tablewriter.Table) {
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")
}
