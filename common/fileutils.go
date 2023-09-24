package common

import (
	"archive/zip"
	"encoding/csv"
	"errors"
	"os"
	"strings"

	"go.uber.org/zap"
)

func MustNotExist(log *zap.SugaredLogger, fn string) {
	if _, err := os.Stat(fn); !os.IsNotExist(err) {
		log.Fatalf("Output file already exists: %s", fn)
	}
}

func MustBeFile(log *zap.SugaredLogger, fn string, extensions []string) {
	s, err := os.Stat(fn)
	if errors.Is(err, os.ErrNotExist) {
		log.Fatalf("Input file does not exist: %s", fn)
	} else if err != nil {
		log.Fatalf("os.Stat: %s", err)
	}
	if s.IsDir() {
		log.Fatalf("Input file is a directory: %s", fn)
	}

	validExtension := false
	for _, ext := range extensions {
		if strings.HasSuffix(fn, ext) {
			validExtension = true
			break
		}
	}
	if !validExtension {
		log.Fatalf("Input file %s has invalid extension (allowed: %s)", fn, strings.Join(extensions, ","))
	}
}

func MustBeCSVFile(log *zap.SugaredLogger, fn string) {
	MustBeFile(log, fn, []string{".csv", ".csv.zip"})
}

func MustBeParquetFile(log *zap.SugaredLogger, fn string) {
	MustBeFile(log, fn, []string{".parquet"})
}

func GetCSVFromFiles(filenames []string) (rows [][]string, err error) {
	rows = make([][]string, 0)
	for _, filename := range filenames {
		_rows, err := GetCSV(filename)
		if err != nil {
			return nil, err
		}
		rows = append(rows, _rows...)
	}
	return rows, nil
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
