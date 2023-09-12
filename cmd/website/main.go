// Website dev server (-dev) and prod build/upload tool (-build and -upload)
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/flashbots/mempool-dumpster/common"
	"github.com/flashbots/mempool-dumpster/website"
	"github.com/tdewolff/minify"
	"github.com/tdewolff/minify/css"
	"github.com/tdewolff/minify/html"
	"go.uber.org/zap"
)

var (
	listenAddr = ":8095"

	dev    = flag.Bool("dev", false, "run dev server")
	build  = flag.Bool("build", false, "build prod output")
	upload = flag.Bool("upload", false, "upload prod output")
	outDir = flag.String("out", "./build/website", "where to save output files")

	// Helpers
	log *zap.SugaredLogger
)

func main() {
	flag.Parse()

	// Logger setup
	log = common.GetLogger(false, false)
	defer func() { _ = log.Sync() }()

	if *dev {
		runDevServer()
	} else if *build {
		buildWebsite()
	} else {
		fmt.Println("No action specified -- use either -dev or -build")
		flag.Usage()
		os.Exit(1)
	}
}

func runDevServer() {
	log.Infof("Starting webserver on %s", listenAddr)
	webserver, err := website.NewWebserver(&website.WebserverOpts{
		ListenAddress: listenAddr,
		Log:           log,
		Dev:           *dev,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = webserver.StartServer()
	if err != nil {
		log.Fatal(err)
	}
}

func buildWebsite() {
	log.Infof("Creating build server in %s", *outDir)
	err := os.MkdirAll(*outDir, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	dir := "ethereum/mainnet/"

	// Setup minifier
	minifier := minify.New()
	minifier.AddFunc("text/html", html.Minify)
	minifier.AddFunc("text/css", css.Minify)

	// Load month folders from S3
	log.Infof("Getting folders from S3 for %s ...", dir)
	months, err := getFoldersFromS3(dir)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(months)

	// build root page
	log.Infof("Building root page ...")
	rootPageData := website.HTMLData{ //nolint:exhaustruct
		Title:            "",
		Path:             "/index.html",
		EthMainnetMonths: months,
	}

	tpl, err := website.ParseIndexTemplate()
	if err != nil {
		log.Fatal(err)
	}

	buf := new(bytes.Buffer)
	err = tpl.ExecuteTemplate(buf, "base", rootPageData)
	if err != nil {
		log.Fatal(err)
	}

	// minify
	mBytes, err := minifier.Bytes("text/html", buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}

	// write to file
	fn := filepath.Join(*outDir, "index.html")
	log.Infof("Writing to %s ...", fn)
	err = os.WriteFile(fn, mBytes, 0o0600)
	if err != nil {
		log.Fatal(err)
	}

	toUpload := []struct{ from, to string }{
		{fn, "/"},
	}

	// build files pages
	for _, month := range months {
		dir := "ethereum/mainnet/" + month + "/"
		log.Infof("Getting files from S3 for %s ...", dir)
		files, err := getFilesFromS3(dir)
		if err != nil {
			log.Fatal(err)
		}

		rootPageData := website.HTMLData{ //nolint:exhaustruct
			Title: month,
			Path:  fmt.Sprintf("ethereum/mainnet/%s/index.html", month),

			CurrentNetwork: "Ethereum Mainnet",
			CurrentMonth:   month,
			Files:          files,
		}

		tpl, err := website.ParseFilesTemplate()
		if err != nil {
			log.Fatal(err)
		}

		buf := new(bytes.Buffer)
		err = tpl.ExecuteTemplate(buf, "base", rootPageData)
		if err != nil {
			log.Fatal(err)
		}

		// minify
		mBytes, err := minifier.Bytes("text/html", buf.Bytes())
		if err != nil {
			log.Fatal(err)
		}

		// write to file
		_outDir := filepath.Join(*outDir, dir)
		err = os.MkdirAll(_outDir, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}

		fn := filepath.Join(_outDir, "index.html")
		log.Infof("Writing to %s ...", fn)
		err = os.WriteFile(fn, mBytes, 0o0600)
		if err != nil {
			log.Fatal(err)
		}

		toUpload = append(toUpload, struct{ from, to string }{fn, "/" + dir})
	}

	if *upload {
		log.Infow("Uploading to S3 ...")
		// for _, file := range toUpload {
		// 	fmt.Printf("- %s -> %s\n", file.from, file.to)
		// }

		for _, file := range toUpload {
			app := "./scripts/s3/upload-file-to-r2.sh"
			cmd := exec.Command(app, file.from, strings.TrimPrefix(file.to, "/")) //nolint:gosec
			stdout, err := cmd.Output()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(string(stdout))
		}
	}
}

func getFoldersFromS3(dir string) ([]string, error) {
	folders := []string{}

	app := "./scripts/s3/get-folders.sh"
	cmd := exec.Command(app, dir)
	stdout, err := cmd.Output()
	if err != nil {
		return folders, err
	}

	// Print the output
	lines := strings.Split(string(stdout), "\n")
	for _, line := range lines {
		if line != "" {
			folders = append(folders, strings.TrimSuffix(line, "/"))
		}
	}
	return folders, nil
}

func getFilesFromS3(month string) ([]website.FileEntry, error) {
	files := []website.FileEntry{}

	app := "./scripts/s3/get-files.sh"
	cmd := exec.Command(app, month)
	stdout, err := cmd.Output()
	if err != nil {
		return files, err
	}

	space := regexp.MustCompile(`\s+`)
	lines := strings.Split(string(stdout), "\n")
	for _, line := range lines {
		if line != "" {
			line = space.ReplaceAllString(line, " ")
			parts := strings.Split(line, " ")

			// parts[2] is the size
			size, err := strconv.ParseUint(parts[2], 10, 64)
			if err != nil {
				return files, err
			}

			if parts[3] == "index.html" {
				continue
			}
			files = append(files, website.FileEntry{
				Filename: parts[3],
				Size:     size,
				Modified: parts[1] + " " + parts[0],
			})
		}
	}
	return files, nil
}
