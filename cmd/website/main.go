// Website dev server (-dev) and prod build/upload tool (-build and -upload)
package cmd_website //nolint:stylecheck

import (
	"bytes"
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
	"github.com/urfave/cli/v2"
)

var Command = cli.Command{
	Name:  "website",
	Usage: "manage website tasks",

	Subcommands: []*cli.Command{
		{
			Name:  "dev",
			Usage: "run dev server",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "listen-addr",
					Aliases: []string{"l"},
					Usage:   "address to listen on for the dev server",
					Value:   ":8095",
				},
			},
			Action: runDevServer,
		},
		{
			Name:  "build",
			Usage: "build prod output",
			Flags: []cli.Flag{
				&cli.BoolFlag{
					Name:    "upload",
					Aliases: []string{"u"},
					Usage:   "upload prod output to S3",
					Value:   false,
				},
				&cli.StringFlag{
					Name:    "out",
					Aliases: []string{"o"},
					Usage:   "where to save output files",
					Value:   "./build/website-html",
				},
			},
			Action: buildWebsite,
		},
	},
}

func runDevServer(cCtx *cli.Context) error {
	listenAddr := cCtx.String("listen-addr")
	dev := cCtx.Bool("dev")

	log := common.GetLogger(false, false)
	defer func() { _ = log.Sync() }()

	log.Infof("Starting webserver on %s", listenAddr)
	webserver, err := website.NewDevWebserver(&website.DevWebserverOpts{ //nolint:exhaustruct
		ListenAddress: listenAddr,
		Log:           log,
		Dev:           dev,
	})
	if err != nil {
		return err
	}
	err = webserver.StartServer()
	return err
}

func buildWebsite(cCtx *cli.Context) error {
	outDir := cCtx.String("out")
	upload := cCtx.Bool("upload")
	if outDir == "" {
		return fmt.Errorf("output directory is required") //nolint:err113
	}

	log := common.GetLogger(false, false)
	defer func() { _ = log.Sync() }()

	log.Infof("Starting HTML build, will output to %s", outDir)
	err := os.MkdirAll(outDir, os.ModePerm)
	if err != nil {
		return err
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
		return err
	}
	fmt.Println("Months:", months)

	// build root page
	log.Infof("Building root page ...")
	rootPageData := website.HTMLData{ //nolint:exhaustruct
		Title:            "",
		Path:             "/index.html",
		EthMainnetMonths: months,
	}

	tpl, err := website.ParseIndexTemplate()
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	err = tpl.ExecuteTemplate(buf, "base", rootPageData)
	if err != nil {
		return err
	}

	// minify
	mBytes, err := minifier.Bytes("text/html", buf.Bytes())
	if err != nil {
		return err
	}

	// write to file
	fn := filepath.Join(outDir, "index.html")
	log.Infof("Writing to %s ...", fn)
	err = os.WriteFile(fn, mBytes, 0o0600)
	if err != nil {
		return err
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
			return err
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
			return err
		}

		buf := new(bytes.Buffer)
		err = tpl.ExecuteTemplate(buf, "base", rootPageData)
		if err != nil {
			return err
		}

		// minify
		mBytes, err := minifier.Bytes("text/html", buf.Bytes())
		if err != nil {
			return err
		}

		// write to file
		_outDir := filepath.Join(outDir, dir)
		err = os.MkdirAll(_outDir, os.ModePerm)
		if err != nil {
			return err
		}

		fn := filepath.Join(_outDir, "index.html")
		log.Infof("Writing to %s ...", fn)
		err = os.WriteFile(fn, mBytes, 0o0600)
		if err != nil {
			return err
		}

		toUpload = append(toUpload, struct{ from, to string }{fn, "/" + dir})
	}

	if upload {
		log.Infow("Uploading to S3 ...")
		// for _, file := range toUpload {
		// 	fmt.Printf("- %s -> %s\n", file.from, file.to)
		// }

		for _, file := range toUpload {
			app := "./scripts/s3/upload-file-to-r2.sh"
			cmd := exec.Command(app, file.from, file.to) //nolint:gosec
			stdout, err := cmd.Output()
			if err != nil {
				return err
			}
			fmt.Println(string(stdout))
		}
	}

	return nil
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
	lines := strings.SplitSeq(string(stdout), "\n")
	for line := range lines {
		if line != "" && strings.HasPrefix(line, "20") {
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
	lines := strings.SplitSeq(string(stdout), "\n")
	for line := range lines {
		if line != "" {
			line = space.ReplaceAllString(line, " ")
			parts := strings.Split(line, " ")

			// parts[2] is the size
			size, err := strconv.ParseUint(parts[2], 10, 64)
			if err != nil {
				return files, err
			}

			filename := parts[3]

			if filename == "index.html" {
				continue
			} else if strings.HasSuffix(filename, ".csv.gz") {
				continue
			}

			files = append(files, website.FileEntry{
				Filename: filename,
				Size:     size,
				Modified: parts[1] + " " + parts[0],
			})
		}
	}
	return files, nil
}
