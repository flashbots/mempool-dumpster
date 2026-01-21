// Website dev server (-dev) and prod build tool
package cmd_website //nolint:stylecheck

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/flashbots/mempool-dumpster/website"
	"github.com/tdewolff/minify"
	"github.com/tdewolff/minify/css"
	"github.com/tdewolff/minify/html"
	"github.com/urfave/cli/v2"
)

const (
	defaultS3Bucket = "flashbots-mempool-dumpster"
	defaultS3Prefix = "ethereum/mainnet/"
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
			Name:  "build-index",
			Usage: "build root index page",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     "out",
					Aliases:  []string{"o"},
					Usage:    "output file path",
					Required: true,
				},
				&cli.StringFlag{
					Name:    "aws-profile",
					Aliases: []string{"p"},
					Usage:   "AWS profile to use for S3 access",
					Value:   "",
				},
				&cli.StringFlag{
					Name:  "s3-bucket",
					Usage: "S3 bucket name",
					Value: defaultS3Bucket,
				},
				&cli.StringFlag{
					Name:  "s3-prefix",
					Usage: "S3 prefix path",
					Value: defaultS3Prefix,
				},
			},
			Action: buildIndex,
		},
		{
			Name:      "build-month",
			Usage:     "build page for a specific month",
			ArgsUsage: "<month>",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     "out",
					Aliases:  []string{"o"},
					Usage:    "output file path",
					Required: true,
				},
				&cli.StringFlag{
					Name:    "aws-profile",
					Aliases: []string{"p"},
					Usage:   "AWS profile to use for S3 access",
					Value:   "",
				},
				&cli.StringFlag{
					Name:  "s3-bucket",
					Usage: "S3 bucket name",
					Value: defaultS3Bucket,
				},
				&cli.StringFlag{
					Name:  "s3-prefix",
					Usage: "S3 prefix path",
					Value: defaultS3Prefix,
				},
			},
			Action: buildMonth,
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

func newS3Client(ctx context.Context, profile string) (*s3.Client, error) {
	var cfg aws.Config
	var err error

	if profile != "" {
		cfg, err = config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile(profile))
	} else {
		cfg, err = config.LoadDefaultConfig(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return s3.NewFromConfig(cfg), nil
}

func listMonths(ctx context.Context, client *s3.Client, bucket, prefix string) ([]string, error) {
	var months []string

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, p := range page.CommonPrefixes {
			if p.Prefix == nil {
				continue
			}
			// Extract month from prefix like "ethereum/mainnet/2023-08/"
			month := strings.TrimPrefix(*p.Prefix, prefix)
			month = strings.TrimSuffix(month, "/")
			months = append(months, month)
		}
	}

	return months, nil
}

func listFilesInMonth(ctx context.Context, client *s3.Client, bucket, s3Prefix, month string) ([]website.FileEntry, error) {
	var files []website.FileEntry
	prefix := s3Prefix + month + "/"

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil || obj.Size == nil || obj.LastModified == nil {
				continue
			}

			filename := strings.TrimPrefix(*obj.Key, prefix)

			// Skip index.html and .csv.gz files
			if filename == "index.html" || strings.HasSuffix(filename, ".csv.gz") {
				continue
			}

			// Skip if it's just the directory itself
			if filename == "" {
				continue
			}

			files = append(files, website.FileEntry{
				Filename: filename,
				Size:     uint64(*obj.Size),
				Modified: obj.LastModified.Format("15:04:05 2006-01-02"),
			})
		}
	}

	return files, nil
}

func buildIndex(cCtx *cli.Context) error {
	outPath := cCtx.String("out")
	awsProfile := cCtx.String("aws-profile")
	s3Bucket := cCtx.String("s3-bucket")
	s3Prefix := cCtx.String("s3-prefix")

	log := common.GetLogger(false, false)
	defer func() { _ = log.Sync() }()

	ctx := context.Background()

	// Create S3 client
	log.Infof("Creating S3 client (profile: %s)...", awsProfile)
	client, err := newS3Client(ctx, awsProfile)
	if err != nil {
		return err
	}

	// Setup minifier
	minifier := minify.New()
	minifier.AddFunc("text/html", html.Minify)
	minifier.AddFunc("text/css", css.Minify)

	// Load month folders from S3
	log.Infof("Getting months from S3 (bucket: %s, prefix: %s)...", s3Bucket, s3Prefix)
	months, err := listMonths(ctx, client, s3Bucket, s3Prefix)
	if err != nil {
		return err
	}
	log.Infof("Found months: %v", months)

	// Build root page
	log.Infof("Building root page...")
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

	// Minify
	mBytes, err := minifier.Bytes("text/html", buf.Bytes())
	if err != nil {
		return err
	}

	// Ensure output directory exists
	outDir := filepath.Dir(outPath)
	if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
		return err
	}

	// Write to file
	log.Infof("Writing to %s...", outPath)
	err = os.WriteFile(outPath, mBytes, 0o0600)
	if err != nil {
		return err
	}

	log.Infof("Done!")
	return nil
}

func buildMonth(cCtx *cli.Context) error {
	outPath := cCtx.String("out")
	awsProfile := cCtx.String("aws-profile")
	s3Bucket := cCtx.String("s3-bucket")
	s3Prefix := cCtx.String("s3-prefix")
	month := cCtx.Args().First()

	if month == "" {
		return fmt.Errorf("month argument is required (e.g., 2023-08)") //nolint:err113
	}

	log := common.GetLogger(false, false)
	defer func() { _ = log.Sync() }()

	ctx := context.Background()

	// Create S3 client
	log.Infof("Creating S3 client (profile: %s)...", awsProfile)
	client, err := newS3Client(ctx, awsProfile)
	if err != nil {
		return err
	}

	// Setup minifier
	minifier := minify.New()
	minifier.AddFunc("text/html", html.Minify)
	minifier.AddFunc("text/css", css.Minify)

	// Get files for the month
	log.Infof("Getting files from S3 for %s (bucket: %s, prefix: %s)...", month, s3Bucket, s3Prefix)
	files, err := listFilesInMonth(ctx, client, s3Bucket, s3Prefix, month)
	if err != nil {
		return err
	}
	log.Infof("Found %d files", len(files))

	// Build month page
	pageData := website.HTMLData{ //nolint:exhaustruct
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
	err = tpl.ExecuteTemplate(buf, "base", pageData)
	if err != nil {
		return err
	}

	// Minify
	mBytes, err := minifier.Bytes("text/html", buf.Bytes())
	if err != nil {
		return err
	}

	// Ensure output directory exists
	outDir := filepath.Dir(outPath)
	if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
		return err
	}

	// Write to file
	log.Infof("Writing to %s...", outPath)
	err = os.WriteFile(outPath, mBytes, 0o0600)
	if err != nil {
		return err
	}

	log.Infof("Done!")
	return nil
}
