package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/flashbots/mempool-dumpster/collector"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/lithammer/shortuuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	version = "dev" // is set during build process

	// Default values
	defaultDebug        = os.Getenv("DEBUG") == "1"
	defaultLogProd      = os.Getenv("LOG_PROD") == "1"
	defaultLogService   = os.Getenv("LOG_SERVICE")
	defaultblxAuthToken = os.Getenv("BLX_AUTH_HEADER")

	// Flags
	printVersion  = flag.Bool("version", false, "only print version")
	debugPtr      = flag.Bool("debug", defaultDebug, "print debug output")
	logProdPtr    = flag.Bool("log-prod", defaultLogProd, "log in production mode (json)")
	logServicePtr = flag.String("log-service", defaultLogService, "'service' tag to logs")
	nodesPtr      = flag.String("nodes", "ws://localhost:8546", "comma separated list of EL nodes")
	outDirPtr     = flag.String("out", "", "path to collect raw transactions into")
	uidPtr        = flag.String("uid", "", "collector uid (part of output CSV filename)")
	blxAuthToken  = flag.String("blx-token", defaultblxAuthToken, "bloxroute auth token (optional)")
	sourcelog     = flag.Bool("sourcelog", false, "write a CSV with all received transactions from any source (timestamp_ms,hash,source)")
)

func main() {
	flag.Parse()

	// perhaps only print the version
	if *printVersion {
		fmt.Printf("mempool-collector %s\n", version)
		return
	}

	// Logger setup
	var logger *zap.Logger
	zapLevel := zap.NewAtomicLevel()
	if *debugPtr {
		zapLevel.SetLevel(zap.DebugLevel)
	}
	if *logProdPtr {
		encoderCfg := zap.NewProductionEncoderConfig()
		encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		logger = zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderCfg),
			zapcore.Lock(os.Stdout),
			zapLevel,
		))
	} else {
		logger = zap.New(zapcore.NewCore(
			zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
			zapcore.Lock(os.Stdout),
			zapLevel,
		))
	}

	defer func() { _ = logger.Sync() }()
	log := logger.Sugar()

	if *logServicePtr != "" {
		log = log.With("service", *logServicePtr)
	}

	if *outDirPtr == "" {
		log.Fatal("No output directory not set (use -out <path>)")
	}

	if *uidPtr == "" {
		*uidPtr = shortuuid.New()[:6]
	}

	if *nodesPtr == "" && *blxAuthToken == "" {
		log.Fatal("No nodes or bloxroute token set (use -nodes <url1>,<url2> and/or -blx-token <token>)")
	}

	nodes := []string{}
	if *nodesPtr != "" {
		nodes = strings.Split(*nodesPtr, ",")
	}

	log.Infow("Starting mempool-collector", "version", version, "outDir", *outDirPtr, "uid", *uidPtr)

	aliases := common.SourceAliasesFromEnv()
	if len(aliases) > 0 {
		log.Infow("Using source aliases:", "aliases", aliases)
	}

	// Start service components
	collector.Start(log, nodes, *outDirPtr, *uidPtr, *blxAuthToken, *sourcelog)

	// Wwait for termination signal
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	<-exit
	log.Info("bye")
}
