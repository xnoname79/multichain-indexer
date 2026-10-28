package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/fystack/multichain-indexer/internal/worker"
	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/logger"
	"github.com/fystack/multichain-indexer/pkg/events"
	"github.com/fystack/multichain-indexer/pkg/infra"
	"github.com/fystack/multichain-indexer/pkg/kvstore"
)

func main() {
	var (
		configPath string
		chainsList string
		debug      bool
		catchup    bool
	)

	flag.StringVar(&configPath, "config", "configs/config.yaml", "Path to configuration file")
	flag.StringVar(&chainsList, "chains", "", "Comma-separated list of chains to test (required)")
	flag.BoolVar(&debug, "debug", false, "Enable debug logging")
	flag.BoolVar(&catchup, "catchup", false, "Enable catchup worker alongside regular worker")
	flag.Parse()

	if chainsList == "" {
		fmt.Fprintln(os.Stderr, "missing required --chains flag (e.g. --chains=ethereum_mainnet)")
		os.Exit(1)
	}
	chains := strings.Split(chainsList, ",")
	for i := range chains {
		chains[i] = strings.TrimSpace(chains[i])
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to load config:", err)
		os.Exit(1)
	}

	if err := logger.InitFromConfig(cfg.Logging.Mode, cfg.Logging.Format, debug); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to initialize logger:", err)
		os.Exit(1)
	}

	if err := cfg.Chains.Validate(chains); err != nil {
		logger.Fatal("Invalid chains", "err", err)
	}

	// Throwaway SQLite file: the devtool only measures block processing.
	tmpDir, err := os.MkdirTemp("", "blockreliability-")
	if err != nil {
		logger.Fatal("Create temp dir failed", "err", err)
	}
	defer os.RemoveAll(tmpDir)
	db, err := infra.OpenSQLite(tmpDir + "/indexer.db")
	if err != nil {
		logger.Fatal("Open SQLite failed", "err", err)
	}
	defer db.Close()

	kv := kvstore.NewSQLiteStore(db, infra.JSON)
	emitter := events.NewEmitter(db, config.WebhookConfig{}) // never post devtool events to the real webhook
	defer emitter.Close()

	// Create tracker
	tracker := NewTracker()

	ctx, cancel := context.WithCancel(context.Background())

	managerCfg := worker.ManagerConfig{
		Chains:        chains,
		EnableRegular: true,
		EnableCatchup: catchup,
		Observer:      tracker.Observe,
	}

	manager := worker.CreateManagerWithWorkers(
		ctx,
		cfg,
		kv,
		db,
		emitter,
		managerCfg,
	)

	logger.Info("Starting workers for block reliability test", "chains", chains)
	manager.Start()

	fmt.Println("\nPress Ctrl+C or Enter to stop and generate report...")

	// Wait for signal or Enter
	done := make(chan struct{}, 1)
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
		<-stop
		select {
		case done <- struct{}{}:
		default:
		}
	}()
	go func() {
		reader := bufio.NewReader(os.Stdin)
		_, _ = reader.ReadString('\n')
		select {
		case done <- struct{}{}:
		default:
		}
	}()
	<-done

	fmt.Println("\nStopping workers...")
	cancel()
	manager.Stop()

	// Generate reports
	fmt.Println("\n========== BLOCK RELIABILITY REPORT ==========")

	for _, chainName := range chains {
		// Indexers uppercase chain names via GetName()
		upperName := strings.ToUpper(chainName)

		tracker.mu.RLock()
		ct, ok := tracker.chains[upperName]
		tracker.mu.RUnlock()

		if !ok {
			fmt.Printf("\n=== %s ===\n", upperName)
			fmt.Println("  No blocks observed")
			continue
		}

		ct.mu.Lock()
		var minBlock, maxBlock uint64
		first := true
		for b := range ct.blocks {
			if first {
				minBlock = b
				maxBlock = b
				first = false
				continue
			}
			if b < minBlock {
				minBlock = b
			}
			if b > maxBlock {
				maxBlock = b
			}
		}
		ct.mu.Unlock()

		if first {
			fmt.Printf("\n=== %s ===\n", upperName)
			fmt.Println("  No blocks observed")
			continue
		}

		report := tracker.Report(upperName, minBlock, maxBlock)
		PrintReport(report)
	}
}
