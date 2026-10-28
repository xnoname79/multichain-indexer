package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alecthomas/kong"

	"github.com/fystack/multichain-indexer/internal/status"
	"github.com/fystack/multichain-indexer/internal/worker"
	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/logger"
	"github.com/fystack/multichain-indexer/pkg/events"
	"github.com/fystack/multichain-indexer/pkg/infra"
	"github.com/fystack/multichain-indexer/pkg/kvstore"
	"github.com/fystack/multichain-indexer/pkg/store/missingblockstore"
	"github.com/fystack/multichain-indexer/pkg/store/pubkeystore"
)

type CLI struct {
	Index IndexCmd `cmd:"" help:"Start the multi-chain transaction indexer with configurable worker modes."`
}

type IndexCmd struct {
	// Core configuration
	ConfigPath string `help:"Path to configuration file containing chain and worker settings." default:"configs/config.yaml" short:"c" name:"config"`

	// Chain selection
	Chains []string `help:"Specific blockchain chains to index (comma-separated). If not specified, all configured chains will be indexed." sep:"," short:"n" name:"chains" placeholder:"ethereum,polygon"`

	// Logging
	Debug bool `help:"Enable debug-level logging for detailed troubleshooting information." short:"d" name:"debug"`

	// Worker modes (these can override config settings)
	EnableCatchup bool `help:"Enable catchup worker to process historical blocks alongside regular indexing. Overrides config setting." name:"catchup"`
	EnableManual  bool `help:"Enable manual worker for processing block ranges queued via POST /manual-ranges. Overrides config setting."     name:"manual"`

	// Block starting point
	FromLatest bool `help:"Start indexing from the latest blockchain block instead of configured starting points. Useful for fresh deployments." name:"from-latest"`
}

func (c *IndexCmd) Run() error {
	runIndexer(c.Chains, c.ConfigPath, c.Debug, c.EnableManual, c.EnableCatchup, c.FromLatest)
	return nil
}

func main() {
	var cli CLI
	ctx := kong.Parse(
		&cli,
		kong.Name("transaction-indexer"),
		kong.Description(
			"Multi-chain blockchain transaction indexer with support for regular, catchup, manual, and rescanner worker modes.",
		),
		kong.UsageOnError(),
		kong.Vars{
			"version": "1.0.0", // You can add version info
		},
	)
	err := ctx.Run()
	ctx.FatalIfErrorf(err)
}

func runIndexer(chains []string, configPath string, debug, manual, catchup, fromLatest bool) {
	ctx := context.Background()
	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to load configuration:", err)
		os.Exit(1)
	}

	if err := logger.InitFromConfig(cfg.Logging.Mode, cfg.Logging.Format, debug); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to initialize logger:", err)
		os.Exit(1)
	}
	logger.Info("Config loaded", "environment", cfg.Environment)

	services := cfg.Services
	dbPath := services.DBPath
	if dbPath == "" {
		dbPath = "data/indexer.db"
	}
	db, err := infra.OpenSQLite(dbPath)
	if err != nil {
		logger.Fatal("Open SQLite failed", "path", dbPath, "err", err)
	}
	defer db.Close()
	logger.Info("SQLite opened", "path", dbPath)

	kvstore := kvstore.NewSQLiteStore(db, infra.JSON)
	emitter := events.NewEmitter(db, services.Webhook)
	defer emitter.Close()

	// If no chains specified, use all configured chains
	if len(chains) == 0 {
		chains = cfg.Chains.EnabledNames()
		logger.Info("No chains specified, using enabled configured chains", "chains", chains)
	} else {
		logger.Info("Indexing specified chains", "chains", chains)
	}

	// Validate chains
	if err := cfg.Chains.Validate(chains); err != nil {
		logger.Fatal("Validate chains failed", "err", err)
	}

	// Override from_latest from CLI if requested
	if fromLatest {
		cfg.Chains.OverrideFromLatest(chains)
		logger.Info("Starting from latest block for all specified chains", "chains", chains)
	}

	// Create manager with all workers using factory
	managerCfg := worker.ManagerConfig{
		Chains:        chains,
		EnableCatchup: catchup,
		EnableManual:  manual,
	}

	manager := worker.CreateManagerWithWorkers(
		ctx,
		cfg,
		kvstore,
		db,
		emitter,
		managerCfg,
	)

	api := &adminAPI{
		token:   services.APIToken,
		cfg:     cfg,
		pubkeys: pubkeystore.NewPublicKeyStore(db),
		ranges:  missingblockstore.NewMissingBlocksStore(db),
	}
	healthServer := startHealthServer(cfg.Services.Port, cfg, manager, api)

	// Start all workers
	logger.Info("Starting all workers")
	manager.Start()

	logger.Info("🚀 Transaction indexer is running... Press Ctrl+C to stop")
	waitForShutdown()

	logger.Info("Shutting down indexer...")

	// Stop workers first so health endpoint can report during drain
	manager.Stop()

	// Then shutdown health server
	if healthServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("Health server shutdown failed", "error", err)
		} else {
			logger.Info("Health server stopped gracefully")
		}
	}

	logger.Info("Indexer stopped gracefully")
}

type HealthResponse struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Version   string    `json:"version"`
}

func startHealthServer(port int, cfg *config.Config, manager *worker.Manager, api *adminAPI) *http.Server {
	mux := http.NewServeMux()
	api.register(mux)

	version := cfg.Version
	if version == "" {
		version = "1.0.0" // fallback version
	}

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		response := HealthResponse{
			Status:    "ok",
			Timestamp: time.Now().UTC(),
			Version:   version,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		response := status.StatusResponse{
			Timestamp: time.Now().UTC(),
			Version:   version,
			Networks:  []status.NetworkStatus{},
		}
		if manager != nil {
			response = manager.StatusSnapshot(version)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		logger.Info("Health check server started", "port", port, "endpoints", []string{"/health", "/status"})
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Health server failed to start", "error", err)
		}
	}()

	return server
}

func waitForShutdown() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
}
