package worker

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fystack/multichain-indexer/internal/indexer"
	"github.com/fystack/multichain-indexer/internal/rpc"
	"github.com/fystack/multichain-indexer/internal/rpc/aptos"
	"github.com/fystack/multichain-indexer/internal/rpc/bitcoin"
	"github.com/fystack/multichain-indexer/internal/rpc/cosmos"
	"github.com/fystack/multichain-indexer/internal/rpc/evm"
	"github.com/fystack/multichain-indexer/internal/rpc/solana"
	"github.com/fystack/multichain-indexer/internal/rpc/stellar"
	"github.com/fystack/multichain-indexer/internal/rpc/sui"
	tonrpc "github.com/fystack/multichain-indexer/internal/rpc/ton"
	"github.com/fystack/multichain-indexer/internal/rpc/tron"
	"github.com/fystack/multichain-indexer/internal/status"
	"github.com/fystack/multichain-indexer/internal/rpc/xrp"
	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/enum"
	"github.com/fystack/multichain-indexer/pkg/common/logger"
	"github.com/fystack/multichain-indexer/pkg/events"
	"github.com/fystack/multichain-indexer/pkg/infra"
	"github.com/fystack/multichain-indexer/pkg/ratelimiter"
	"github.com/fystack/multichain-indexer/pkg/store/blockstore"
	"github.com/fystack/multichain-indexer/pkg/store/missingblockstore"
	"github.com/fystack/multichain-indexer/pkg/store/pubkeystore"
	tonaddr "github.com/xssnick/tonutils-go/address"
)

// WorkerDeps bundles dependencies injected into workers.
type WorkerDeps struct {
	Ctx            context.Context
	KVStore        infra.KVStore
	BlockStore     blockstore.Store
	Emitter        events.Emitter
	Pubkey         pubkeystore.Store
	MissingBlocks  missingblockstore.MissingBlocksStore
	FailedChan     chan FailedBlockEvent
	Observer       BlockResultObserver
	StatusRegistry status.StatusRegistry
}

// ManagerConfig defines which workers to enable per chain.
type ManagerConfig struct {
	Chains          []string
	EnableRegular   bool
	EnableRescanner bool
	EnableCatchup   bool
	EnableManual    bool
	Observer        BlockResultObserver
}

// setObserverOnWorkers injects the observer callback into each worker's BaseWorker.
func setObserverOnWorkers(workers []Worker, obs BlockResultObserver) {
	if obs == nil {
		return
	}
	for _, w := range workers {
		switch wt := w.(type) {
		case *RegularWorker:
			wt.BaseWorker.observer = obs
		case *CatchupWorker:
			wt.BaseWorker.observer = obs
		case *RescannerWorker:
			wt.BaseWorker.observer = obs
		case *ManualWorker:
			wt.BaseWorker.observer = obs
		case *MempoolWorker:
			wt.BaseWorker.observer = obs
		}
	}
}

const (
	tonPreloadJettonResolveTimeout     = 6 * time.Second
	tonPreloadJettonConcurrencyDefault = 8
)

// BuildWorkers constructs workers for a given mode.
// Note: This function now expects the indexer to be created with the appropriate mode-scoped rate limiter
func BuildWorkers(
	idxr indexer.Indexer,
	cfg config.ChainConfig,
	mode WorkerMode,
	deps WorkerDeps,
) []Worker {
	var workers []Worker

	switch mode {
	case ModeRegular:
		workers = []Worker{
			NewRegularWorker(
				deps.Ctx,
				idxr,
				cfg,
				deps.KVStore,
				deps.BlockStore,
				deps.Emitter,
				deps.Pubkey,
				deps.FailedChan,
				deps.StatusRegistry,
			),
		}
	case ModeCatchup:
		workers = []Worker{
			NewCatchupWorker(
				deps.Ctx,
				idxr,
				cfg,
				deps.KVStore,
				deps.BlockStore,
				deps.Emitter,
				deps.Pubkey,
				deps.FailedChan,
				deps.StatusRegistry,
			),
		}
	case ModeRescanner:
		workers = []Worker{
			NewRescannerWorker(
				deps.Ctx,
				idxr,
				cfg,
				deps.KVStore,
				deps.BlockStore,
				deps.Emitter,
				deps.Pubkey,
				deps.FailedChan,
				deps.StatusRegistry,
			),
		}
	case ModeManual:
		workers = []Worker{
			NewManualWorker(
				deps.Ctx,
				idxr,
				cfg,
				deps.KVStore,
				deps.MissingBlocks,
				deps.BlockStore,
				deps.Emitter,
				deps.Pubkey,
				deps.FailedChan,
				deps.StatusRegistry,
			),
		}
	case ModeMempool:
		workers = []Worker{
			NewMempoolWorker(
				deps.Ctx,
				idxr,
				cfg,
				deps.KVStore,
				deps.BlockStore,
				deps.Emitter,
				deps.Pubkey,
				deps.FailedChan,
				deps.StatusRegistry,
			),
		}
	default:
		return nil
	}

	setObserverOnWorkers(workers, deps.Observer)
	return workers
}

func newEVMProvider(chainName string, idx int, node config.NodeConfig, timeout time.Duration,
	rl *ratelimiter.PooledRateLimiter) *rpc.Provider {
	client := evm.NewEthereumClient(
		node.URL,
		&rpc.AuthConfig{Type: rpc.AuthType(node.Auth.Type), Key: node.Auth.Key, Value: node.Auth.Value},
		timeout, rl,
	)
	if len(node.Headers) > 0 {
		client.SetCustomHeaders(node.Headers)
	}
	return &rpc.Provider{
		Name:       chainName + "-" + strconv.Itoa(idx),
		URL:        node.URL,
		Network:    chainName,
		ClientType: "rpc",
		Client:     client,
		State:      rpc.StateHealthy,
	}
}

// buildEVMIndexer constructs an EVM indexer with failover and providers.
func buildEVMIndexer(chainName string, chainCfg config.ChainConfig, mode WorkerMode, pubkeyStore pubkeystore.Store) indexer.Indexer {
	failover := rpc.NewFailover[evm.EthereumAPI](nil)
	var traceFailover *rpc.Failover[evm.EthereumAPI]

	// Main pool rate limiter
	rl := ratelimiter.GetOrCreateSharedPooledRateLimiter(
		chainName, chainCfg.Throttle.RPS, chainCfg.Throttle.Burst,
	)

	// Trace pool rate limiter — only created when debug_trace is enabled.
	// Dedicated budget to avoid starving main pool.
	// Defaults to half the main pool if not explicitly configured.
	var traceRL *ratelimiter.PooledRateLimiter
	if chainCfg.DebugTrace {
		traceRPS := chainCfg.TraceThrottle.RPS
		traceBurst := chainCfg.TraceThrottle.Burst
		if traceRPS <= 0 {
			traceRPS = max(1, chainCfg.Throttle.RPS/2)
		}
		if traceBurst <= 0 {
			traceBurst = max(1, chainCfg.Throttle.Burst/2)
		}
		// Note: keyed by chainName (not node URL), so all trace providers for this chain
		// share one budget. This is intentional — trace_rps/trace_burst is a chain-level
		// cap, not per-node. Same pattern as the main rate limiter.
		traceRL = ratelimiter.GetOrCreateScopedPooledRateLimiter(chainName, "trace", traceRPS, traceBurst)
	}

	for i, node := range chainCfg.Nodes {
		// Main pool provider
		failover.AddProvider(newEVMProvider(chainName, i+1, node, chainCfg.Client.Timeout, rl))

		// Trace pool: SEPARATE provider instance — no shared mutable state
		if node.DebugTrace && chainCfg.DebugTrace {
			if traceFailover == nil {
				traceFailover = rpc.NewFailover[evm.EthereumAPI](nil)
			}
			traceFailover.AddProvider(newEVMProvider(chainName+"-trace", i+1, node, chainCfg.Client.Timeout, traceRL))
		}
	}

	if chainCfg.DebugTrace && traceFailover == nil {
		logger.Warn("debug_trace enabled but no nodes have debug_trace: true", "chain", chainName)
	}

	return indexer.NewEVMIndexer(chainName, chainCfg, failover, traceFailover, pubkeyStore)
}

// buildTronIndexer constructs a Tron indexer with failover and providers.
func buildTronIndexer(chainName string, chainCfg config.ChainConfig, mode WorkerMode, pubkeyStore pubkeystore.Store) indexer.Indexer {
	failover := rpc.NewFailover[tron.TronAPI](nil)

	// Shared rate limiter for all workers of this chain (global across regular, catchup, etc.)
	rl := ratelimiter.GetOrCreateSharedPooledRateLimiter(
		chainName, chainCfg.Throttle.RPS, chainCfg.Throttle.Burst,
	)

	for i, node := range chainCfg.Nodes {
		client := tron.NewTronClient(
			node.URL,
			&rpc.AuthConfig{
				Type:  rpc.AuthType(node.Auth.Type),
				Key:   node.Auth.Key,
				Value: node.Auth.Value,
			},
			chainCfg.Client.Timeout,
			rl,
		)

		failover.AddProvider(&rpc.Provider{
			Name:       chainName + "-" + strconv.Itoa(i+1),
			URL:        node.URL,
			Network:    chainName,
			ClientType: "rpc",
			Client:     client,
			State:      rpc.StateHealthy, // Initialize as healthy
		})
	}

	return indexer.NewTronIndexer(chainName, chainCfg, failover, pubkeyStore)
}

// buildBitcoinIndexer constructs a Bitcoin indexer with failover and providers.
func buildBitcoinIndexer(
	chainName string,
	chainCfg config.ChainConfig,
	mode WorkerMode,
	pubkeyStore pubkeystore.Store,
) indexer.Indexer {
	failover := rpc.NewFailover[bitcoin.BitcoinAPI](nil)

	// Shared rate limiter for all workers of this chain (global across regular, catchup, etc.)
	rl := ratelimiter.GetOrCreateSharedPooledRateLimiter(
		chainName, chainCfg.Throttle.RPS, chainCfg.Throttle.Burst,
	)

	for i, node := range chainCfg.Nodes {
		client := bitcoin.NewBitcoinClient(
			node.URL,
			&rpc.AuthConfig{
				Type:  rpc.AuthType(node.Auth.Type),
				Key:   node.Auth.Key,
				Value: node.Auth.Value,
			},
			chainCfg.Client.Timeout,
			rl,
		)

		failover.AddProvider(&rpc.Provider{
			Name:       chainName + "-" + strconv.Itoa(i+1),
			URL:        node.URL,
			Network:    chainName,
			ClientType: "rpc",
			Client:     client,
			State:      rpc.StateHealthy, // Initialize as healthy
		})
	}

	return indexer.NewBitcoinIndexer(chainName, chainCfg, failover, pubkeyStore)
}

// buildSolanaIndexer constructs a Solana indexer with failover and providers.
func buildSolanaIndexer(chainName string, chainCfg config.ChainConfig, mode WorkerMode, pubkeyStore pubkeystore.Store) indexer.Indexer {
	failover := rpc.NewFailover[solana.SolanaAPI](nil)

	rl := ratelimiter.GetOrCreateSharedPooledRateLimiter(
		chainName, chainCfg.Throttle.RPS, chainCfg.Throttle.Burst,
	)

	for i, node := range chainCfg.Nodes {
		client := solana.NewSolanaClient(
			node.URL,
			&rpc.AuthConfig{
				Type:  rpc.AuthType(node.Auth.Type),
				Key:   node.Auth.Key,
				Value: node.Auth.Value,
			},
			chainCfg.Client.Timeout,
			rl,
		)

		failover.AddProvider(&rpc.Provider{
			Name:       chainName + "-" + strconv.Itoa(i+1),
			URL:        node.URL,
			Network:    chainName,
			ClientType: "rpc",
			Client:     client,
			State:      rpc.StateHealthy,
		})
	}

	return indexer.NewSolanaIndexer(chainName, chainCfg, failover, pubkeyStore)
}

// buildSuiIndexer constructs a Sui indexer with failover and providers.
func buildSuiIndexer(
	chainName string,
	chainCfg config.ChainConfig,
	mode WorkerMode,
	pubkeyStore pubkeystore.Store,
) indexer.Indexer {
	failover := rpc.NewFailover[sui.SuiAPI](nil)

	for i, node := range chainCfg.Nodes {
		client := sui.NewSuiClient(node.URL)

		failover.AddProvider(&rpc.Provider{
			Name:       chainName + "-" + strconv.Itoa(i+1),
			URL:        node.URL,
			Network:    chainName,
			ClientType: "grpc",
			Client:     client,
			State:      rpc.StateHealthy,
		})
	}

	return indexer.NewSuiIndexer(chainName, chainCfg, failover, pubkeyStore)
}

// buildCosmosIndexer constructs a Cosmos indexer with failover and providers.
func buildCosmosIndexer(
	chainName string,
	chainCfg config.ChainConfig,
	mode WorkerMode,
	pubkeyStore pubkeystore.Store,
) indexer.Indexer {
	failover := rpc.NewFailover[cosmos.CosmosAPI](nil)

	rl := ratelimiter.GetOrCreateSharedPooledRateLimiter(
		chainName, chainCfg.Throttle.RPS, chainCfg.Throttle.Burst,
	)

	for i, node := range chainCfg.Nodes {
		client := cosmos.NewCosmosClient(
			node.URL,
			&rpc.AuthConfig{
				Type:  rpc.AuthType(node.Auth.Type),
				Key:   node.Auth.Key,
				Value: node.Auth.Value,
			},
			chainCfg.Client.Timeout,
			rl,
		)

		failover.AddProvider(&rpc.Provider{
			Name:       chainName + "-" + strconv.Itoa(i+1),
			URL:        node.URL,
			Network:    chainName,
			ClientType: rpc.ClientTypeREST,
			Client:     client,
			State:      rpc.StateHealthy,
		})
	}

	return indexer.NewCosmosIndexer(chainName, chainCfg, failover, pubkeyStore)
}

// buildAptosIndexer constructs an Aptos indexer with failover and providers.
func buildAptosIndexer(
	chainName string,
	chainCfg config.ChainConfig,
	mode WorkerMode,
	pubkeyStore pubkeystore.Store,
) indexer.Indexer {
	failover := rpc.NewFailover[aptos.AptosAPI](nil)

	rl := ratelimiter.GetOrCreateSharedPooledRateLimiter(
		chainName, chainCfg.Throttle.RPS, chainCfg.Throttle.Burst,
	)

	for i, node := range chainCfg.Nodes {
		client := aptos.NewAptosClient(
			node.URL,
			&rpc.AuthConfig{
				Type:  rpc.AuthType(node.Auth.Type),
				Key:   node.Auth.Key,
				Value: node.Auth.Value,
			},
			chainCfg.Client.Timeout,
			rl,
		)

		failover.AddProvider(&rpc.Provider{
			Name:       chainName + "-" + strconv.Itoa(i+1),
			URL:        node.URL,
			Network:    chainName,
			ClientType: rpc.ClientTypeREST,
			Client:     client,
			State:      rpc.StateHealthy,
		})
	}

	return indexer.NewAptosIndexer(chainName, chainCfg, failover, pubkeyStore)
}

// buildTonIndexer constructs a TON indexer using tonutils lite-server client.
func buildTonIndexer(
	chainName string,
	chainCfg config.ChainConfig,
	pubkeyStore pubkeystore.Store,
	db *sql.DB,
) indexer.Indexer {
	if len(chainCfg.Nodes) == 0 {
		logger.Fatal("TON chain has no configured nodes", "chain", chainName)
	}

	client, err := tonrpc.NewClient(context.Background(), tonrpc.ClientConfig{
		ConfigURL: chainCfg.Nodes[0].URL,
	})
	if err != nil {
		logger.Fatal("Failed to initialize TON client", "chain", chainName, "err", err)
	}

	pretrackedJettonWallets := preloadTONJettonWallets(
		context.Background(),
		chainName,
		chainCfg,
		client,
		db,
	)

	return indexer.NewTonIndexer(
		chainName,
		chainCfg,
		client,
		pubkeyStore,
		pretrackedJettonWallets,
	)
}

// buildXRPIndexer constructs an XRP indexer with failover and providers.
func buildXRPIndexer(
	chainName string,
	chainCfg config.ChainConfig,
	mode WorkerMode,
	pubkeyStore pubkeystore.Store,
) indexer.Indexer {
	failover := rpc.NewFailover[xrp.XRPLAPI](nil)

	rl := ratelimiter.GetOrCreateSharedPooledRateLimiter(
		chainName, chainCfg.Throttle.RPS, chainCfg.Throttle.Burst,
	)

	for i, node := range chainCfg.Nodes {
		client := xrp.NewClient(
			node.URL,
			&rpc.AuthConfig{
				Type:  rpc.AuthType(node.Auth.Type),
				Key:   node.Auth.Key,
				Value: node.Auth.Value,
			},
			chainCfg.Client.Timeout,
			rl,
		)
		if len(node.Headers) > 0 {
			client.SetCustomHeaders(node.Headers)
		}

		failover.AddProvider(&rpc.Provider{
			Name:       chainName + "-" + strconv.Itoa(i+1),
			URL:        node.URL,
			Network:    chainName,
			ClientType: rpc.ClientTypeRPC,
			Client:     client,
			State:      rpc.StateHealthy,
		})
	}

	return indexer.NewXRPIndexer(chainName, chainCfg, failover, pubkeyStore)
}

// buildStellarIndexer constructs a Stellar indexer with failover and providers.
func buildStellarIndexer(
	chainName string,
	chainCfg config.ChainConfig,
	mode WorkerMode,
	kvstore infra.KVStore,
	pubkeyStore pubkeystore.Store,
) indexer.Indexer {
	failover := rpc.NewFailover[stellar.StellarAPI](nil)

	rl := ratelimiter.GetOrCreateSharedPooledRateLimiter(
		chainName, chainCfg.Throttle.RPS, chainCfg.Throttle.Burst,
	)

	for i, node := range chainCfg.Nodes {
		client := stellar.NewClient(
			node.URL,
			&rpc.AuthConfig{
				Type:  rpc.AuthType(node.Auth.Type),
				Key:   node.Auth.Key,
				Value: node.Auth.Value,
			},
			chainCfg.Client.Timeout,
			rl,
		)
		if len(node.Headers) > 0 {
			client.SetCustomHeaders(node.Headers)
		}

		failover.AddProvider(&rpc.Provider{
			Name:       chainName + "-" + strconv.Itoa(i+1),
			URL:        node.URL,
			Network:    chainName,
			ClientType: rpc.ClientTypeREST,
			Client:     client,
			State:      rpc.StateHealthy,
		})
	}

	return indexer.NewStellarIndexer(chainName, chainCfg, failover, kvstore, pubkeyStore)
}

func preloadTONJettonWallets(
	ctx context.Context,
	chainName string,
	chainCfg config.ChainConfig,
	client tonrpc.TonAPI,
	db *sql.DB,
) []string {
	if db == nil {
		logger.Warn("skip TON jetton wallet preload: database not configured", "chain", chainName)
		return nil
	}
	if client == nil {
		logger.Warn("skip TON jetton wallet preload: ton client not configured", "chain", chainName)
		return nil
	}

	owners, err := loadTONOwnerWallets(ctx, db)
	if err != nil {
		logger.Warn("failed loading TON owner wallets for jetton preload", "chain", chainName, "err", err)
		return nil
	}
	if len(owners) == 0 {
		logger.Info("skip TON jetton wallet preload: no TON owner wallets found", "chain", chainName)
		return nil
	}

	masters := normalizeTONAddresses(chainCfg.Ton.JettonMasters)
	if len(masters) == 0 {
		logger.Info("skip TON jetton wallet preload: no ton.jetton_masters configured", "chain", chainName)
		return nil
	}

	pairCount := len(owners) * len(masters)
	logger.Info(
		"Preloading TON jetton wallets",
		"chain", chainName,
		"owners", len(owners),
		"jetton_masters", len(masters),
		"pairs", pairCount,
	)

	wallets := deriveTONJettonWallets(ctx, chainName, chainCfg, client, owners, masters)
	logger.Info(
		"TON jetton wallet preload completed",
		"chain", chainName,
		"owners", len(owners),
		"jetton_masters", len(masters),
		"jetton_wallets", len(wallets),
	)

	return wallets
}

func loadTONOwnerWallets(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT address FROM wallet_addresses WHERE type = ?`, string(enum.NetworkTypeTon))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var addrs []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		addrs = append(addrs, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return normalizeTONAddresses(addrs), nil
}

// normalizeTONAddresses returns the distinct raw forms of the parseable addresses.
func normalizeTONAddresses(addrs []string) []string {
	set := make(map[string]struct{}, len(addrs))
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		normalized := normalizeTONAddressForPreload(a)
		if normalized == "" {
			continue
		}
		if _, ok := set[normalized]; ok {
			continue
		}
		set[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func deriveTONJettonWallets(
	ctx context.Context,
	chainName string,
	chainCfg config.ChainConfig,
	client tonrpc.TonAPI,
	owners []string,
	masters []string,
) []string {
	if len(owners) == 0 || len(masters) == 0 {
		return nil
	}

	workerCount := chainCfg.Throttle.Concurrency
	if workerCount <= 0 {
		workerCount = tonPreloadJettonConcurrencyDefault
	}
	if workerCount > 32 {
		workerCount = 32
	}

	type resolveJob struct {
		owner  string
		master string
	}

	jobs := make(chan resolveJob, workerCount*2)
	wallets := make(chan string, workerCount*2)

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()

		for job := range jobs {
			callCtx, cancel := context.WithTimeout(ctx, tonPreloadJettonResolveTimeout)
			wallet, err := client.ResolveJettonWalletAddress(callCtx, job.master, job.owner)
			cancel()
			if err != nil {
				continue
			}

			normalized := normalizeTONAddressForPreload(wallet)
			if normalized == "" {
				continue
			}
			wallets <- normalized
		}
	}

	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go worker()
	}

	go func() {
		defer close(jobs)
		for _, owner := range owners {
			for _, master := range masters {
				select {
				case <-ctx.Done():
					return
				case jobs <- resolveJob{
					owner:  owner,
					master: master,
				}:
				}
			}
		}
	}()

	go func() {
		wg.Wait()
		close(wallets)
	}()

	walletSet := make(map[string]struct{})
	for wallet := range wallets {
		walletSet[wallet] = struct{}{}
	}

	out := make([]string, 0, len(walletSet))
	for wallet := range walletSet {
		out = append(out, wallet)
	}

	return out
}

func normalizeTONAddressForPreload(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}

	if parsed, err := tonaddr.ParseRawAddr(addr); err == nil && parsed != nil {
		return parsed.StringRaw()
	}
	if parsed, err := tonaddr.ParseAddr(addr); err == nil && parsed != nil {
		return parsed.StringRaw()
	}

	return ""
}

// CreateManagerWithWorkers initializes manager and all workers for configured chains.
func CreateManagerWithWorkers(
	ctx context.Context,
	cfg *config.Config,
	kvstore infra.KVStore,
	db *sql.DB,
	emitter events.Emitter,
	managerCfg ManagerConfig,
) *Manager {
	// Shared stores
	blockStore := blockstore.NewBlockStore(kvstore)
	pubkeyStore := pubkeystore.NewPublicKeyStore(db)
	missingBlocks := missingblockstore.NewMissingBlocksStore(db)
	statusRegistry := status.NewRegistry()

	manager := NewManager(ctx, kvstore, blockStore, emitter, pubkeyStore)
	manager.registry = statusRegistry

	// Loop each chain
	for _, chainName := range managerCfg.Chains {
		chainCfg, err := cfg.Chains.GetChain(chainName)
		if err != nil {
			logger.Error("Chain not found in config", "chain", chainName, "err", err)
			continue
		}

		// Build indexer once - shared across all worker modes with global rate limiter
		var idxr indexer.Indexer
		switch chainCfg.Type {
		case enum.NetworkTypeEVM:
			idxr = buildEVMIndexer(chainName, chainCfg, ModeRegular, pubkeyStore)
		case enum.NetworkTypeTron:
			idxr = buildTronIndexer(chainName, chainCfg, ModeRegular, pubkeyStore)
		case enum.NetworkTypeBtc:
			idxr = buildBitcoinIndexer(chainName, chainCfg, ModeRegular, pubkeyStore)
		case enum.NetworkTypeSol:
			idxr = buildSolanaIndexer(chainName, chainCfg, ModeRegular, pubkeyStore)
		case enum.NetworkTypeSui:
			idxr = buildSuiIndexer(chainName, chainCfg, ModeRegular, pubkeyStore)
		case enum.NetworkTypeCosmos:
			idxr = buildCosmosIndexer(chainName, chainCfg, ModeRegular, pubkeyStore)
		case enum.NetworkTypeApt:
			idxr = buildAptosIndexer(chainName, chainCfg, ModeRegular, pubkeyStore)
		case enum.NetworkTypeTon:
			idxr = buildTonIndexer(chainName, chainCfg, pubkeyStore, db)
		case enum.NetworkTypeXRP:
			idxr = buildXRPIndexer(chainName, chainCfg, ModeRegular, pubkeyStore)
		case enum.NetworkTypeStellar:
			idxr = buildStellarIndexer(chainName, chainCfg, ModeRegular, kvstore, pubkeyStore)
		default:
			logger.Fatal("Unsupported network type", "chain", chainName, "type", chainCfg.Type)
		}
		statusRegistry.RegisterChain(idxr.GetName(), chainName, chainCfg)
		if existingFailed, err := blockStore.GetFailedBlocks(idxr.GetNetworkInternalCode()); err == nil {
			statusRegistry.SetFailedBlocks(idxr.GetName(), existingFailed)
		}
		if existingCatchup, err := blockStore.GetCatchupProgress(idxr.GetNetworkInternalCode()); err == nil {
			statusRegistry.SetCatchupRanges(idxr.GetName(), existingCatchup)
		} else {
			logger.Warn("Failed to load catchup progress for status registry",
				"chain", chainName,
				"internal_code", idxr.GetNetworkInternalCode(),
				"error", err,
			)
		}

		failedChan := make(chan FailedBlockEvent, 100)

		// Worker deps
		deps := WorkerDeps{
			Ctx:            ctx,
			KVStore:        kvstore,
			BlockStore:     blockStore,
			Emitter:        emitter,
			Pubkey:         pubkeyStore,
			MissingBlocks:  missingBlocks,
			FailedChan:     failedChan,
			Observer:       managerCfg.Observer,
			StatusRegistry: statusRegistry,
		}

		// Helper: add workers if enabled (all modes share the same indexer and global rate limiter).
		addIfEnabled := func(mode WorkerMode, enabled bool) {
			if enabled {
				ws := BuildWorkers(idxr, chainCfg, mode, deps)
				manager.AddWorkers(ws...)
				logger.Info("Worker enabled", "chain", chainName, "mode", mode)
			} else {
				logger.Info("Worker disabled", "chain", chainName, "mode", mode)
			}
		}

		addIfEnabled(ModeRegular, managerCfg.EnableRegular || cfg.Services.Worker.Regular.Enabled)
		addIfEnabled(
			ModeRescanner,
			managerCfg.EnableRescanner || cfg.Services.Worker.Rescanner.Enabled,
		)
		addIfEnabled(ModeCatchup, managerCfg.EnableCatchup || cfg.Services.Worker.Catchup.Enabled)
		addIfEnabled(ModeManual, managerCfg.EnableManual || cfg.Services.Worker.Manual.Enabled)

		// Mempool worker is Bitcoin-specific (0-conf transaction tracking)
		if chainCfg.Type == enum.NetworkTypeBtc {
			addIfEnabled(ModeMempool, cfg.Services.Worker.Mempool.Enabled)
		}
	}

	return manager
}
