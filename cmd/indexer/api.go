package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/enum"
	"github.com/fystack/multichain-indexer/pkg/common/logger"
	"github.com/fystack/multichain-indexer/pkg/store/missingblockstore"
	"github.com/fystack/multichain-indexer/pkg/store/pubkeystore"
)

const (
	maxAPIBody          = 4 << 20
	maxAddressesPerCall = 10000
	maxAddressLen       = 255
	maxBlocksPerCall    = 100000
)

// adminAPI lets other systems register watched addresses and queue manual block ranges.
type adminAPI struct {
	token   string
	cfg     *config.Config
	pubkeys pubkeystore.Store
	ranges  missingblockstore.MissingBlocksStore
}

func (a *adminAPI) register(mux *http.ServeMux) {
	if a.token == "" {
		logger.Warn("services.api_token not set: POST /addresses and /manual-ranges are disabled")
		return
	}
	mux.HandleFunc("POST /addresses", a.auth(a.addAddresses))
	mux.HandleFunc("POST /manual-ranges", a.auth(a.addManualRange))
}

func (a *adminAPI) auth(next http.HandlerFunc) http.HandlerFunc {
	want := []byte("Bearer " + a.token)
	return func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxAPIBody)
		next(w, r)
	}
}

// POST /addresses {"type":"evm","addresses":["0xabc..."]}
// Addresses match exactly (case-sensitive), so send them in the form the chain reports.
func (a *adminAPI) addAddresses(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Type      enum.NetworkType `json:"type"`
		Addresses []string         `json:"addresses"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if !slices.Contains(enum.AllNetworkTypes, req.Type) {
		http.Error(w, fmt.Sprintf("unknown type %q", req.Type), http.StatusBadRequest)
		return
	}
	if len(req.Addresses) == 0 || len(req.Addresses) > maxAddressesPerCall {
		http.Error(w, fmt.Sprintf("addresses must have 1 to %d entries", maxAddressesPerCall), http.StatusBadRequest)
		return
	}
	// Validate everything before writing so a bad entry doesn't leave a partial import.
	for i, addr := range req.Addresses {
		if addr == "" || len(addr) > maxAddressLen || strings.TrimSpace(addr) != addr {
			http.Error(w, fmt.Sprintf("addresses[%d] is empty, too long or has surrounding spaces", i), http.StatusBadRequest)
			return
		}
	}
	for _, addr := range req.Addresses {
		if err := a.pubkeys.Save(req.Type, addr); err != nil {
			logger.Error("Save address failed", "type", req.Type, "err", err)
			http.Error(w, "save failed", http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /manual-ranges {"chain":"ethereum_mainnet","start":100,"end":200}
func (a *adminAPI) addManualRange(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Chain string `json:"chain"`
		Start uint64 `json:"start"`
		End   uint64 `json:"end"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	chainCfg, err := a.cfg.Chains.GetChain(req.Chain)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Start == 0 || req.End < req.Start || req.End-req.Start >= maxBlocksPerCall {
		http.Error(w, fmt.Sprintf("need 1 <= start <= end and at most %d blocks per call", maxBlocksPerCall), http.StatusBadRequest)
		return
	}
	if err := a.ranges.AddMissingBlockRange(r.Context(), chainCfg.InternalCode, req.Start, req.End); err != nil {
		logger.Error("Queue manual range failed", "chain", req.Chain, "err", err)
		http.Error(w, "queue failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
