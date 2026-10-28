INDEXER_BINARY := indexer

.PHONY: build build-all clean run stop run-ethereum-mainnet run-tron-mainnet run-bitcoin-testnet run-bitcoin-mainnet

build:
	go build -o $(INDEXER_BINARY) ./cmd/indexer

build-blockreliability:
	go build -o block-reliability ./cmd/devtools/blockreliability

build-all: build build-blockreliability

clean:
	rm -f $(INDEXER_BINARY) block-reliability

run:
	./$(INDEXER_BINARY) index --catchup

run-ethereum-mainnet: build
	./$(INDEXER_BINARY) index --chain=ethereum_mainnet --catchup

run-tron-mainnet: build
	./$(INDEXER_BINARY) index --chain=tron_mainnet --catchup

run-bitcoin-testnet: build
	./$(INDEXER_BINARY) index --chain=bitcoin_testnet --catchup

run-bitcoin-mainnet: build
	./$(INDEXER_BINARY) index --chain=bitcoin_mainnet --catchup

stop:
	@echo "Killing indexer..."
	- pkill -f "./$(INDEXER_BINARY)" || true

fmt:
	go fmt ./...
