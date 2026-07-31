SHELL := /bin/sh

INTEGRATION_COMPOSE = docker compose -p oh-my-lazier-integration -f docker-compose.integration.yml
INTEGRATION_POSTGRES_URL = postgres://laz_worker:laz_worker@localhost:55432/laz_worker?sslmode=disable
INTEGRATION_RUSTACK_ENDPOINT = http://localhost:4566

E2E_COMPOSE = docker compose -p oh-my-lazier-e2e -f docker-compose.e2e.yml
E2E_TMP_DIR = tmp/e2e
E2E_DEPLOY_SCRIPT_PARAMS ?= config/scripts/e2e-local-deploy.json
E2E_RUN_SCRIPT_PARAMS ?= config/scripts/e2e-local-run.json
E2E_CI_RUN_SCRIPT_PARAMS ?= config/scripts/e2e-local-run-ci.json
E2E_DEPLOYER_PRIVATE_KEY ?= 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
E2E_WORKER_PRIVATE_KEY ?= 0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d
E2E_KEYSTORE_PASSWORD ?= local-e2e-password
E2E_WORKER_UP_FLAGS ?= --build
E2E_AWS_ACCESS_KEY_ID ?= test
E2E_AWS_SECRET_ACCESS_KEY ?= test
E2E_KMS_REGION ?= us-east-1
E2E_KMS_HOST_ENDPOINT ?= http://127.0.0.1:4566
E2E_CHAIN_A_HOST_RPC_URL ?= http://127.0.0.1:18545
E2E_CHAIN_B_HOST_RPC_URL ?= http://127.0.0.1:18546
E2E_HOST_DATABASE_URL ?= postgres://laz_worker:laz_worker@127.0.0.1:55433/laz_worker?sslmode=disable
E2E_CHAIN_A_CONTAINER_RPC_URL ?= $(E2E_CHAIN_A_HOST_RPC_URL)
E2E_CHAIN_B_CONTAINER_RPC_URL ?= $(E2E_CHAIN_B_HOST_RPC_URL)
E2E_CONTAINER_DATABASE_URL ?= $(E2E_HOST_DATABASE_URL)
E2E_KMS_CONTAINER_ENDPOINT ?= $(E2E_KMS_HOST_ENDPOINT)
E2E_CI_WORKER_IMAGE ?= oh-my-lazier-worker:e2e
E2E_CI_WORKER_NAME ?= oh-my-lazier-e2e-worker
E2E_CI_WORKER_RUN_FLAGS ?= --network host

.PHONY: check \
	generate-lzabi check-lzabi generate-pricing-abi check-pricing-abi \
	test-integration test-kms-rustack \
	e2e-local e2e-ci \
	security-check docker-smoke \
	fmt-go-check fmt-sol-check

check:
	npm run compile
	npm run typecheck
	npm run check:lzabi
	npm run check:pricing-abi
	npx hardhat test solidity --no-compile
	npm run test:scripts
	go test ./...
	npm run check:runbooks
	golangci-lint run ./...
	@$(MAKE) --no-print-directory fmt-go-check
	@$(MAKE) --no-print-directory fmt-sol-check

generate-lzabi:
	npm run generate:lzabi

check-lzabi:
	npm run check:lzabi

generate-pricing-abi:
	npm run generate:pricing-abi

check-pricing-abi:
	npm run check:pricing-abi

# Integration tests share one Postgres instance; chain/pathway pause and enable
# flags are load-bearing global state for the send-scope gates, so packages must
# not run in parallel against the same database (-p 1).
test-integration:
	@set -e; \
	cleanup() { \
		$(INTEGRATION_COMPOSE) down -v --remove-orphans; \
	}; \
	trap cleanup EXIT INT TERM; \
	$(INTEGRATION_COMPOSE) up -d --wait; \
	TEST_POSTGRES_URL="$(INTEGRATION_POSTGRES_URL)" RUSTACK_KMS_ENDPOINT="$(INTEGRATION_RUSTACK_ENDPOINT)" go test -count=1 -p 1 ./...; \

test-kms-rustack:
	@if [ -z "$$RUSTACK_KMS_ENDPOINT" ]; then \
		printf '%s\n' "RUSTACK_KMS_ENDPOINT is required"; \
		exit 1; \
	fi
	go test ./go/internal/signer/kms -run TestRustackKMSIntegrationSignsEthereumTransaction -count=1

e2e-local:
	rm -rf $(E2E_TMP_DIR)
	mkdir -p $(E2E_TMP_DIR)
	@set -e; \
	cleanup() { \
		$(E2E_COMPOSE) --profile worker down -v --remove-orphans; \
		rm -rf $(E2E_TMP_DIR); \
	}; \
	trap cleanup EXIT INT TERM; \
	npm run compile; \
	$(E2E_COMPOSE) up -d --wait postgres anvil-a anvil-b localstack; \
	AWS_ACCESS_KEY_ID="$(E2E_AWS_ACCESS_KEY_ID)" AWS_SECRET_ACCESS_KEY="$(E2E_AWS_SECRET_ACCESS_KEY)" AWS_REGION="$(E2E_KMS_REGION)" E2E_KMS_REGION="$(E2E_KMS_REGION)" E2E_KMS_HOST_ENDPOINT="$(E2E_KMS_HOST_ENDPOINT)" go run ./go/cmd/e2ekmskey -out "$(E2E_TMP_DIR)/kms.json"; \
	E2E_WORKER_PRIVATE_KEY="$(E2E_WORKER_PRIVATE_KEY)" E2E_KEYSTORE_PASSWORD="$(E2E_KEYSTORE_PASSWORD)" go run ./go/cmd/e2ekeystore -out "$(E2E_TMP_DIR)/worker-keystore.json"; \
	OML_SCRIPT_PARAMS="$(E2E_DEPLOY_SCRIPT_PARAMS)" OML_IGNITION_DIR="$(E2E_TMP_DIR)/ignition" E2E_DEPLOYER_PRIVATE_KEY="$(E2E_DEPLOYER_PRIVATE_KEY)" E2E_CHAIN_A_HOST_RPC_URL="$(E2E_CHAIN_A_HOST_RPC_URL)" E2E_CHAIN_B_HOST_RPC_URL="$(E2E_CHAIN_B_HOST_RPC_URL)" E2E_KMS_REGION="$(E2E_KMS_REGION)" E2E_KMS_HOST_ENDPOINT="$(E2E_KMS_HOST_ENDPOINT)" npm run e2e:deploy-local; \
	E2E_KEYSTORE_PASSWORD="$(E2E_KEYSTORE_PASSWORD)" go run ./go/cmd/configcheck -config "$(E2E_TMP_DIR)/worker.host.yaml"; \
	E2E_KEYSTORE_PASSWORD="$(E2E_KEYSTORE_PASSWORD)" $(E2E_COMPOSE) --profile worker up -d $(E2E_WORKER_UP_FLAGS) --wait worker; \
	OML_SCRIPT_PARAMS="$(E2E_RUN_SCRIPT_PARAMS)" E2E_DEPLOYER_PRIVATE_KEY="$(E2E_DEPLOYER_PRIVATE_KEY)" E2E_CHAIN_A_HOST_RPC_URL="$(E2E_CHAIN_A_HOST_RPC_URL)" E2E_CHAIN_B_HOST_RPC_URL="$(E2E_CHAIN_B_HOST_RPC_URL)" npm run e2e:run-local; \
	go run ./go/cmd/e2eindexcheck -config "$(E2E_TMP_DIR)/worker.host.yaml" -evidence "$(E2E_TMP_DIR)/multi-oft-send-indexer.json"; \
	$(E2E_COMPOSE) --profile worker stop worker; \
	go run ./go/cmd/e2ereplaycheck -config "$(E2E_TMP_DIR)/worker.host.yaml" -evidence "$(E2E_TMP_DIR)/destination-replay.json"

e2e-ci:
	rm -rf $(E2E_TMP_DIR)
	mkdir -p $(E2E_TMP_DIR)
	@set -e; \
	cleanup() { \
		status=$$?; \
		if [ $$status -ne 0 ]; then \
			docker logs "$(E2E_CI_WORKER_NAME)" >/dev/stderr 2>&1 || true; \
		fi; \
		docker rm -f "$(E2E_CI_WORKER_NAME)" >/dev/null 2>&1 || true; \
		rm -rf $(E2E_TMP_DIR); \
		exit $$status; \
	}; \
	trap cleanup EXIT INT TERM; \
	npm run compile; \
	AWS_ACCESS_KEY_ID="$(E2E_AWS_ACCESS_KEY_ID)" AWS_SECRET_ACCESS_KEY="$(E2E_AWS_SECRET_ACCESS_KEY)" AWS_REGION="$(E2E_KMS_REGION)" E2E_KMS_REGION="$(E2E_KMS_REGION)" E2E_KMS_HOST_ENDPOINT="$(E2E_KMS_HOST_ENDPOINT)" go run ./go/cmd/e2ekmskey -out "$(E2E_TMP_DIR)/kms.json"; \
	E2E_WORKER_PRIVATE_KEY="$(E2E_WORKER_PRIVATE_KEY)" E2E_KEYSTORE_PASSWORD="$(E2E_KEYSTORE_PASSWORD)" go run ./go/cmd/e2ekeystore -out "$(E2E_TMP_DIR)/worker-keystore.json"; \
	E2E_TMP_DIR="$(E2E_TMP_DIR)" \
	OML_SCRIPT_PARAMS="$(E2E_DEPLOY_SCRIPT_PARAMS)" \
	OML_IGNITION_DIR="$(E2E_TMP_DIR)/ignition" \
	E2E_DEPLOYER_PRIVATE_KEY="$(E2E_DEPLOYER_PRIVATE_KEY)" \
	E2E_KMS_REGION="$(E2E_KMS_REGION)" \
	E2E_KMS_HOST_ENDPOINT="$(E2E_KMS_HOST_ENDPOINT)" \
	E2E_KMS_CONTAINER_ENDPOINT="$(E2E_KMS_CONTAINER_ENDPOINT)" \
	E2E_CHAIN_A_HOST_RPC_URL="$(E2E_CHAIN_A_HOST_RPC_URL)" \
	E2E_CHAIN_B_HOST_RPC_URL="$(E2E_CHAIN_B_HOST_RPC_URL)" \
	E2E_CHAIN_A_CONTAINER_RPC_URL="$(E2E_CHAIN_A_CONTAINER_RPC_URL)" \
	E2E_CHAIN_B_CONTAINER_RPC_URL="$(E2E_CHAIN_B_CONTAINER_RPC_URL)" \
	E2E_HOST_DATABASE_URL="$(E2E_HOST_DATABASE_URL)" \
	E2E_CONTAINER_DATABASE_URL="$(E2E_CONTAINER_DATABASE_URL)" \
	npm run e2e:deploy-local; \
	E2E_KEYSTORE_PASSWORD="$(E2E_KEYSTORE_PASSWORD)" DATABASE_URL="$(E2E_HOST_DATABASE_URL)" go run ./go/cmd/configcheck -config "$(E2E_TMP_DIR)/worker.host.yaml"; \
	docker rm -f "$(E2E_CI_WORKER_NAME)" >/dev/null 2>&1 || true; \
	docker run -d --name "$(E2E_CI_WORKER_NAME)" $(E2E_CI_WORKER_RUN_FLAGS) \
		-v "$$(pwd)/$(E2E_TMP_DIR):/app/tmp/e2e:ro" \
		-e E2E_KEYSTORE_PASSWORD="$(E2E_KEYSTORE_PASSWORD)" \
		-e DATABASE_URL="$(E2E_CONTAINER_DATABASE_URL)" \
		-e AWS_ACCESS_KEY_ID="$(E2E_AWS_ACCESS_KEY_ID)" \
		-e AWS_SECRET_ACCESS_KEY="$(E2E_AWS_SECRET_ACCESS_KEY)" \
		-e AWS_REGION="$(E2E_KMS_REGION)" \
		-e AWS_EC2_METADATA_DISABLED=true \
		"$(E2E_CI_WORKER_IMAGE)" -config /app/tmp/e2e/worker.container.yaml; \
	OML_SCRIPT_PARAMS="$(E2E_CI_RUN_SCRIPT_PARAMS)" \
	E2E_DEPLOYER_PRIVATE_KEY="$(E2E_DEPLOYER_PRIVATE_KEY)" \
	E2E_CHAIN_A_HOST_RPC_URL="$(E2E_CHAIN_A_HOST_RPC_URL)" \
	E2E_CHAIN_B_HOST_RPC_URL="$(E2E_CHAIN_B_HOST_RPC_URL)" \
	npm run e2e:run-local; \
	go run ./go/cmd/e2eindexcheck -config "$(E2E_TMP_DIR)/worker.host.yaml" -evidence "$(E2E_TMP_DIR)/multi-oft-send-indexer.json"; \
	docker stop "$(E2E_CI_WORKER_NAME)" >/dev/null; \
	go run ./go/cmd/e2ereplaycheck -config "$(E2E_TMP_DIR)/worker.host.yaml" -evidence "$(E2E_TMP_DIR)/destination-replay.json"

security-check:
	npm run check:security-review
	npm run check:npm-audit-disposition
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

docker-smoke:
	docker build -t oh-my-lazier-worker:local .
	docker run --rm oh-my-lazier-worker:local -h

fmt-go-check:
	@files="$$(gofmt -l go)"; \
	if [ -n "$$files" ]; then \
		printf '%s\n' "$$files"; \
		exit 1; \
	fi

fmt-sol-check:
	forge fmt --check contracts

build-go:
	mkdir -p go/bin
	go build -o ./go/bin ./go/cmd/...
