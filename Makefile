HARNESS_RUN_ID ?= local
HARNESS_TOXIPROXY_PORT ?= 18474
HARNESS_PROXY_A_PORT ?= 12100
HARNESS_PROXY_B_PORT ?= 12101
HARNESS_PROXY_C_PORT ?= 12102

export HARNESS_TOXIPROXY_PORT
export HARNESS_PROXY_A_PORT
export HARNESS_PROXY_B_PORT
export HARNESS_PROXY_C_PORT

COMPOSE_PROJECT_NAME := overmesh-harness-$(HARNESS_RUN_ID)
COMPOSE_FILE := harness/environments/azurite/compose.yaml
COMPOSE := COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME) docker compose -f $(COMPOSE_FILE)
HARNESS := cargo run --quiet -p overmesh-harness --
ZOLA_VERSION := 0.23.3
PAGEFIND_VERSION := 1.5.2
ZOLA ?= zola
PAGEFIND ?= npx --yes pagefind@$(PAGEFIND_VERSION)
SITE_SERVE_HOST ?= 127.0.0.1
SITE_SERVE_PORT ?= 1111

.PHONY: harness-certs dev-up dev-down dev-reset fault-reset gateway-smoke placement-smoke reconciler-smoke validate-system harness-list harness-run-all version-check doc-check performance-contract-check site-content site-tool-check site-build site-serve infra-build test-pr test-main test-nightly test-pre-pr-live test-live-azure test-live-azure-storage test-live-azure-posture test-live-azure-gateway test-live-azure-client-compat test-live-azure-placement test-live-azure-reconciliation test-live-azure-performance test-release

HARNESS_CERT_DIR := .harness/certs
HARNESS_CERT := $(HARNESS_CERT_DIR)/azurite.pem
HARNESS_CERT_KEY := $(HARNESS_CERT_DIR)/azurite-key.pem

harness-certs:
	@mkdir -p $(HARNESS_CERT_DIR)
	@if [ ! -f $(HARNESS_CERT) ] || [ ! -f $(HARNESS_CERT_KEY) ]; then \
		openssl req -x509 -nodes -newkey rsa:2048 \
			-keyout $(HARNESS_CERT_KEY) \
			-out $(HARNESS_CERT) \
			-days 7 \
			-subj '/CN=localhost' \
			-addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' >/dev/null 2>&1; \
	fi

dev-up: harness-certs
	$(COMPOSE) up -d --wait storage-a storage-b storage-c toxiproxy
	$(COMPOSE) run --rm --no-deps toxiproxy-config
	$(HARNESS) doctor

dev-down:
	$(COMPOSE) down --volumes --remove-orphans

dev-reset:
	$(MAKE) dev-down HARNESS_RUN_ID=$(HARNESS_RUN_ID)
	$(MAKE) dev-up HARNESS_RUN_ID=$(HARNESS_RUN_ID)

fault-reset:
	$(HARNESS) fault reset

gateway-smoke:
	./harness/scripts/gateway-smoke.sh

placement-smoke:
	./harness/scripts/placement-smoke.sh

reconciler-smoke:
	./harness/scripts/reconciler-smoke.sh

validate-system:
	$(HARNESS) validate-system

harness-list:
	$(HARNESS) list

harness-run-all:
	$(HARNESS) run-all

version-check:
	$(HARNESS) version-check

doc-check:
	$(HARNESS) doc-check

performance-contract-check:
	python3 harness/environments/azure/performance/overmesh_live_performance.py \
		--contract harness/performance/live-v2.toml \
		--plan >/dev/null
	PYTHONPATH=harness/environments/azure/performance \
		python3 -m unittest discover \
		-s harness/environments/azure/performance \
		-p 'test_*.py'

site-content:
	$(HARNESS) site-content

site-tool-check:
	@test "$$($(ZOLA) --version 2>/dev/null)" = "zola $(ZOLA_VERSION)" || \
		{ echo "expected Zola $(ZOLA_VERSION); set ZOLA to the pinned binary" >&2; exit 1; }
	@test "$$($(PAGEFIND) --version 2>/dev/null)" = "pagefind $(PAGEFIND_VERSION)" || \
		{ echo "expected Pagefind $(PAGEFIND_VERSION); set PAGEFIND to the pinned binary" >&2; exit 1; }

site-build: site-content site-tool-check
	$(ZOLA) --root site check
	$(ZOLA) --root site build
	$(PAGEFIND) --site site/public

site-serve: site-content site-tool-check
	$(ZOLA) --root site check
	$(ZOLA) --root site build --base-url http://$(SITE_SERVE_HOST):$(SITE_SERVE_PORT)
	$(PAGEFIND) --site site/public
	python3 harness/scripts/site-serve.py --bind $(SITE_SERVE_HOST) --port $(SITE_SERVE_PORT) --directory site/public

infra-build:
	az bicep build --file infra/main.bicep --stdout >/dev/null

test-pr: version-check doc-check performance-contract-check
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace
	$(HARNESS) run-all --no-report

test-main: test-pr
	./harness/scripts/test-main.sh

test-nightly: test-main
	$(HARNESS) generate-dataset .harness/generated/nightly-16m.bin --size 16777216 --seed 20260815

test-live-azure-storage:
	./harness/environments/azure/validate-storage-authorization.sh

test-live-azure-posture:
	./harness/environments/azure/validate-live-posture.sh

test-live-azure-gateway:
	./harness/environments/azure/validate-gateway-authorization.sh

test-live-azure-client-compat:
	./harness/environments/azure/validate-client-compatibility.sh

test-live-azure-placement:
	./harness/environments/azure/validate-live-placement.sh

test-live-azure-reconciliation:
	./harness/environments/azure/validate-live-reconciliation.sh

test-live-azure-performance:
	./harness/environments/azure/validate-live-performance.sh

test-live-azure:
	$${HARNESS_LIVE_AZURE_COMMAND:-./harness/environments/azure/validate-live-azure.sh}

test-pre-pr-live: test-live-azure

test-release: test-main test-live-azure test-live-azure-performance
