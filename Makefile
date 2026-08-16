HARNESS_RUN_ID ?= local
HARNESS_TOXIPROXY_PORT ?= 18474
HARNESS_PROXY_A_PORT ?= 12100
HARNESS_PROXY_B_PORT ?= 12101

export HARNESS_TOXIPROXY_PORT
export HARNESS_PROXY_A_PORT
export HARNESS_PROXY_B_PORT

COMPOSE_PROJECT_NAME := overmesh-harness-$(HARNESS_RUN_ID)
COMPOSE_FILE := harness/environments/azurite/compose.yaml
COMPOSE := COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME) docker compose -f $(COMPOSE_FILE)
HARNESS := cargo run --quiet -p overmesh-harness --

.PHONY: harness-certs dev-up dev-down dev-reset fault-reset gateway-smoke reconciler-smoke validate-system harness-list harness-run-all version-check test-pr test-main test-nightly test-live-azure test-release

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
	$(COMPOSE) up -d --wait storage-a storage-b toxiproxy
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

test-pr: version-check
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace
	$(HARNESS) run-all --no-report

test-main: test-pr
	./harness/scripts/test-main.sh

test-nightly: test-main
	$(HARNESS) generate-dataset .harness/generated/nightly-16m.bin --size 16777216 --seed 20260815

test-live-azure:
	$${HARNESS_LIVE_AZURE_COMMAND:-./harness/environments/azure/validate-storage-authorization.sh}

test-release: test-main test-live-azure
