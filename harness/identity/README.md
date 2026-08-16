# Test Identity

This directory contains public JWKS fixtures for the deterministic local test
issuer.

The matching private key is derived only by harness test code. It is not a
production credential. Production gateways must trust Microsoft Entra JWKS
material supplied through their deployment configuration.

Local process tests write separate caller, gateway-control, and
reconciler-control token files even though Azurite cannot enforce their Azure
RBAC differences. Rust credential types and backend operation families enforce
their non-interchangeability locally; the live Azure provider proves the
actual role boundaries.
