# Changelog

## 0.3.0 - Unreleased

- Move the adapter to `fips-core 0.4.0` without changing its bounded FSP
  datagram protocol.
- Keep external peerfinding on the application-provided `EventBus`; the
  adapter neither opens relay sockets nor adds an adapter-local workaround.
