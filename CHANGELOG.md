# Changelog

## 1.0.0 (2026-06-29)

### Initial Release

- **Nodes**
  - `edgelink-pg-config` — PostgreSQL connection configuration node
  - `edgelink-pg-store` — Batch write node with buffer, retry, and multi-format input

- **Features**
  - Three auto-detected input formats: MC driver, batch rows, single-row object
  - Global connection pool with reference counting (multi-node sharing)
  - Batch INSERT with parameterized queries and `ON CONFLICT DO NOTHING`
  - Dual buffer protection: `bufferMax` + `retryBufferMax` with FIFO overflow
  - Fixed-interval retry (no backoff) for connection failures
  - 3-tier error classification: connection, table-not-found, data-error
  - Auto-create table + index + TimescaleDB hypertable (MC format)
  - Dynamic table name via `${deviceId}` template, `msg.tableName`, or `msg.topic`
  - `_writing` mutex lock preventing concurrent INSERT race conditions
  - `close` handler with `done()` callback ensuring last-flush before shutdown
  - Real-time status indicator (green/yellow/red)
  - ES5 syntax, Node-RED 3.x/4.x compatible
