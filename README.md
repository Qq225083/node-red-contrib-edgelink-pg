# node-red-contrib-edgelink-pg

[![npm version](https://img.shields.io/npm/v/node-red-contrib-edgelink-pg.svg)](https://www.npmjs.com/package/node-red-contrib-edgelink-pg)
[![license](https://img.shields.io/npm/l/node-red-contrib-edgelink-pg.svg)](LICENSE)
[![Node-RED](https://img.shields.io/badge/Node--RED-%E2%89%A53.0.0-red.svg)](https://nodered.org)

> PostgreSQL/TimescaleDB batch-write node for industrial SCADA edge computing.
> Zero-code pipeline: **PLC data → database**, with buffer, retry, and connection pooling.

---

## Why This Node?

| | Generic PG Node | **edgelink-pg-store** |
|---|---|---|
| **Write method** | Single-row SQL per msg | Batch buffer → one `INSERT`, **100× faster** |
| **PG connection lost** | msg dropped | FIFO retry queue, auto-replay |
| **Connection pool** | One pool per node | **Global singleton**, multi-node shared |
| **Race condition** | None | `_writing` mutex lock |
| **Memory safety** | None | `bufferMax` + `retryBufferMax` dual caps |
| **Upstream integration** | Write SQL manually | Native MC driver / Modbus format |
| **On shutdown** | Data lost | `flush → done()` guarantees last write |
| **Code size** | 800–2000 lines | **~400 lines** |

---

## Installation

```bash
cd ~/.node-red
npm install node-red-contrib-edgelink-pg
```

Requires Node.js ≥ 14 and Node-RED ≥ 3.0. The `pg` driver is installed automatically.

---

## Nodes

### `edgelink-pg-config` (config)

Stores PostgreSQL connection parameters. Multiple store nodes can reference a single config — they share one connection pool.

| Property | Default | Description |
|----------|---------|-------------|
| Name | `PG-本地` | Display name |
| Host | `127.0.0.1` | Database host |
| Port | `5432` | Database port |
| Database | `ruoyi_pg` | Database name |
| User | `postgres` | Database user |
| Password | *(empty)* | Database password |
| Max Connections | `10` | Pool max connections |
| Idle Timeout (ms) | `30000` | Idle connection TTL |

### `edgelink-pg-store` (output)

Receives data, buffers, batch-inserts, retries on failure.

| Property | Default | Description |
|----------|---------|-------------|
| PG Config | *(required)* | Reference to `edgelink-pg-config` |
| Table Name | `plc_data` | Table name; supports `${deviceId}` template |
| Batch Size | `100` | Rows per `INSERT` |
| Buffer Max | `5000` | Main buffer cap (prevents OOM) |
| Flush Interval (ms) | `5000` | Auto-flush timer; `0` = disabled |
| Retry Buffer Max | `1000` | Retry queue cap |
| Retry Interval (ms) | `5000` | Retry timer, fixed interval |
| Auto Create Table | `false` | `CREATE TABLE IF NOT EXISTS` (MC format only) |
| TimescaleDB Hypertable | `false` | `create_hypertable` (MC format only) |

---

## Input Formats (Auto-Detected)

The node automatically detects which format an incoming message uses. **No configuration required.**

### Format 1 — MC Driver (mitsubishi-read / modbus-read)

```javascript
msg.payload = {
  success: true,
  deviceId: "PLC-Oven-01",
  data: {
    "Temperature": {
      rawValue: 2530,          // raw PLC value
      engValue: 253.0,         // engineering value
      quality: 0,              // 0 = good
      ts: "2026-06-27T16:00:00.000Z",
      regType: "D"             // PLC register type
    },
    "Switch": {
      rawValue: 1, engValue: 1,
      quality: 0, ts: "2026-06-27T16:00:00.000Z",
      regType: "X"
    }
  }
}
```

Detection: `payload.data` exists with nested tag objects.  
Table schema: fixed 7 columns (`insert_time, device_id, tag_id, register_type, raw_value, eng_value, quality`).  
Supports `ON CONFLICT DO NOTHING`, `autoCreateTable`, TimescaleDB hypertable, and `${deviceId}` dynamic sharding.

### Format 2 — Batch Rows

```javascript
msg.payload = {
  rows: [
    { sensor: "temp", value: 25.5, ts: "2026-06-29T16:00:00Z" },
    { sensor: "press", value: 1.2, ts: "2026-06-29T16:00:00Z" }
  ]
}
msg.topic = "sensor_data"   // table name (optional)
```

Detection: `payload.rows` is an array.  
Columns: auto-detected from `Object.keys()` of the first row.  
Table name: `msg.tableName` → `msg.topic` → node config `tableName`.

### Format 3 — Single-Row Object

```javascript
msg.payload = {
  sensor: "temp",
  value: 25.5,
  ts: "2026-06-29T16:00:00Z"
}
msg.topic = "sensor_data"
```

Detection: plain object (no `.data`, no `.rows`).  
Auto-wrapped as a single-row batch.

### Dynamic Controls (All Formats)

| Field | Effect |
|-------|--------|
| `msg.tableName` | Override table name |
| `msg.topic` | Table name fallback (generic formats) |
| `msg.flush = true` | Force immediate INSERT |
| `payload.success === false` | Skip (upstream read failed) |

---

## Output Format

Two messages per input cycle:

```javascript
// 1. Immediately after input (data buffered)
{
  success: true,
  inserted: 0,
  buffered: 15,
  tableName: "plc_data",
  roundTimeMs: 1,
  originalData: { /* original msg.payload.data */ }
}

// 2. After batch INSERT completes (async)
{
  success: true,
  inserted: 100,
  failed: 0,
  buffered: 0,
  tableName: "plc_data",
  roundTimeMs: 45
}
```

---

## Architecture

```
 ┌──────────┐    ┌─────────────────────────────┐    ┌──────────┐
 │ mitsubishi│    │     edgelink-pg-store        │    │PostgreSQL│
 │  -read    │───→│                              │───→│/Timescale│
 │   PLC     │    │  buffer[] ──→ batch INSERT   │    │    DB    │
 └──────────┘    │    │           (parameterized) │    └──────────┘
                 │    ├──→ success → output       │
 ┌──────────┐    │    └──→ failure → retryBuffer  │
 │ inject   │───→│         │                      │
 │(generic) │    │         └──→ retryInterval     │
 └──────────┘    │              (fixed, no backoff)│
                 └─────────────────────────────┘

 Pool: { "user@host:port/db" → pg.Pool }  (global singleton, ref-counted)
 Mutex: _writing  (prevents concurrent INSERT)
```

**Key behaviors:**

- **Flush triggers**: `buffer.length ≥ batchSize` OR `msg.flush === true` OR `flushInterval` timer
- **`_writing` lock**: Only one `INSERT` in flight at a time; concurrent triggers are silently skipped
- **Column/table change**: Auto-flushes existing buffer before switching to new schema
- **retryBuffer**: FIFO queue; max-length capped; connection errors retry, data errors discard

---

## Error Classification

| Type | SQLSTATE | Action |
|------|----------|--------|
| Connection | `ECONNREFUSED`, `ETIMEDOUT`, `08xxx` | → retryBuffer, keep retrying |
| Table missing | `42P01` | MC format + autoCreateTable → create & retry; otherwise → discard |
| Data type | `22P02` | → discard (bad data, no point retrying) |
| Other | *everything else* | → retryBuffer |

---

## Table Structure (MC Format)

When `autoCreateTable = true`:

```sql
CREATE TABLE IF NOT EXISTS plc_data (
    insert_time   TIMESTAMPTZ NOT NULL,
    device_id     VARCHAR(64) NOT NULL,
    tag_id        VARCHAR(64) NOT NULL,
    register_type VARCHAR(8),
    raw_value     NUMERIC,
    eng_value     NUMERIC,
    quality       INTEGER DEFAULT 0,
    PRIMARY KEY (insert_time, device_id, tag_id)
);

CREATE INDEX IF NOT EXISTS idx_plc_data_dt
    ON plc_data (device_id, tag_id, insert_time DESC);
```

With `useTimescaleDB = true`:

```sql
SELECT create_hypertable('plc_data', 'insert_time', if_not_exists => TRUE);
```

> **Primary key limitation**: Sub-second duplicate `(time, device, tag)` records are rejected via `ON CONFLICT DO NOTHING`. Recommended polling interval ≥ 1 second.

---

## Quick Start

### 1. Add config node

Drag `edgelink-pg-config` to the canvas. Fill in your PostgreSQL credentials.

### 2. Wire up

```
[mitsubishi-read] → [edgelink-pg-store] → [debug]
                        ↑
               [edgelink-pg-config]
```

### 3. Deploy

Data starts flowing. Status indicator: 🟢 green (inserting) / 🟡 yellow (buffering) / 🔴 red (error).

### 4. Standalone test (no PLC needed)

Use an inject node with JSON payload:

```json
{"sensor": "temp", "value": 25.5}
```

Set `msg.topic` to `sensor_data`, wire to `edgelink-pg-store`. Deploy and press the inject button.

---

## Integration with EdgeLink Ecosystem

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────────────┐
│ mitsubishi-  │────→│ edgelink-pg-store│────→│ PostgreSQL/Timescale │
│ read (3E/4E) │     │                  │     │                      │
└──────────────┘     │  • auto-detect   │     │  • plc_data          │
                     │  • batch INSERT  │     │  • hypertable (opt)  │
┌──────────────┐     │  • retry on fail │     │                      │
│ modbus-read  │────→│  • ON CONFLICT   │     └──────────────────────┘
└──────────────┘     └──────────────────┘

Compatible with any upstream node that outputs the standard EdgeLink format.
```

---

## Status Indicator

| Color | Meaning | Example |
|-------|---------|---------|
| 🟢 Green | Insert succeeded | `inserted: 100` |
| 🟡 Yellow | Data buffered | `buffer: 15` |
| 🔴 Red | Write failed | `error: [CONNECT] ...` |
| ⚪ Grey | Node closed | `closed` |

---

## FAQ

**Q: Can I execute arbitrary SQL?**  
No. This is a data logger, not a SQL client. Use `node-red-contrib-postgresql` for custom queries.

**Q: Does it support table auto-creation for generic formats?**  
No. Auto-create is MC format only (fixed schema). For generic formats, create the table manually.

**Q: What happens if data arrives while a flush is in progress?**  
Data is appended to the buffer. The next flush trigger (batch size, timer, or `msg.flush`) will pick it up. No data is lost.

**Q: How is the connection pool shared across nodes?**  
A module-level `POOLS` map, keyed by `user@host:port/database`. Reference-counted. Last node standing calls `pool.end()`.

**Q: Why no exponential backoff for retries?**  
Edge scenarios: PG either recovers quickly (network hiccup) or stays dead (needs human). Fixed 5s retry is sufficient; backoff adds state without benefit.

---

## License

MIT — see [LICENSE](LICENSE) for full text.
