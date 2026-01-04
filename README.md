# small-db
A simple, durable, and efficient in-memory key/value store written in Go.

## Features

- **In-Memory Speed**: All data is held in memory for low-latency reads.
- **Durability**: All writes are appended to a Write-Ahead Log (WAL) and fsync'd before acknowledgment.
- **Group Commit**: Concurrent writes are batched into a single disk sync to improve throughput.
- **Checkpointing**: Supports background and manual checkpointing to compact the WAL and speed up recovery.
- **Crash Recovery**: Automatically recovers state by loading the latest checkpoint and replaying the WAL.
- **Concurrency**: Thread-safe with support for concurrent readers and a serialized writer.

## Build (Go)

- Requirements: Go 1.22+.
- Run example: `go run ./cmd/small_db_example`
- Build example: `go build ./cmd/small_db_example`
- Run server: `go run ./cmd/small_db_server -dir ./data -create -addr :8080`
- Build server: `go build ./cmd/small_db_server`
- Run tests: `go test ./...`
- Run benchmarks: `go test -bench=. ./...`

## Data Directory Layout

All persistent state lives under `Options.Dir`:

- `version`: active generation number (e.g. `0`)
- `newVersion`: temporary generation marker used during checkpoint switching (reserved for later)
- `checkpoint.N`: full snapshot for generation `N`
- `logfile.N`: WAL records after `checkpoint.N`

## HTTP+JSON Server

The server is a single-leader process that exposes the DB over HTTP+JSON.

Start it:

```
go run ./cmd/small_db_server -dir ./data -create -addr :8080
```

### API

- `GET /v1/health` -> `{ "ok": true }`
- `GET /v1/kv/{key}` -> `{ "key": "...", "value": "...", "encoding": "base64" }` (404 if missing)
- `PUT /v1/kv/{key}` with `{ "value": "...", "encoding": "base64|utf-8" }` -> `{ "ok": true }`
- `DELETE /v1/kv/{key}` -> `{ "ok": true }`
- `POST /v1/checkpoint` -> `{ "ok": true }`

Keys are URL path segments (URL-encode if you need special characters). If
`encoding` is omitted on writes, the server assumes `base64`.

### Primary/Backup Replication (experimental)

Backup mode connects to a primary and replays WAL records. Writes are rejected
on the backup (read-only). Background checkpointing is disabled automatically
for backups.

Basic flow:

- Start a primary normally.
- Copy the primary data directory to the backup before starting it.
- Start a backup with `-backup-of host:port` and a separate `-dir`.

### Examples

```
curl -X PUT http://localhost:8080/v1/kv/hello \
  -H 'Content-Type: application/json' \
  -d '{"value":"d29ybGQ=","encoding":"base64"}'

curl http://localhost:8080/v1/kv/hello

curl -X DELETE http://localhost:8080/v1/kv/hello
```
