# small-db
A simple and efficient implementation for small databases

## Build (Go)

- Requirements: Go 1.22+.
- Run example: `go run ./cmd/small_db_example`
- Build example: `go build ./cmd/small_db_example`
- Run server: `go run ./cmd/small_db_server -dir ./data -create -addr :8080`
- Build server: `go build ./cmd/small_db_server`
- Run tests: `go test ./...`

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

### Examples

```
curl -X PUT http://localhost:8080/v1/kv/hello \
  -H 'Content-Type: application/json' \
  -d '{"value":"d29ybGQ=","encoding":"base64"}'

curl http://localhost:8080/v1/kv/hello

curl -X DELETE http://localhost:8080/v1/kv/hello
```
