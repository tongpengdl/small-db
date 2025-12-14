# small-db
A simple and efficient implementation for small databases

## Build (Go)

- Requirements: Go 1.22+.
- Run example: `go run ./cmd/small_db_example`
- Build binary: `go build ./cmd/small_db_example`
- Run tests: `go test ./...`

## Data Directory Layout

All persistent state lives under `Options.Dir`:

- `version`: active generation number (e.g. `0`)
- `newVersion`: temporary generation marker used during checkpoint switching (reserved for later)
- `checkpoint.N`: full snapshot for generation `N` (not implemented yet)
- `logfile.N`: WAL records after `checkpoint.N`
