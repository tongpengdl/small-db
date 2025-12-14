package smalldb

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// resolveActiveVersion chooses the active version for a DB directory.
//
// We prefer `newVersion` when present, otherwise fall back to `version`.
// This matches the paper's "atomic version switch" approach: during a
// checkpoint switch, a crash can leave intermediate markers behind; recovery
// must still find a well-defined (checkpoint.N, logfile.N) pair.
//
// Today we don't create `newVersion` yet, but keeping this logic now makes
// version switching a drop-in later.
func resolveActiveVersion(dir string) (uint64, error) {
	if v, ok, err := readVersionIfExists(newVersionPath(dir)); err != nil {
		return 0, err
	} else if ok {
		return v, nil
	}

	if v, ok, err := readVersionIfExists(versionPath(dir)); err != nil {
		return 0, err
	} else if ok {
		return v, nil
	}

	return 0, nil
}

// writeVersionFileIfMissing creates the `version` file if it doesn't exist.
//
// This gives the directory a stable anchor for "current version" from the very
// first open, so recovery can always start by reading `version`.
func writeVersionFileIfMissing(dir string, version uint64) error {
	path := versionPath(dir)
	_, err := os.Stat(path)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat version file: %w", err)
	}
	return os.WriteFile(path, []byte(strconv.FormatUint(version, 10)+"\n"), 0o644)
}

// readVersionIfExists reads a uint64 version number from a small text file.
//
// The file is expected to contain a decimal uint64 (with optional trailing
// newline). If the file doesn't exist, (0, false, nil) is returned.
func readVersionIfExists(path string) (version uint64, ok bool, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read version file: %w", err)
	}

	s := strings.TrimSpace(string(data))
	if s == "" {
		return 0, false, fmt.Errorf("invalid version file (empty): %s", path)
	}

	version, err = strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("invalid version file %s: %w", path, err)
	}
	return version, true, nil
}
