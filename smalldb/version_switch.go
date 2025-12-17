package smalldb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Milestone 5 — Version switch (paper-style)
//
// The paper’s core idea is a tiny "filesystem transaction" to atomically
// publish a new (checkpoint, log) pair:
//
// When making a new checkpoint from generation N:
//   1) Write checkpoint.(N+1)   [temp file + fsync + rename]
//   2) Create empty logfile.(N+1)
//   3) Write newVersion containing (N+1) and fsync   <-- COMMIT POINT
//   4) Delete old checkpoint.N, logfile.N, version
//   5) Rename newVersion -> version
//
// On restart:
//   - If newVersion exists and looks valid, prefer it; otherwise use version.
//   - Clean up redundant files.
//   - Load checkpoint.N, replay logfile.N.

// writeNewVersionFile writes `newVersion` with the given generation and fsyncs it,
// making the new generation durable as the checkpoint-switch commit point.
//
// This is the commit point of the version switch, so it must be durable.
//
// Implementation note:
// We write to `newVersion.tmp` first, fsync it, then atomically rename to
// `newVersion` and fsync the directory. This avoids leaving behind a partially
// written-but-parseable `newVersion` file if we crash mid-write.
func writeNewVersionFile(dir string, generation uint64) error {
	finalPath := newVersionPath(dir)
	if v, ok, err := readVersionIfExists(finalPath); err != nil {
		return fmt.Errorf("read existing newVersion: %w", err)
	} else if ok {
		if v == generation {
			return nil
		}
		return fmt.Errorf("newVersion already exists with different generation: have=%d want=%d", v, generation)
	}

	tmpPath := newVersionTempPath(dir)
	_ = os.Remove(tmpPath)

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create newVersion temp file: %w", err)
	}

	if _, err := f.WriteString(strconv.FormatUint(generation, 10) + "\n"); err != nil {
		_ = f.Close()
		return fmt.Errorf("write newVersion temp file: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("fsync newVersion temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close newVersion temp file: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("rename newVersion temp -> newVersion: %w", err)
	}
	return syncDir(dir)
}

// finalizeVersionSwitch atomically makes newVersion become version.
func finalizeVersionSwitch(dir string) error {
	if err := os.Rename(newVersionPath(dir), versionPath(dir)); err != nil {
		return fmt.Errorf("rename newVersion -> version: %w", err)
	}
	return syncDir(dir)
}

// cleanupOldGenerationFiles removes files from the old generation.
func cleanupOldGenerationFiles(dir string, oldGen uint64) error {
	if err := os.Remove(checkpointPath(dir, oldGen)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove old checkpoint: %w", err)
	}
	if err := os.Remove(logPath(dir, oldGen)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove old logfile: %w", err)
	}
	if err := os.Remove(versionPath(dir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove old version: %w", err)
	}
	return nil
}

// recoverAndCleanupVersionSwitch is a startup helper you can call from Open().
//
// It removes any leftover `newVersion.tmp`, finalizes a committed-but-not-
// finalized switch by renaming `newVersion` -> `version`, and prunes redundant
// generation files so the directory converges back to a single active
// (checkpoint.N, logfile.N) pair.
//
// Returning the resolved generation keeps Open() logic simple:
//
//	gen, err := recoverAndCleanupVersionSwitch(opts.Dir)
func recoverAndCleanupVersionSwitch(dir string) (uint64, error) {
	// If we crashed mid-write of newVersion, the temp file might exist. It is
	// never a commit point, so it can be safely removed during recovery.
	if err := os.Remove(newVersionTempPath(dir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("remove newVersion temp: %w", err)
	}

	gen, err := resolveActiveVersion(dir)
	if err != nil {
		return 0, err
	}

	// If newVersion exists, it means we committed to a new version but might have
	// crashed before finalizing the rename. Complete the switch now.
	if _, err := os.Stat(newVersionPath(dir)); err == nil {
		if err := finalizeVersionSwitch(dir); err != nil {
			return 0, fmt.Errorf("finalize pending version switch: %w", err)
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("stat newVersion: %w", err)
	}

	// Cleanup redundant files from non-active generations.
	//
	// The paper's switch deletes old generation files during checkpointing, but a
	// crash can leave:
	// - older checkpoint/log files that were supposed to be deleted, or
	// - newer checkpoint/log files created before the commit point (newVersion).
	//
	// Since `gen` represents the active generation, we can remove checkpoint/log
	// files for other generations to get back to the canonical "one pair on disk"
	// layout: checkpoint.gen + logfile.gen.
	entries, err := listDir(dir)
	if err != nil {
		return 0, err
	}

	removedAny := false
	for _, e := range entries {
		name := e.Name()

		// Remove checkpoint temp files (they are never a commit point).
		if strings.HasPrefix(name, "checkpoint.") && strings.HasSuffix(name, ".tmp") {
			if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return 0, fmt.Errorf("remove checkpoint temp: %w", err)
			}
			removedAny = true
			continue
		}

		if v, ok := parseVersionFilename("checkpoint", name); ok && v != gen {
			if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return 0, fmt.Errorf("remove checkpoint: %w", err)
			}
			removedAny = true
			continue
		}
		if v, ok := parseVersionFilename("logfile", name); ok && v != gen {
			if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return 0, fmt.Errorf("remove logfile: %w", err)
			}
			removedAny = true
			continue
		}
	}
	if removedAny {
		if err := syncDir(dir); err != nil {
			return 0, err
		}
	}

	return gen, nil
}

func newVersionTempPath(dir string) string {
	return newVersionPath(dir) + ".tmp"
}

// createEmptyLogFile creates logfile.(generation) if it doesn't exist.
func createEmptyLogFile(dir string, generation uint64) error {
	path := logPath(dir, generation)
	w, err := openWAL(path)
	if err != nil {
		return err
	}
	return w.close()
}

// parseVersionFilename parses "checkpoint.N" or "logfile.N" suffixes.
// This is useful for cleanup code that scans the directory.
func parseVersionFilename(prefix, name string) (uint64, bool) {
	if len(name) <= len(prefix)+1 {
		return 0, false
	}
	if name[:len(prefix)] != prefix || name[len(prefix)] != '.' {
		return 0, false
	}
	n, err := strconv.ParseUint(name[len(prefix)+1:], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// listDir is a small helper for future cleanup code.
func listDir(dir string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("readdir: %w", err)
	}
	return entries, nil
}

// dirOf returns the directory containing path.
func dirOf(path string) string { return filepath.Dir(path) }
