package smalldb

import (
	"fmt"
	"path/filepath"
)

const (
	versionFileName    = "version"
	newVersionFileName = "newVersion"
)

func versionPath(dir string) string {
	return filepath.Join(dir, versionFileName)
}

func newVersionPath(dir string) string {
	return filepath.Join(dir, newVersionFileName)
}

func checkpointFileName(version uint64) string {
	return fmt.Sprintf("checkpoint.%d", version)
}

func checkpointPath(dir string, version uint64) string {
	return filepath.Join(dir, checkpointFileName(version))
}

func logFileName(version uint64) string {
	return fmt.Sprintf("logfile.%d", version)
}

func logPath(dir string, version uint64) string {
	return filepath.Join(dir, logFileName(version))
}
