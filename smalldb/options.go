package smalldb

import "time"

type Options struct {
	Dir             string
	CreateIfMissing bool

	// DisableBackgroundCheckpoint disables the background checkpoint goroutine.
	DisableBackgroundCheckpoint bool

	// CheckpointInterval triggers a checkpoint when the last checkpoint is older
	// than this duration. If zero and background checkpointing is enabled, a
	// default is used.
	CheckpointInterval time.Duration

	// CheckpointLogBytes triggers a checkpoint when the current generation's WAL
	// file size reaches this many bytes. If zero and background checkpointing is
	// enabled, a default is used.
	CheckpointLogBytes int64

	// CheckpointUpdates triggers a checkpoint when this many updates have been
	// committed since the last checkpoint. If zero, this trigger is disabled.
	CheckpointUpdates uint64
}
