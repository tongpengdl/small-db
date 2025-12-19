package smalldb

import (
	"time"
)

type checkpointPolicy struct {
	enabled      bool
	interval     time.Duration
	maxLogBytes  int64
	maxUpdates   uint64
	checkEvery   time.Duration
	minRetryBack time.Duration
}

func policyFromOptions(opts Options) checkpointPolicy {
	if opts.DisableBackgroundCheckpoint {
		return checkpointPolicy{}
	}

	// Defaults based on the "good starting policy" in the milestone:
	// checkpoint when log > 64MB or every 30–60 seconds.
	interval := opts.CheckpointInterval
	if interval == 0 {
		interval = 60 * time.Second
	}
	maxLogBytes := opts.CheckpointLogBytes
	if maxLogBytes == 0 {
		maxLogBytes = 64 << 20
	}

	p := checkpointPolicy{
		enabled:      true,
		interval:     interval,
		maxLogBytes:  maxLogBytes,
		maxUpdates:   opts.CheckpointUpdates,
		checkEvery:   1 * time.Second,
		minRetryBack: 2 * time.Second,
	}
	if p.interval > 0 && p.interval < p.checkEvery {
		p.checkEvery = p.interval
	}
	if p.maxLogBytes <= 0 && p.maxUpdates == 0 && p.interval <= 0 {
		return checkpointPolicy{}
	}
	return p
}
