package caddystorage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/psviderski/uncloud/pkg/client"
	"github.com/psviderski/uncloud/pkg/distlock"
)

const lockResourcePrefix = "caddy_storage:"

// Lock acquires an automatically renewed distributed lock and waits for the local store to catch up with versions
// observed on responding machines. Writers using the same lock can then read locally. Unavailable machines may have
// writes that this wait does not cover, and reads outside a lock remain eventually consistent.
func (s *Storage) Lock(ctx context.Context, name string) (err error) {
	if name == "" {
		return errors.New("lock name is empty")
	}
	s.locksMu.Lock()
	if s.locks == nil {
		s.locksMu.Unlock()
		return errors.New("storage is closed")
	}
	s.locksMu.Unlock()

	// Stop acquisition retries when Caddy unloads the module, even if the caller's context is still active.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stopOnCleanup := context.AfterFunc(s.ctx, func() {
		cancel(context.Cause(s.ctx))
	})
	defer stopOnCleanup()

	log := s.log.With("lock", name)
	started := time.Now()
	stage := "acquire_lease"
	log.Debug("acquiring lock", "lock_ttl", time.Duration(s.LockTTL))
	defer func() {
		if err != nil {
			log.Debug("failed to acquire lock",
				"stage", stage, "duration", time.Since(started), "error", err)
		}
	}()

	lease, err := s.locker.Acquire(ctx, lockResourcePrefix+name)
	if err != nil {
		return fmt.Errorf("acquire lock '%s': %w", name, err)
	}
	log.Debug("lock lease acquired", "duration", time.Since(started))
	// Keep observing after Lock returns so lease loss during protected work remains visible.
	context.AfterFunc(lease.Context(), func() {
		if cause := context.Cause(lease.Context()); errors.Is(cause, distlock.ErrLeaseLost) {
			log.Error("lock lease lost", "error", cause)
		}
	})

	defer func() {
		if err == nil {
			return
		}
		// Capture lease loss before Release cancels the lease context itself.
		err = errors.Join(err, context.Cause(lease.Context()))
		err = fmt.Errorf("acquire lock '%s': %w", name,
			errors.Join(err, s.releaseLock(ctx, name, lease, "failed acquisition")))
	}()

	stopOnLeaseLoss := context.AfterFunc(lease.Context(), func() {
		cancel(context.Cause(lease.Context()))
	})
	defer stopOnLeaseLoss()

	stage = "collect_store_versions"
	version, machines, err := s.clusterStoreVersion(ctx, log)
	if err != nil {
		return err
	}
	stage = "wait_for_replication"
	waitStarted := time.Now()
	log.Debug("waiting for local store replication", "machine_names", machines, "store_version", version)
	if err := s.client.WaitForStoreVersion(ctx, version); err != nil {
		return fmt.Errorf("wait for local store replication: %w", err)
	}
	log.Debug("local store replication complete", "duration", time.Since(waitStarted))

	stage = "register_lock"
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := context.Cause(lease.Context()); err != nil {
		return err
	}
	if s.locks == nil {
		// A lease obtained during cleanup must be released instead of reopening the module's lock map.
		return errors.New("storage is closed")
	}
	if _, exists := s.locks[name]; exists {
		return errors.New("lock is already held by this storage instance")
	}
	s.locks[name] = lease

	log.Debug("lock acquired", "duration", time.Since(started))
	return nil
}

// clusterStoreVersion returns the per-actor maximum store versions from responding machines and their names.
func (s *Storage) clusterStoreVersion(ctx context.Context, log *slog.Logger) (map[string]uint64, []string, error) {
	ctx, cancel := context.WithTimeout(ctx, distlock.DefaultMaxNodeCallTimeout)
	defer cancel()
	resp, err := s.client.MachineClient.InspectMachine(client.ProxyMachinesContext(ctx, nil), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect machines for store versions: %w", err)
	}

	maxVersion := make(map[string]uint64)
	machines := make([]string, 0, len(resp.Machines))
	for _, m := range resp.Machines {
		if m.Metadata.Error != "" {
			log.Debug("skipping machine when collecting store versions",
				"id", m.Metadata.MachineId, "name", m.Metadata.MachineName, "error", m.Metadata.Error)
			continue
		}
		machines = append(machines, m.Metadata.MachineName)
		for actor, v := range m.StoreVersion {
			maxVersion[actor] = max(maxVersion[actor], v)
		}
	}
	slices.Sort(machines)
	return maxVersion, machines, nil
}

// Unlock releases a previously acquired distributed lock.
func (s *Storage) Unlock(ctx context.Context, name string) error {
	s.locksMu.Lock()
	lease, exists := s.locks[name]
	if exists {
		delete(s.locks, name)
	}
	s.locksMu.Unlock()
	if !exists {
		return fmt.Errorf("lock '%s' is not held by this storage instance", name)
	}
	// Release stops renewal even on error. Any nodes that cannot be reached will let the lease expire.
	if err := s.releaseLock(ctx, name, lease, "unlock"); err != nil {
		return fmt.Errorf("release lock '%s': %w", name, err)
	}

	return nil
}

// releaseLock logs releases consistently across unlock, failed acquisition, and module cleanup.
func (s *Storage) releaseLock(ctx context.Context, name string, lease *distlock.Lease, reason string) error {
	// Unlock and rollback must attempt node cleanup even if the caller or module has already been cancelled.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), distlock.DefaultMaxNodeCallTimeout)
	defer cancel()

	log := s.log.With("lock", name, "reason", reason)
	started := time.Now()
	log.Debug("releasing lock")
	if err := lease.Release(ctx); err != nil {
		log.Debug("failed to release lock", "duration", time.Since(started), "error", err)
		return err
	}
	log.Debug("lock released", "duration", time.Since(started))
	return nil
}
