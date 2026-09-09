// Package caddystorage provides Caddy storage backed by an Uncloud cluster.
package caddystorage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"github.com/psviderski/uncloud/pkg/client"
	"github.com/psviderski/uncloud/pkg/client/connector"
	"github.com/psviderski/uncloud/pkg/distlock"
)

const (
	// ModuleID is the Caddy module ID for Uncloud storage.
	ModuleID = "caddy.storage.uncloud"
	// DefaultSocketPath is the default path to the Uncloud API socket.
	DefaultSocketPath = "/run/uncloud/uncloud.sock"
	// DefaultLockTTL is the default duration of a distributed lock lease.
	DefaultLockTTL = 20 * time.Second
)

func init() {
	caddy.RegisterModule(new(Storage))
}

// Storage implements a Caddy storage backend that uses an Uncloud cluster to store assets such as TLS certificates.
type Storage struct {
	// Socket is the path to the Uncloud API socket.
	// Defaults to /run/uncloud/uncloud.sock when not set.
	Socket string `json:"socket,omitempty"`
	// LockTTL is the duration of a distributed lock after which it expires if not renewed. Locks renew automatically
	// until unlocked. If an instance crashes or cannot renew, expiry allows another instance to acquire the stale lock.
	// Longer durations tolerate longer interruptions but delay recovery after a crash. Normal unlocks release the lock
	// immediately.
	// Defaults to 20 seconds when not set.
	LockTTL caddy.Duration `json:"lock_ttl,omitempty"`

	client *client.Client
	locker *distlock.Locker
	log    *slog.Logger

	locksMu sync.Mutex
	locks   map[string]*distlock.Lease
}

// CaddyModule returns the Caddy module information.
func (*Storage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  ModuleID,
		New: func() caddy.Module { return new(Storage) },
	}
}

// Provision connects the storage to the local Uncloud API and initialises the distributed locker.
func (s *Storage) Provision(ctx caddy.Context) error {
	s.log = ctx.Slogger()

	if s.Socket == "" {
		s.Socket = DefaultSocketPath
	}
	if s.LockTTL == 0 {
		s.LockTTL = caddy.Duration(DefaultLockTTL)
	}
	if s.LockTTL < 0 {
		return errors.New("lock_ttl must be positive")
	}

	cli, err := client.New(ctx, connector.NewUnixConnector(s.Socket))
	if err != nil {
		return fmt.Errorf("connect to Uncloud API: %w", err)
	}

	locker, err := cli.NewLocker(distlock.Config{
		LeaseDuration: time.Duration(s.LockTTL),
	})
	if err != nil {
		_ = cli.Close()
		return fmt.Errorf("create distributed locker: %w", err)
	}

	s.client = cli
	s.locker = locker
	s.locks = make(map[string]*distlock.Lease)

	s.log.Debug("module provisioned", "socket", s.Socket, "lock_ttl", time.Duration(s.LockTTL))
	return nil
}

// Cleanup releases active locks and closes the Uncloud API connection.
func (s *Storage) Cleanup() (err error) {
	s.locksMu.Lock()
	locks := s.locks
	s.locks = nil
	s.locksMu.Unlock()

	started := time.Now()
	s.log.Debug("cleaning up module", "locks", len(locks))
	defer func() {
		if err != nil {
			s.log.Debug("failed to clean up module", "duration", time.Since(started), "error", err)
		} else {
			s.log.Debug("module cleanup complete", "duration", time.Since(started))
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), distlock.DefaultMaxNodeCallTimeout)
	defer cancel()

	errCh := make(chan error, len(locks))
	var wg sync.WaitGroup
	for name, lease := range locks {
		wg.Go(func() {
			if err := s.releaseLock(ctx, name, lease, "cleanup"); err != nil {
				errCh <- fmt.Errorf("release lock '%s': %w", name, err)
			}
		})
	}
	wg.Wait()
	close(errCh)

	errs := make([]error, 0, len(errCh)+1)
	for err := range errCh {
		errs = append(errs, err)
	}

	if s.client != nil {
		errs = append(errs, s.client.Close())
		s.client = nil
		s.locker = nil
	}

	return errors.Join(errs...)
}

// CertMagicStorage returns the provisioned CertMagic storage implementation.
func (s *Storage) CertMagicStorage() (certmagic.Storage, error) {
	return s, nil
}

// UnmarshalCaddyfile configures Uncloud storage from the Caddyfile global storage block.
//
//	{
//	    storage uncloud {
//	        socket /run/uncloud/uncloud.sock
//	        lock_ttl 20s
//	    }
//	}
func (s *Storage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // Skip the module name 'uncloud'.
	// Reject inline arguments. NextArg leaves an opening brace for NextBlock.
	if d.NextArg() {
		return d.ArgErr()
	}

	// Read the optional options block, skipping its surrounding braces.
	for d.NextBlock(0) {
		switch d.Val() {
		case "socket":
			// Require a socket path on the same line as 'socket' option.
			if !d.NextArg() {
				return d.ArgErr()
			}
			s.Socket = d.Val()
			// Reject extra arguments after the socket path.
			if d.NextArg() {
				return d.ArgErr()
			}
		case "lock_ttl":
			if !d.NextArg() {
				return d.ArgErr()
			}
			ttl, err := caddy.ParseDuration(d.Val())
			if err != nil {
				return d.Errf("invalid lock_ttl '%s': %v", d.Val(), err)
			}
			if ttl <= 0 {
				return d.Err("lock_ttl must be positive")
			}
			if d.NextArg() {
				return d.ArgErr()
			}
			s.LockTTL = caddy.Duration(ttl)
		default:
			return d.Errf("unknown uncloud storage option: '%s'", d.Val())
		}
	}

	return nil
}

var (
	_ caddy.Module           = (*Storage)(nil)
	_ caddy.Provisioner      = (*Storage)(nil)
	_ caddy.CleanerUpper     = (*Storage)(nil)
	_ caddy.StorageConverter = (*Storage)(nil)
	_ caddyfile.Unmarshaler  = (*Storage)(nil)
	_ certmagic.Storage      = (*Storage)(nil)
)
