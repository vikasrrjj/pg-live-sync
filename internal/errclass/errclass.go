// Package errclass separates errors that will never succeed no matter how often
// they are replayed (bad data, unsupported shape) from errors that are worth
// retrying (Kafka or PostgreSQL temporarily unavailable).
package errclass

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// permanent wraps an error that replaying the same event cannot fix.
type permanent struct{ err error }

func (p *permanent) Error() string { return p.err.Error() }
func (p *permanent) Unwrap() error { return p.err }

// Permanent marks err as a poison event: the writer must quarantine it rather
// than retry it forever and block every later change.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanent{err: err}
}

// IsPermanent reports whether err (or anything it wraps) was marked poison.
func IsPermanent(err error) bool {
	var p *permanent
	return errors.As(err, &p)
}

// Permanentf is Permanent with a formatted message.
func Permanentf(format string, args ...any) error {
	return Permanent(fmt.Errorf(format, args...))
}

// PostgresPermanent classifies a destination database error. A *pgconn.PgError
// is a statement-level failure (constraint, type, syntax) that repeating the
// same statement will not fix. Anything else (connection reset, context
// cancellation) is treated as transient so the writer retries the transaction.
func PostgresPermanent(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr)
}
