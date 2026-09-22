package errclass

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestPermanent(t *testing.T) {
	if got := Permanent(nil); got != nil {
		t.Fatalf("Permanent(nil) = %v, want nil", got)
	}
	base := errors.New("bad row")
	err := Permanent(base)
	if !IsPermanent(err) {
		t.Fatal("Permanent error should be classified permanent")
	}
	if err.Error() != "bad row" {
		t.Fatalf("Error() = %q, want %q", err.Error(), "bad row")
	}
	if !errors.Is(err, base) {
		t.Fatal("errors.Is should unwrap to the base error")
	}
}

func TestIsPermanentWrapped(t *testing.T) {
	err := fmt.Errorf("apply failed: %w", Permanent(errors.New("poison event")))
	if !IsPermanent(err) {
		t.Fatal("a permanent error wrapped in another error should stay permanent")
	}
	if IsPermanent(errors.New("plain transient error")) {
		t.Fatal("plain error must not be permanent")
	}
	if IsPermanent(nil) {
		t.Fatal("nil must not be permanent")
	}
}

func TestPermanentf(t *testing.T) {
	err := Permanentf("row %d invalid", 7)
	if !IsPermanent(err) {
		t.Fatal("Permanentf should produce a permanent error")
	}
	if err.Error() != "row 7 invalid" {
		t.Fatalf("Error() = %q, want %q", err.Error(), "row 7 invalid")
	}
}

func TestPostgresPermanent(t *testing.T) {
	if !PostgresPermanent(&pgconn.PgError{Code: "23505"}) {
		t.Fatal("pgconn.PgError should be permanent")
	}
	if PostgresPermanent(errors.New("connection refused")) {
		t.Fatal("plain error should be transient")
	}
	if !PostgresPermanent(fmt.Errorf("apply: %w", &pgconn.PgError{Code: "42883"})) {
		t.Fatal("a wrapped pgconn.PgError should still be permanent")
	}
	if PostgresPermanent(context.Canceled) {
		t.Fatal("context cancellation should be transient")
	}
	if PostgresPermanent(nil) {
		t.Fatal("nil must not be permanent")
	}
}