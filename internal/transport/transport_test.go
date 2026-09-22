package transport

import "testing"

func TestPostgresTLSFromEnvDefaults(t *testing.T) {
	t.Setenv("PGSSLMODE", "")
	t.Setenv("PGSSLROOTCERT", "")
	t.Setenv("PGSSLCERT", "")
	t.Setenv("PGSSLKEY", "")
	cfg, mode, err := postgresTLSFromEnv("db.example.com")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if cfg != nil || mode != "" {
		t.Fatalf("got cfg=%v mode=%q, want nil config and empty mode", cfg, mode)
	}
}

func TestPostgresTLSFromEnvDisable(t *testing.T) {
	t.Setenv("PGSSLMODE", "disable")
	t.Setenv("PGSSLROOTCERT", "")
	t.Setenv("PGSSLCERT", "")
	t.Setenv("PGSSLKEY", "")
	cfg, mode, err := postgresTLSFromEnv("db.example.com")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if mode != "disable" {
		t.Fatalf("mode = %q, want disable", mode)
	}
	if cfg != nil {
		t.Fatalf("cfg = %v, want nil for sslmode=disable", cfg)
	}
}

func TestPostgresTLSFromEnvBadRootCert(t *testing.T) {
	t.Setenv("PGSSLMODE", "verify-ca")
	t.Setenv("PGSSLROOTCERT", "/nonexistent/root.pem")
	if _, _, err := postgresTLSFromEnv("db.example.com"); err == nil {
		t.Fatal("want error for missing root cert file")
	}
}

func TestKafkaTLSFromEnvDisabled(t *testing.T) {
	t.Setenv("KAFKA_TLS_ENABLED", "")
	t.Setenv("KAFKA_CA_CERT", "")
	t.Setenv("KAFKA_CLIENT_CERT", "")
	t.Setenv("KAFKA_CLIENT_KEY", "")
	cfg, err := kafkaTLSFromEnv()
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if cfg != nil {
		t.Fatalf("got non-nil config for disabled TLS: %+v", cfg)
	}
}

func TestKafkaTLSFromEnvMissingCA(t *testing.T) {
	t.Setenv("KAFKA_TLS_ENABLED", "true")
	t.Setenv("KAFKA_CA_CERT", "/nonexistent/ca.pem")
	if _, err := kafkaTLSFromEnv(); err == nil {
		t.Fatal("want error for missing CA file")
	}
}

func TestKafkaSASLFromEnv(t *testing.T) {
	t.Setenv("KAFKA_SASL_USERNAME", "")
	t.Setenv("KAFKA_SASL_PASSWORD", "")
	if got := kafkaSASLFromEnv(); got != nil {
		t.Fatalf("got %v, want nil when credentials are absent", got)
	}
	t.Setenv("KAFKA_SASL_USERNAME", "user")
	t.Setenv("KAFKA_SASL_PASSWORD", "pass")
	t.Setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
	if got := kafkaSASLFromEnv(); got == nil {
		t.Fatal("expected a SASL mechanism when credentials are set")
	}
}