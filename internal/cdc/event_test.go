package cdc

import "testing"

func TestKafkaKeyIsDeterministic(t *testing.T) {
	one := "1"
	two := "2"
	event := Event{
		Schema: "public",
		Table:  "users",
		Key: Row{
			"z": &two,
			"a": &one,
		},
	}

	got := string(event.KafkaKey())
	want := "public.users|a=1|z=2"
	if got != want {
		t.Fatalf("KafkaKey() = %q, want %q", got, want)
	}
}
