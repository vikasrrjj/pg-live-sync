package main

import "testing"

func TestFenceKeyDeterminism(t *testing.T) {
	first := fenceKey("artie_demo_slot")
	for i := 0; i < 5; i++ {
		if got := fenceKey("artie_demo_slot"); got != first {
			t.Fatalf("fenceKey must be deterministic, got %d then %d", first, got)
		}
	}
}

func TestFenceKeyDistinctSlots(t *testing.T) {
	seen := map[int64]bool{}
	for _, slot := range []string{"artie_demo_slot", "artie_demo_slot_b", "", "x"} {
		key := fenceKey(slot)
		if seen[key] {
			t.Fatalf("fenceKey collision for different slot names: %d", key)
		}
		seen[key] = true
	}
}
