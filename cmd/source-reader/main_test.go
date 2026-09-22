package main

import "testing"

func TestFenceKeyDeterminism(t *testing.T) {
	first := fenceKey("live_demo_slot")
	for i := 0; i < 5; i++ {
		if got := fenceKey("live_demo_slot"); got != first {
			t.Fatalf("fenceKey must be deterministic, got %d then %d", first, got)
		}
	}
}

func TestFenceKeyDistinctSlots(t *testing.T) {
	seen := map[int64]bool{}
	for _, slot := range []string{"live_demo_slot", "live_demo_slot_b", "", "x"} {
		key := fenceKey(slot)
		if seen[key] {
			t.Fatalf("fenceKey collision for different slot names: %d", key)
		}
		seen[key] = true
	}
}
