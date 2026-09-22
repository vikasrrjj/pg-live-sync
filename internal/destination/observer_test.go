package destination

import "testing"

func TestKafkaLagForDemo(t *testing.T) {
	cases := []struct {
		produced, consumed, want int64
	}{
		{10, 4, 6},
		{10, 10, 0},
		{10, 11, -1},
		{-1, 4, -1},
		{10, -1, -1},
	}
	for _, tc := range cases {
		if got := KafkaLagForDemo(tc.produced, tc.consumed); got != tc.want {
			t.Fatalf("KafkaLagForDemo(%d, %d) = %d, want %d", tc.produced, tc.consumed, got, tc.want)
		}
	}
}

func TestRetentionOutranConsumption(t *testing.T) {
	cases := []struct {
		committed, start int64
		want             bool
	}{
		{3, 5, true},
		{5, 5, false},
		{6, 5, false},
		{-1, 5, false},
		{0, 0, false},
	}
	for _, tc := range cases {
		if got := RetentionOutranConsumption(tc.committed, tc.start); got != tc.want {
			t.Fatalf("RetentionOutranConsumption(%d, %d) = %v, want %v", tc.committed, tc.start, got, tc.want)
		}
	}
}
