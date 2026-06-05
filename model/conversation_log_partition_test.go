package model

import "testing"

func TestPartitionHourStart(t *testing.T) {
	cases := []struct {
		ts   int64
		want int64
	}{
		{0, 0},
		{3599, 0},
		{3600, 3600},
		{3601, 3600},
		{1780668536, 1780668000}, // floors to the hour boundary
	}
	for _, c := range cases {
		if got := partitionHourStart(c.ts); got != c.want {
			t.Fatalf("partitionHourStart(%d) = %d, want %d", c.ts, got, c.want)
		}
	}
}

func TestPartitionNameRoundTrip(t *testing.T) {
	for _, hourStart := range []int64{0, 3600, 1780668000} {
		name := partitionNameForStart(hourStart)
		got, ok := partitionStartFromName(name)
		if !ok {
			t.Fatalf("partitionStartFromName(%q) failed to parse", name)
		}
		if got != hourStart {
			t.Fatalf("round trip for %d: name=%q parsed=%d", hourStart, name, got)
		}
	}
}

func TestPartitionStartFromNameRejectsForeign(t *testing.T) {
	for _, name := range []string{"conversation_logs", "other_p_123", "conversation_logs_p_abc", ""} {
		if _, ok := partitionStartFromName(name); ok {
			t.Fatalf("partitionStartFromName(%q) should not parse", name)
		}
	}
}
