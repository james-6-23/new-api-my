package perfmetrics

import "testing"

func TestStatusColor(t *testing.T) {
	if got := statusColor(100, 0); got != StatusEmpty {
		t.Fatalf("empty: got %s", got)
	}
	if got := statusColor(99, 10); got != StatusGreen {
		t.Fatalf("green: got %s", got)
	}
	if got := statusColor(90, 10); got != StatusYellow {
		t.Fatalf("yellow: got %s", got)
	}
	if got := statusColor(50, 10); got != StatusRed {
		t.Fatalf("red: got %s", got)
	}
}
