package common

import "testing"

func TestConversationCaptureUnbounded(t *testing.T) {
	capture := NewConversationCapture(0)
	capture.SetClientRequestBody(make([]byte, 1<<20))
	capture.AppendUpstreamResponseBody(make([]byte, 2<<20))
	snap := capture.Snapshot()
	if len(snap.ClientRequestBody) != 1<<20 || len(snap.UpstreamResponseBodyRaw) != 2<<20 {
		t.Fatalf("unbounded capture must retain all bytes, got req=%d resp=%d",
			len(snap.ClientRequestBody), len(snap.UpstreamResponseBodyRaw))
	}
	if snap.Truncated {
		t.Fatal("unbounded capture must not be flagged truncated")
	}
}

func TestConversationCaptureCapAcrossFields(t *testing.T) {
	// Cap is the COMBINED budget across every captured field.
	capture := NewConversationCapture(1000)
	capture.SetClientRequestBody(make([]byte, 600))       // 600 retained, 400 budget left
	capture.AppendUpstreamResponseBody(make([]byte, 300)) // 300 retained, 100 budget left
	capture.AppendUpstreamResponseBody(make([]byte, 300)) // only 100 retained, rest dropped

	snap := capture.Snapshot()
	total := len(snap.ClientRequestBody) + len(snap.UpstreamResponseBodyRaw)
	if total != 1000 {
		t.Fatalf("combined retained bytes must equal the cap, got %d", total)
	}
	if len(snap.UpstreamResponseBodyRaw) != 400 {
		t.Fatalf("response buffer must stop at the remaining budget, got %d", len(snap.UpstreamResponseBodyRaw))
	}
	if !snap.Truncated {
		t.Fatal("dropping bytes must flag the capture truncated")
	}
}

func TestConversationCaptureExactCapNotTruncated(t *testing.T) {
	capture := NewConversationCapture(500)
	capture.SetClientRequestBody(make([]byte, 500))
	snap := capture.Snapshot()
	if snap.Truncated {
		t.Fatal("filling the budget exactly must not flag truncated")
	}
	if len(snap.ClientRequestBody) != 500 {
		t.Fatalf("expected 500 retained bytes, got %d", len(snap.ClientRequestBody))
	}
	// One more byte beyond the budget must be dropped and flagged.
	capture.AppendUpstreamResponseBody([]byte{1})
	if snap = capture.Snapshot(); !snap.Truncated || len(snap.UpstreamResponseBodyRaw) != 0 {
		t.Fatalf("over-budget append must be dropped and flagged, truncated=%v len=%d",
			snap.Truncated, len(snap.UpstreamResponseBodyRaw))
	}
}

func TestConversationCaptureSetBytesReclaimsBudget(t *testing.T) {
	// Re-setting a field must release the previous field's bytes back to the
	// shared budget rather than leaking them.
	capture := NewConversationCapture(1000)
	capture.SetClientRequestBody(make([]byte, 800))
	capture.SetClientRequestBody(make([]byte, 300)) // replaces, not adds
	capture.AppendUpstreamResponseBody(make([]byte, 700))
	snap := capture.Snapshot()
	if len(snap.ClientRequestBody) != 300 || len(snap.UpstreamResponseBodyRaw) != 700 {
		t.Fatalf("set must reclaim prior budget, got req=%d resp=%d",
			len(snap.ClientRequestBody), len(snap.UpstreamResponseBodyRaw))
	}
	if snap.Truncated {
		t.Fatal("within reclaimed budget must not be flagged truncated")
	}
}

func TestConversationCaptureGlobalBudget(t *testing.T) {
	// Global budget bounds the sum across concurrent captures; release returns it.
	t.Cleanup(func() { SetGlobalCaptureMaxBytes(0); globalCaptureBytes.Store(0) })
	SetGlobalCaptureMaxBytes(0)
	globalCaptureBytes.Store(0)
	SetGlobalCaptureMaxBytes(1000)

	// Two captures, each per-request cap 800, but global cap is 1000.
	c1 := NewConversationCapture(800)
	c2 := NewConversationCapture(800)
	c1.SetClientRequestBody(make([]byte, 800)) // takes 800 of the 1000 global
	c2.SetClientRequestBody(make([]byte, 800)) // only 200 left globally

	s1, s2 := c1.Snapshot(), c2.Snapshot()
	if len(s1.ClientRequestBody) != 800 {
		t.Fatalf("c1 should retain 800, got %d", len(s1.ClientRequestBody))
	}
	if len(s2.ClientRequestBody) != 200 {
		t.Fatalf("c2 should be globally capped at 200, got %d", len(s2.ClientRequestBody))
	}
	if !s2.Truncated {
		t.Fatal("c2 hitting the global cap must be flagged truncated")
	}
	if got := globalCaptureBytes.Load(); got != 1000 {
		t.Fatalf("global counter should be 1000, got %d", got)
	}

	// Releasing c1 frees its 800 back to the global budget.
	c1.Release()
	if got := globalCaptureBytes.Load(); got != 200 {
		t.Fatalf("after releasing c1, global should be 200, got %d", got)
	}
	// A new capture can now retain again.
	c3 := NewConversationCapture(800)
	c3.AppendUpstreamResponseBody(make([]byte, 800)) // 800 free now (1000-200)
	if got := globalCaptureBytes.Load(); got != 1000 {
		t.Fatalf("c3 should reserve up to the global cap, got %d", got)
	}
}
