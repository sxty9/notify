package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestEmitListAndUnread(t *testing.T) {
	st := openTest(t)
	n, created, err := st.Emit(Notification{User: "alice", Service: "icaly", Title: "Hi", Level: "info"})
	if err != nil || !created {
		t.Fatalf("emit: created=%v err=%v", created, err)
	}
	if n.Seq == 0 || n.ID == "" {
		t.Fatalf("expected an assigned seq+id, got seq=%d id=%q", n.Seq, n.ID)
	}
	if n.Created.IsZero() {
		t.Fatalf("Created should default to now when unset")
	}
	// A second user's notification must never leak into alice's view.
	if _, _, err := st.Emit(Notification{User: "bob", Title: "Bob only", Level: "info"}); err != nil {
		t.Fatalf("emit bob: %v", err)
	}
	list, unread, err := st.List("alice", 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].Title != "Hi" {
		t.Fatalf("expected alice's single notification, got %+v", list)
	}
	if unread != 1 {
		t.Fatalf("expected unread=1, got %d", unread)
	}
}

func TestEmitDedupeAndOnChange(t *testing.T) {
	st := openTest(t)
	var mu sync.Mutex
	var seen []int64
	st.OnChange(func(n Notification) {
		mu.Lock()
		seen = append(seen, n.Seq)
		mu.Unlock()
	})

	first, created, err := st.Emit(Notification{User: "alice", Title: "Reminder", Level: "info", Dedupe: "evt-1"})
	if err != nil || !created {
		t.Fatalf("first emit: created=%v err=%v", created, err)
	}
	// Same (user, dedupe) → no new row, created=false, no fan-out.
	again, created2, err := st.Emit(Notification{User: "alice", Title: "Reminder (dup)", Level: "info", Dedupe: "evt-1"})
	if err != nil {
		t.Fatalf("second emit: %v", err)
	}
	if created2 {
		t.Fatalf("expected the dedupe emit to be a no-op (created=false)")
	}
	if again.Seq != first.Seq {
		t.Fatalf("dedupe should return the existing seq %d, got %d", first.Seq, again.Seq)
	}
	// The same dedupe key under a different user IS a distinct notification.
	if _, created3, err := st.Emit(Notification{User: "bob", Title: "Reminder", Level: "info", Dedupe: "evt-1"}); err != nil || !created3 {
		t.Fatalf("bob emit: created=%v err=%v", created3, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("expected exactly 2 fan-outs (alice once, bob once), got %d: %v", len(seen), seen)
	}
}

func TestMarkReadScopedAndPartial(t *testing.T) {
	st := openTest(t)
	a, _, _ := st.Emit(Notification{User: "alice", Title: "one", Level: "info"})
	if _, _, err := st.Emit(Notification{User: "alice", Title: "two", Level: "info"}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if _, _, err := st.Emit(Notification{User: "bob", Title: "b", Level: "info"}); err != nil {
		t.Fatalf("emit: %v", err)
	}

	// Marking under the wrong user must not touch alice's row.
	if err := st.MarkRead("bob", []string{a.ID}); err != nil {
		t.Fatalf("markread: %v", err)
	}
	if _, unread, _ := st.List("alice", 0); unread != 2 {
		t.Fatalf("cross-user markRead leaked; alice unread=%d, want 2", unread)
	}
	// The correct user marks exactly the one id.
	if err := st.MarkRead("alice", []string{a.ID}); err != nil {
		t.Fatalf("markread: %v", err)
	}
	if _, unread, _ := st.List("alice", 0); unread != 1 {
		t.Fatalf("alice unread=%d, want 1", unread)
	}
	// bob remains untouched.
	if _, unread, _ := st.List("bob", 0); unread != 1 {
		t.Fatalf("bob unread=%d, want 1", unread)
	}
}

func TestMarkAllRead(t *testing.T) {
	st := openTest(t)
	for i := 0; i < 3; i++ {
		if _, _, err := st.Emit(Notification{User: "alice", Title: "x", Level: "info"}); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}
	if err := st.MarkAllRead("alice"); err != nil {
		t.Fatalf("mark all read: %v", err)
	}
	if _, unread, _ := st.List("alice", 0); unread != 0 {
		t.Fatalf("expected unread 0 after MarkAllRead, got %d", unread)
	}
}

func TestMaxSeqAndChangesSince(t *testing.T) {
	st := openTest(t)
	if seq, err := st.MaxSeq("alice"); err != nil || seq != 0 {
		t.Fatalf("empty MaxSeq=%d err=%v, want 0", seq, err)
	}
	var seqs []int64
	for i := 0; i < 3; i++ {
		n, _, err := st.Emit(Notification{User: "alice", Title: fmt.Sprintf("n%d", i), Level: "info"})
		if err != nil {
			t.Fatalf("emit: %v", err)
		}
		seqs = append(seqs, n.Seq)
	}
	if seq, _ := st.MaxSeq("alice"); seq != seqs[2] {
		t.Fatalf("MaxSeq=%d, want %d", seq, seqs[2])
	}
	// ChangesSince returns everything strictly after the cursor, oldest first — the SSE replay.
	missed, err := st.ChangesSince("alice", seqs[0])
	if err != nil {
		t.Fatalf("changes: %v", err)
	}
	if len(missed) != 2 || missed[0].Seq != seqs[1] || missed[1].Seq != seqs[2] {
		t.Fatalf("ChangesSince(seq0) should return seq1,seq2 oldest-first; got %+v", missed)
	}
}

func TestListLimitDefaultsAndClamps(t *testing.T) {
	st := openTest(t)
	for i := 0; i < 60; i++ {
		if _, _, err := st.Emit(Notification{User: "alice", Title: "x", Level: "info"}); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}
	if list, _, _ := st.List("alice", 0); len(list) != 50 {
		t.Fatalf("limit<=0 should default to 50, got %d", len(list))
	}
	if list, _, _ := st.List("alice", 9999); len(list) != 50 {
		t.Fatalf("oversized limit should clamp to 50, got %d", len(list))
	}
	if list, _, _ := st.List("alice", 10); len(list) != 10 {
		t.Fatalf("explicit limit=10 should be honoured, got %d", len(list))
	}
}

func TestCompactKeepsRecentFloor(t *testing.T) {
	st := openTest(t)
	old := time.Now().AddDate(0, 0, -60)
	for i := 0; i < 105; i++ {
		if _, _, err := st.Emit(Notification{User: "alice", Title: fmt.Sprintf("n%d", i), Level: "info", Created: old}); err != nil {
			t.Fatalf("emit %d: %v", i, err)
		}
	}
	if err := st.MarkAllRead("alice"); err != nil {
		t.Fatalf("mark all read: %v", err)
	}
	// Cutoff 30 days ago: all 105 rows are older AND read, but the most-recent-100 are
	// floor-protected, so only the oldest 5 are trimmed.
	if err := st.Compact(time.Now().AddDate(0, 0, -30)); err != nil {
		t.Fatalf("compact: %v", err)
	}
	all, err := st.ChangesSince("alice", 0)
	if err != nil {
		t.Fatalf("changes: %v", err)
	}
	if len(all) != 100 {
		t.Fatalf("expected the 100-row floor to survive, got %d", len(all))
	}
	if all[0].Title == "n0" {
		t.Errorf("the oldest notification should have been trimmed, but n0 survived")
	}
}

func TestCompactSparesUnread(t *testing.T) {
	st := openTest(t)
	old := time.Now().AddDate(0, 0, -60)
	for i := 0; i < 105; i++ {
		if _, _, err := st.Emit(Notification{User: "carol", Title: "u", Level: "info", Created: old}); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}
	// Nothing marked read → Compact must delete nothing, even though every row is old and
	// beyond the 100-row floor. Only read notifications are ever trimmed.
	if err := st.Compact(time.Now().AddDate(0, 0, -30)); err != nil {
		t.Fatalf("compact: %v", err)
	}
	all, _ := st.ChangesSince("carol", 0)
	if len(all) != 105 {
		t.Fatalf("unread notifications must never be trimmed; expected 105, got %d", len(all))
	}
}
