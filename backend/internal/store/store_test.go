package store

import (
	"sync"
	"sync/atomic"
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

func emit(t *testing.T, st *Store, user, title string) Notification {
	t.Helper()
	n, created, err := st.Emit(Notification{User: user, Service: "test", Title: title, Level: "info"})
	if err != nil {
		t.Fatalf("emit %q: %v", title, err)
	}
	if !created {
		t.Fatalf("emit %q: expected a new row", title)
	}
	return n
}

func TestEmitAndList(t *testing.T) {
	st := openTest(t)
	emit(t, st, "alice", "first")
	emit(t, st, "alice", "second")
	emit(t, st, "bob", "not-yours")

	list, unread, err := st.List("alice", 50)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 of alice's notifications, got %d", len(list))
	}
	// Newest first (descending seq).
	if list[0].Title != "second" || list[1].Title != "first" {
		t.Fatalf("wrong order: %q, %q", list[0].Title, list[1].Title)
	}
	if unread != 2 {
		t.Fatalf("expected unread=2, got %d", unread)
	}
	// bob's row is never visible to alice — the pool routes strictly by user.
	for _, n := range list {
		if n.Title == "not-yours" {
			t.Fatal("cross-user leak: alice saw bob's notification")
		}
	}
}

func TestDedupeIsIdempotent(t *testing.T) {
	st := openTest(t)
	first, created, err := st.Emit(Notification{User: "alice", Service: "test", Title: "reminder", Level: "info", Dedupe: "tick-1"})
	if err != nil || !created {
		t.Fatalf("first emit: created=%v err=%v", created, err)
	}
	second, created, err := st.Emit(Notification{User: "alice", Service: "test", Title: "reminder again", Level: "info", Dedupe: "tick-1"})
	if err != nil {
		t.Fatalf("second emit: %v", err)
	}
	if created {
		t.Fatal("second emit with the same (user, dedupe) must be a no-op, not a new row")
	}
	if second.Seq != first.Seq {
		t.Fatalf("dedupe returned a different row: %d vs %d", second.Seq, first.Seq)
	}
	// A different user with the same dedupe key is independent.
	if _, created, _ := st.Emit(Notification{User: "bob", Service: "test", Title: "reminder", Level: "info", Dedupe: "tick-1"}); !created {
		t.Fatal("dedupe must be scoped per user")
	}
	_, unread, err := st.List("alice", 50)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if unread != 1 {
		t.Fatalf("expected exactly one row for alice, got unread=%d", unread)
	}
}

func TestMarkReadAndMarkAllRead(t *testing.T) {
	st := openTest(t)
	a := emit(t, st, "alice", "a")
	emit(t, st, "alice", "b")
	emit(t, st, "alice", "c")

	if err := st.MarkRead("alice", []string{a.ID}); err != nil {
		t.Fatalf("mark read: %v", err)
	}
	if _, unread, _ := st.List("alice", 50); unread != 2 {
		t.Fatalf("after marking one read, expected unread=2, got %d", unread)
	}
	// Marking another user's id is a no-op (scoped to the caller).
	if err := st.MarkRead("bob", []string{a.ID}); err != nil {
		t.Fatalf("mark read (bob): %v", err)
	}

	if err := st.MarkAllRead("alice"); err != nil {
		t.Fatalf("mark all read: %v", err)
	}
	if _, unread, _ := st.List("alice", 50); unread != 0 {
		t.Fatalf("after mark-all-read, expected unread=0, got %d", unread)
	}
}

func TestChangesSinceAndMaxSeq(t *testing.T) {
	st := openTest(t)
	n1 := emit(t, st, "alice", "one")
	n2 := emit(t, st, "alice", "two")

	max, err := st.MaxSeq("alice")
	if err != nil {
		t.Fatalf("max seq: %v", err)
	}
	if max != n2.Seq {
		t.Fatalf("expected max seq %d, got %d", n2.Seq, max)
	}
	// Replay everything after the first row — oldest first.
	changes, err := st.ChangesSince("alice", n1.Seq)
	if err != nil {
		t.Fatalf("changes since: %v", err)
	}
	if len(changes) != 1 || changes[0].Seq != n2.Seq {
		t.Fatalf("expected only n2 after n1, got %+v", changes)
	}
}

// TestListReadsUnderWriteLock deterministically proves List is atomic: it acquires the same write
// lock every mutation takes, so it cannot interleave with a writer. While the lock is held by a
// (simulated) in-flight write, a List call must block; once released, it completes.
func TestListReadsUnderWriteLock(t *testing.T) {
	st := openTest(t)
	emit(t, st, "alice", "seed")

	st.mu.Lock()
	done := make(chan struct{})
	go func() {
		_, _, _ = st.List("alice", 50)
		close(done)
	}()
	select {
	case <-done:
		st.mu.Unlock()
		t.Fatal("List returned while the write lock was held — its read is not atomic")
	case <-time.After(50 * time.Millisecond):
		// Correctly blocked on the lock.
	}
	st.mu.Unlock()
	<-done // now it can complete
}

// TestListSnapshotUnderConcurrentWrites is the behavioural proof (run under -race): while writers
// emit new unread rows and mark everything read, every List must return a consistent snapshot —
// the unread count must equal the number of unread rows it returned. The whole history stays within
// the limit, so no unread row can hide off-page; a torn read (rows and count from different points
// in time) would break the equality.
func TestListSnapshotUnderConcurrentWrites(t *testing.T) {
	st := openTest(t)
	const user = "alice"

	var done atomic.Bool
	go func() {
		var wg sync.WaitGroup
		for e := 0; e < 3; e++ { // emitters: 150 unread rows total, well under the 200 limit
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					_, _, _ = st.Emit(Notification{User: user, Service: "test", Title: "n", Level: "info"})
				}
			}()
		}
		for m := 0; m < 3; m++ { // markers: flip everything to read
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					_ = st.MarkAllRead(user)
				}
			}()
		}
		wg.Wait()
		done.Store(true)
	}()

	checks := 0
	for !done.Load() || checks == 0 {
		list, unread, err := st.List(user, 200)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		got := 0
		for _, n := range list {
			if !n.Read {
				got++
			}
		}
		if got != unread {
			t.Fatalf("torn read: list holds %d unread rows but count reported %d (len=%d)", got, unread, len(list))
		}
		checks++
	}
}
