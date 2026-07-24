package push

import (
	"testing"
	"time"

	"notify/internal/store"
)

func newHub() *Hub {
	return &Hub{subs: make(map[string]map[*sub]struct{})}
}

func recv(t *testing.T, ch <-chan store.Notification) store.Notification {
	t.Helper()
	select {
	case n := <-ch:
		return n
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for a notification")
		return store.Notification{}
	}
}

func TestHubFanout(t *testing.T) {
	h := newHub()

	a1, _, cancelA1 := h.Subscribe("alice")
	a2, _, cancelA2 := h.Subscribe("alice")
	b1, _, cancelB1 := h.Subscribe("bob")
	defer cancelB1()

	// A notification for alice reaches both of her subscribers, never bob's.
	h.Publish(store.Notification{Seq: 1, User: "alice", Title: "x"})
	if recv(t, a1).Seq != 1 || recv(t, a2).Seq != 1 {
		t.Fatal("both alice subscribers should receive seq 1")
	}
	select {
	case n := <-b1:
		t.Fatalf("bob must not receive alice's notification: %+v", n)
	case <-time.After(50 * time.Millisecond):
	}

	// After cancelling one, only the other still receives.
	cancelA1()
	h.Publish(store.Notification{Seq: 2, User: "alice", Title: "y"})
	if recv(t, a2).Seq != 2 {
		t.Fatal("remaining alice subscriber should receive seq 2")
	}
	cancelA2()
	cancelA2() // cancel is idempotent
}

func TestPublishDropsAndFlagsOverflow(t *testing.T) {
	h := newHub()
	ch, overflow, cancel := h.Subscribe("alice")
	defer cancel()

	// Overflow the 64-deep buffer; Publish must never block on a slow consumer.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			h.Publish(store.Notification{Seq: int64(i), User: "alice", Title: "n"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on a full subscriber channel")
	}
	// Dropping ≥1 frame must raise the overflow flag so the SSE handler replays from the store.
	if !overflow.Load() {
		t.Fatal("overflow flag should be set after frames were dropped")
	}
	_ = ch
}

func TestSubscribeCleanupDropsUser(t *testing.T) {
	h := newHub()
	_, _, cancel := h.Subscribe("alice")

	h.mu.Lock()
	n := len(h.subs["alice"])
	h.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected 1 subscriber for alice, got %d", n)
	}

	cancel()

	h.mu.Lock()
	_, ok := h.subs["alice"]
	h.mu.Unlock()
	if ok {
		t.Fatal("cancelling the last subscriber should drop the user's map entry")
	}
}
