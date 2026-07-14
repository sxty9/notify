// Package push is notify's in-process live hub. Every committed notification is handed to the
// hub (via store.OnChange) and fanned out to that user's subscribers, each backing one SSE
// client. The hub holds no history. A slow consumer is never allowed to block the writer: if its
// buffer is full the frame is dropped AND the subscriber's overflow flag is raised, so the SSE
// handler notices and replays what it missed from the store (rather than the frame being lost
// until an unrelated reconnect).
package push

import (
	"sync"
	"sync/atomic"

	"notify/internal/store"
)

// sub is one live subscriber: a buffered channel plus an overflow flag the writer sets when a
// frame had to be dropped.
type sub struct {
	ch       chan store.Notification
	overflow atomic.Bool
}

// Hub fans new notifications out to per-user subscribers.
type Hub struct {
	mu   sync.Mutex
	subs map[string]map[*sub]struct{} // user -> set of subscribers
}

// New builds a hub and wires it to the store's change stream.
func New(st *store.Store) *Hub {
	h := &Hub{subs: make(map[string]map[*sub]struct{})}
	st.OnChange(h.Publish)
	return h
}

// Publish delivers a notification to every subscriber of its user. Non-blocking: on a full buffer
// (slow client) the frame is dropped and the subscriber's overflow flag is raised so the handler
// replays the gap from the store.
func (h *Hub) Publish(n store.Notification) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs[n.User] {
		select {
		case s.ch <- n:
		default:
			s.overflow.Store(true)
		}
	}
}

// Subscribe registers a new live subscriber for user and returns its channel, its overflow flag,
// and an idempotent cancel that detaches and closes it. Cancel must be called exactly once.
func (h *Hub) Subscribe(user string) (<-chan store.Notification, *atomic.Bool, func()) {
	s := &sub{ch: make(chan store.Notification, 64)}
	h.mu.Lock()
	if h.subs[user] == nil {
		h.subs[user] = make(map[*sub]struct{})
	}
	h.subs[user][s] = struct{}{}
	h.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs[user], s)
			if len(h.subs[user]) == 0 {
				delete(h.subs, user)
			}
			close(s.ch)
			h.mu.Unlock()
		})
	}
	return s.ch, &s.overflow, cancel
}
