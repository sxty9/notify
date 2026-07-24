// Package api serves notify's HTTP surface under /api/services/notify/, behind the shared
// holistic session. It is the platform-wide notification interface: any service daemon posts a
// notification to POST internal/emit (machine-to-machine, shared-secret authenticated); the
// shell's notification centre lists them (GET notifications), marks them read, and streams new
// ones live (GET notifications/stream, SSE). Error bodies follow holistic's contract:
// {"detail": "..."}.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"notify/internal/auth"
	"notify/internal/push"
	"notify/internal/rights"
	"notify/internal/store"
)

const (
	base    = "/api/services/notify/"
	service = "notify"
	version = "0.1.0"

	maxBody = 1 << 16 // 64 KiB: notifications are small

	maxTitle     = 200
	maxBodyRunes = 2000
	maxURL       = 1000
	maxIcon      = 16
)

var validLevels = map[string]bool{"info": true, "success": true, "warning": true, "error": true}

// Server wires the verifier, store and live hub into HTTP handlers.
type Server struct {
	v              *auth.Verifier
	st             *store.Store
	hub            *push.Hub
	internalSecret string // guards POST internal/emit (machine-to-machine); "" disables it (fail closed)
	enumGroup      string // Linux group enumerating all users, for admin broadcast (default smbusers)
}

// New builds a server. internalSecret guards the machine-to-machine emit endpoint; "" disables
// it so a misconfigured deploy fails closed rather than open.
func New(v *auth.Verifier, st *store.Store, hub *push.Hub, internalSecret, enumGroup string) *Server {
	if enumGroup == "" {
		enumGroup = "smbusers"
	}
	return &Server{v: v, st: st, hub: hub, internalSecret: internalSecret, enumGroup: enumGroup}
}

type handler func(w http.ResponseWriter, r *http.Request, u *auth.User)

// Handler returns the routed http.Handler (Go 1.22 method+path patterns).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+base+"health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	// Your own notifications (session auth; no special right — everyone sees their own).
	mux.HandleFunc("GET "+base+"notifications", s.guard("", false, s.list))
	mux.HandleFunc("POST "+base+"notifications/read", s.guard("", true, s.markRead))
	mux.HandleFunc("POST "+base+"notifications/read-all", s.guard("", true, s.markAllRead))
	// Live stream: cookie-authenticated GET, NO CSRF (EventSource can't set headers) — same model
	// as icaly's events/stream. Needs Caddy flush_interval -1.
	mux.HandleFunc("GET "+base+"notifications/stream", s.guard("", false, s.stream))
	// Admin broadcast to all users (hp_notify_admin || admin).
	mux.HandleFunc("POST "+base+"notifications/broadcast", s.guard(rights.GroupAdmin, true, s.broadcast))
	// Machine-to-machine ingest: any service daemon emits on a user's behalf with the shared secret.
	mux.HandleFunc("POST "+base+"internal/emit", s.internalEmit)
	return mux
}

// guard runs auth → optional right → optional CSRF, then the handler.
func (s *Server) guard(perm string, csrf bool, h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, err := s.v.User(r)
		if err != nil {
			writeErr(w, http.StatusUnauthorized, "Not authenticated")
			return
		}
		if perm != "" && !u.Can(perm) {
			writeErr(w, http.StatusForbidden, "You do not have permission for this action")
			return
		}
		if csrf && !s.v.CheckCSRF(r) {
			writeErr(w, http.StatusForbidden, "CSRF check failed")
			return
		}
		h(w, r, u)
	}
}

func (s *Server) list(w http.ResponseWriter, r *http.Request, u *auth.User) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	ns, unread, err := s.st.List(u.Username, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "Could not list notifications")
		return
	}
	if ns == nil {
		ns = []store.Notification{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"notifications": ns, "unread": unread})
}

func (s *Server) markRead(w http.ResponseWriter, r *http.Request, u *auth.User) {
	var body struct {
		IDs []string `json:"ids"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, "Invalid request")
		return
	}
	// Bound the batch so one request can't hold the single write connection across a huge
	// per-id UPDATE loop (the client only ever marks the visible page).
	if len(body.IDs) > 500 {
		writeErr(w, http.StatusBadRequest, "Too many ids")
		return
	}
	if err := s.st.MarkRead(u.Username, body.IDs); err != nil {
		writeErr(w, http.StatusInternalServerError, "Could not update notifications")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) markAllRead(w http.ResponseWriter, r *http.Request, u *auth.User) {
	if err := s.st.MarkAllRead(u.Username); err != nil {
		writeErr(w, http.StatusInternalServerError, "Could not update notifications")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// changesPage is the LIMIT used by store.ChangesSince; a full page means "there may be more".
const changesPage = 200

// stream is the per-user SSE endpoint. Cookie-authenticated, no CSRF (EventSource sets no
// headers). It sends a "hello", replays everything missed since a reconnect's Last-Event-ID (paged
// so a large backlog is not truncated), then streams "notify" frames until the client disconnects.
// A high-water-mark (lastSent) dedupes the replay/live overlap, so no seq is ever emitted twice on
// one connection; a per-write deadline guarantees a stuck client is dropped (freeing the goroutine
// and subscription) rather than blocking forever; and the subscriber's overflow flag triggers a
// store replay to recover any frame the hub had to drop.
func (s *Server) stream(w http.ResponseWriter, r *http.Request, u *auth.User) {
	fl, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "Streaming unsupported")
		return
	}
	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	ch, overflow, cancel := s.hub.Subscribe(u.Username)
	defer cancel()

	// lastSent is the highest seq written on THIS connection; it seeds from the reconnect cursor so
	// the replay resends exactly what was missed, and dedupes any live frame that overlaps a replay.
	lastSent := lastEventID(r)

	// write sets a deadline first so a stalled client fails the write (and we return, releasing the
	// subscription) instead of blocking indefinitely.
	write := func(id int64, event string, data any) bool {
		_ = rc.SetWriteDeadline(time.Now().Add(15 * time.Second))
		if err := writeSSE(w, fl, id, event, data); err != nil {
			return false
		}
		if id > lastSent {
			lastSent = id
		}
		return true
	}
	// replay sends every unseen row with seq > lastSent, paging until the store is exhausted.
	replay := func() bool {
		for {
			missed, err := s.st.ChangesSince(u.Username, lastSent)
			if err != nil || len(missed) == 0 {
				return true
			}
			for _, n := range missed {
				if n.Seq <= lastSent {
					continue
				}
				if !write(n.Seq, "notify", n) {
					return false
				}
			}
			if len(missed) < changesPage {
				return true
			}
		}
	}

	maxSeq, _ := s.st.MaxSeq(u.Username)
	// hello carries no id, so it does not advance lastSent (which would suppress the replay).
	if !write(0, "hello", map[string]int64{"seq": maxSeq}) {
		return
	}
	if lastSent > 0 {
		if !replay() {
			return
		}
	}

	ctx := r.Context()
	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case n, ok := <-ch:
			if !ok {
				return
			}
			if n.Seq > lastSent { // skip a live frame already delivered by the replay
				if !write(n.Seq, "notify", n) {
					return
				}
			}
			if overflow.Swap(false) && !replay() { // the hub dropped ≥1 frame — recover from the store
				return
			}
		case <-ping.C:
			if overflow.Swap(false) && !replay() {
				return
			}
			_ = rc.SetWriteDeadline(time.Now().Add(15 * time.Second))
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			fl.Flush()
		}
	}
}

// broadcast sends one notification to every enumerated user (admin only). Useful for
// system-wide announcements (maintenance windows, etc.).
func (s *Server) broadcast(w http.ResponseWriter, r *http.Request, u *auth.User) {
	var body struct {
		Title string `json:"title"`
		Body  string `json:"body"`
		URL   string `json:"url"`
		Icon  string `json:"icon"`
		Level string `json:"level"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, "Invalid request")
		return
	}
	title := strings.TrimSpace(body.Title)
	if title == "" {
		writeErr(w, http.StatusBadRequest, "A title is required")
		return
	}
	users := s.enumUsers()
	sent := 0
	for _, name := range users {
		_, created, err := s.st.Emit(store.Notification{
			User:    name,
			Service: "system",
			Title:   truncate(title, maxTitle),
			Body:    truncate(strings.TrimSpace(body.Body), maxBodyRunes),
			URL:     sanitizeURL(body.URL),
			Icon:    truncate(strings.TrimSpace(body.Icon), maxIcon),
			Level:   sanitizeLevel(body.Level),
		})
		if err == nil && created {
			sent++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "recipients": len(users), "sent": sent})
}

// internalEmit is the machine-to-machine ingest point: a service daemon posts a notification on
// a user's behalf, authenticated only by the shared internal secret (never a session). The
// target user must be a real Linux account (parity with maild's on-behalf-of validation).
func (s *Server) internalEmit(w http.ResponseWriter, r *http.Request) {
	if s.internalSecret == "" {
		writeErr(w, http.StatusServiceUnavailable, "Emit not configured")
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Notify-Internal-Secret")), []byte(s.internalSecret)) != 1 {
		writeErr(w, http.StatusUnauthorized, "Not authenticated")
		return
	}
	var body struct {
		User    string `json:"user"`
		Service string `json:"service"`
		Title   string `json:"title"`
		Body    string `json:"body"`
		URL     string `json:"url"`
		Icon    string `json:"icon"`
		Level   string `json:"level"`
		Dedupe  string `json:"dedupe"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, "Invalid request")
		return
	}
	user := strings.TrimSpace(body.User)
	title := strings.TrimSpace(body.Title)
	if user == "" || title == "" {
		writeErr(w, http.StatusBadRequest, "user and title are required")
		return
	}
	// Only emit to real accounts, so a compromised/buggy caller can't seed rows for arbitrary
	// principal strings.
	if !auth.UserExists(user) {
		writeErr(w, http.StatusUnprocessableEntity, "Unknown user")
		return
	}
	svc := strings.TrimSpace(body.Service)
	if svc == "" {
		svc = "system"
	}
	n, created, err := s.st.Emit(store.Notification{
		User:    user,
		Service: truncate(svc, 40),
		Title:   truncate(title, maxTitle),
		Body:    truncate(strings.TrimSpace(body.Body), maxBodyRunes),
		URL:     sanitizeURL(body.URL),
		Icon:    truncate(strings.TrimSpace(body.Icon), maxIcon),
		Level:   sanitizeLevel(body.Level),
		Dedupe:  truncate(strings.TrimSpace(body.Dedupe), 200),
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "Could not store notification")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"seq": n.Seq, "id": n.ID, "created": created})
}

// enumUsers lists the members of the enumeration group (holistic-managed users).
func (s *Server) enumUsers() []string {
	out, err := exec.Command("getent", "group", s.enumGroup).Output()
	if err != nil {
		return nil
	}
	// getent group line: name:passwd:gid:member1,member2,...
	line := strings.TrimSpace(string(out))
	i := strings.LastIndex(line, ":")
	if i < 0 || i+1 >= len(line) {
		return nil
	}
	var users []string
	for _, m := range strings.Split(line[i+1:], ",") {
		if m = strings.TrimSpace(m); m != "" {
			users = append(users, m)
		}
	}
	return users
}

// --- helpers ---

// sanitizeURL permits only a root-relative in-app path or an http(s) URL, so a notification link
// can never carry a javascript:/data: scheme into the shell's window.open.
func sanitizeURL(raw string) string {
	u := strings.TrimSpace(raw)
	if u == "" {
		return ""
	}
	// Root-relative in-app path only. Reject "//host" and "/\host" — browsers normalise the
	// backslash to a slash, turning both into a protocol-relative jump to an external origin.
	if strings.HasPrefix(u, "/") && !strings.HasPrefix(u, "//") && !strings.HasPrefix(u, "/\\") {
		return truncate(u, maxURL)
	}
	if strings.HasPrefix(u, "https://") || strings.HasPrefix(u, "http://") {
		return truncate(u, maxURL)
	}
	return ""
}

func sanitizeLevel(l string) string {
	l = strings.ToLower(strings.TrimSpace(l))
	if validLevels[l] {
		return l
	}
	return "info"
}

func truncate(s string, maxRunes int) string {
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	r := []rune(s)
	return string(r[:maxRunes])
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func writeSSE(w io.Writer, fl http.Flusher, id int64, eventName string, data any) error {
	b, _ := json.Marshal(data)
	var sb strings.Builder
	if id > 0 {
		fmt.Fprintf(&sb, "id: %d\n", id)
	}
	fmt.Fprintf(&sb, "event: %s\ndata: %s\n\n", eventName, b)
	if _, err := io.WriteString(w, sb.String()); err != nil {
		return err
	}
	fl.Flush()
	return nil
}

func lastEventID(r *http.Request) int64 {
	v := r.Header.Get("Last-Event-ID")
	if v == "" {
		v = r.URL.Query().Get("lastEventId")
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	return n
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}
