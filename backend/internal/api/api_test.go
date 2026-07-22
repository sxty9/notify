package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/user"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"notify/internal/auth"
	"notify/internal/push"
	"notify/internal/store"
)

const (
	csrfVal      = "csrf-test-value"
	emitSecret   = "test-internal-secret"
	signingToken = "test-secret-please-ignore"
)

// harness spins up the full HTTP surface backed by a temp store, authenticated as the current OS
// user whose primary group is used as the admin group — so rights always pass. This exercises
// routing/handlers, not the (separately covered) rights resolution.
type harness struct {
	st    *store.Store
	srv   *httptest.Server
	user  string
	group string
	token string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	cur, g := currentUserGroup(t)
	secret := []byte(signingToken)
	v := auth.NewVerifier(secret, g) // current user ∈ this group ⇒ admin ⇒ all rights pass

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	hub := push.New(st)
	srv := httptest.NewServer(New(v, st, hub, emitSecret, g).Handler())
	t.Cleanup(srv.Close)

	return &harness{st: st, srv: srv, user: cur, group: g, token: mintToken(t, secret, cur)}
}

func currentUserGroup(t *testing.T) (string, string) {
	t.Helper()
	cur, err := user.Current()
	if err != nil || cur.Username == "" {
		t.Skip("no current OS user")
	}
	gids, err := cur.GroupIds()
	if err != nil || len(gids) == 0 {
		t.Skip("cannot resolve current user's groups")
	}
	g, err := user.LookupGroupId(gids[0])
	if err != nil || g.Name == "" {
		t.Skip("cannot resolve a group name")
	}
	return cur.Username, g.Name
}

func mintToken(t *testing.T, secret []byte, sub string) string {
	t.Helper()
	claims := jwt.MapClaims{"sub": sub, "type": "access", "exp": time.Now().Add(time.Hour).Unix()}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return tok
}

func (h *harness) req(t *testing.T, method, path string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, h.srv.URL+path, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: "h_access", Value: h.token})
	if method != http.MethodGet && method != http.MethodHead {
		req.AddCookie(&http.Cookie{Name: "h_csrf", Value: csrfVal})
		req.Header.Set("X-CSRF-Token", csrfVal)
	}
	return req
}

func (h *harness) do(t *testing.T, req *http.Request) *http.Response {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", req.Method, req.URL.Path, err)
	}
	return resp
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

type listResp struct {
	Notifications []store.Notification `json:"notifications"`
	Unread        int                  `json:"unread"`
}

func TestHealthNoAuth(t *testing.T) {
	h := newHarness(t)
	resp, err := http.Get(h.srv.URL + base + "health")
	if err != nil {
		t.Fatalf("get health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status %d", resp.StatusCode)
	}
}

func TestListRequiresSession(t *testing.T) {
	h := newHarness(t)
	req, _ := http.NewRequest(http.MethodGet, h.srv.URL+base+"notifications", nil) // no cookie
	resp := h.do(t, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without a session, got %d", resp.StatusCode)
	}
}

func TestListMarkReadRoundTrip(t *testing.T) {
	h := newHarness(t)
	n1, _, err := h.st.Emit(store.Notification{User: h.user, Service: "icaly", Title: "One", Level: "info"})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, _, err := h.st.Emit(store.Notification{User: h.user, Service: "mail", Title: "Two", Level: "warning"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var listed listResp
	decode(t, h.do(t, h.req(t, http.MethodGet, base+"notifications", nil)), &listed)
	if len(listed.Notifications) != 2 || listed.Unread != 2 {
		t.Fatalf("expected 2 notifications + unread 2, got %d / %d", len(listed.Notifications), listed.Unread)
	}
	if listed.Notifications[0].Title != "Two" {
		t.Errorf("expected newest-first ordering, got %q first", listed.Notifications[0].Title)
	}

	// Mark the older one read.
	resp := h.do(t, h.req(t, http.MethodPost, base+"notifications/read", strings.NewReader(`{"ids":["`+n1.ID+`"]}`)))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("markRead status %d", resp.StatusCode)
	}
	decode(t, h.do(t, h.req(t, http.MethodGet, base+"notifications", nil)), &listed)
	if listed.Unread != 1 {
		t.Fatalf("expected unread 1 after markRead, got %d", listed.Unread)
	}

	// read-all clears the remainder.
	resp = h.do(t, h.req(t, http.MethodPost, base+"notifications/read-all", nil))
	resp.Body.Close()
	decode(t, h.do(t, h.req(t, http.MethodGet, base+"notifications", nil)), &listed)
	if listed.Unread != 0 {
		t.Fatalf("expected unread 0 after read-all, got %d", listed.Unread)
	}
}

func TestMarkReadRequiresCSRF(t *testing.T) {
	h := newHarness(t)
	req, _ := http.NewRequest(http.MethodPost, h.srv.URL+base+"notifications/read", strings.NewReader(`{"ids":[]}`))
	req.AddCookie(&http.Cookie{Name: "h_access", Value: h.token}) // valid session, but no CSRF token
	resp := h.do(t, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 without a CSRF token, got %d", resp.StatusCode)
	}
}

func TestInternalEmit(t *testing.T) {
	h := newHarness(t)
	emit := func(secret, body string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, h.srv.URL+base+"internal/emit", strings.NewReader(body))
		if secret != "" {
			req.Header.Set("X-Notify-Internal-Secret", secret)
		}
		return h.do(t, req)
	}

	// Wrong secret → 401 (machine-to-machine auth, never a session).
	r := emit("nope", `{"user":"`+h.user+`","title":"hi"}`)
	r.Body.Close()
	if r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong secret: want 401, got %d", r.StatusCode)
	}

	// Unknown target user → 422 (only real Linux accounts may be seeded).
	r = emit(emitSecret, `{"user":"definitely-not-a-real-user-zzz","title":"hi"}`)
	r.Body.Close()
	if r.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("unknown user: want 422, got %d", r.StatusCode)
	}

	// Missing title → 400.
	r = emit(emitSecret, `{"user":"`+h.user+`"}`)
	r.Body.Close()
	if r.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing title: want 400, got %d", r.StatusCode)
	}

	// Valid → 200 created, then a same-(user,dedupe) replay is idempotent (created=false, same seq).
	var first struct {
		Seq     int64  `json:"seq"`
		ID      string `json:"id"`
		Created bool   `json:"created"`
	}
	decode(t, emit(emitSecret, `{"user":"`+h.user+`","service":"icaly","title":"Reminder","dedupe":"k1"}`), &first)
	if !first.Created || first.Seq == 0 {
		t.Fatalf("valid emit: created=%v seq=%d, want created=true seq>0", first.Created, first.Seq)
	}
	var second struct {
		Seq     int64 `json:"seq"`
		Created bool  `json:"created"`
	}
	decode(t, emit(emitSecret, `{"user":"`+h.user+`","service":"icaly","title":"Reminder dup","dedupe":"k1"}`), &second)
	if second.Created || second.Seq != first.Seq {
		t.Fatalf("dedupe replay: created=%v seq=%d, want created=false seq=%d", second.Created, second.Seq, first.Seq)
	}
}

func TestBroadcastRequiresAdmin(t *testing.T) {
	cur, _ := currentUserGroup(t)
	secret := []byte(signingToken)
	// Admin group the current user is NOT a member of ⇒ not admin ⇒ lacks hp_notify_admin ⇒ 403.
	v := auth.NewVerifier(secret, "holistic-no-such-admin-group-zzz")
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := httptest.NewServer(New(v, st, push.New(st), emitSecret, "nogroup").Handler())
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+base+"notifications/broadcast", strings.NewReader(`{"title":"Maintenance"}`))
	req.AddCookie(&http.Cookie{Name: "h_access", Value: mintToken(t, secret, cur)})
	req.AddCookie(&http.Cookie{Name: "h_csrf", Value: csrfVal})
	req.Header.Set("X-CSRF-Token", csrfVal)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("non-admin broadcast: want 403, got %d", resp.StatusCode)
	}
}

func TestBroadcastAsAdmin(t *testing.T) {
	h := newHarness(t) // current user is admin
	resp := h.do(t, h.req(t, http.MethodPost, base+"notifications/broadcast",
		strings.NewReader(`{"title":"Maintenance at 9pm","level":"warning"}`)))
	var out struct {
		OK         bool `json:"ok"`
		Recipients int  `json:"recipients"`
		Sent       int  `json:"sent"`
	}
	decode(t, resp, &out)
	if !out.OK {
		t.Fatalf("admin broadcast should succeed, got %+v", out)
	}
	// Recipient enumeration is environment-dependent; only assert sane bounds.
	if out.Recipients < 0 || out.Sent < 0 || out.Sent > out.Recipients {
		t.Fatalf("implausible broadcast counts: %+v", out)
	}
}

func TestStreamHelloAndLiveFrame(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, h.srv.URL+base+"notifications/stream", nil)
	req.AddCookie(&http.Cookie{Name: "h_access", Value: h.token})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("stream content-type %q", ct)
	}

	sc := bufio.NewScanner(resp.Body)
	if !scanForLine(sc, func(l string) bool { return l == "event: hello" }) {
		t.Fatal("did not receive the hello frame")
	}
	// Emit a live notification; it must arrive as a notify frame carrying the title.
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _, _ = h.st.Emit(store.Notification{User: h.user, Service: "icaly", Title: "LiveOne", Level: "info"})
	}()
	if !scanForLine(sc, func(l string) bool { return strings.HasPrefix(l, "data:") && strings.Contains(l, "LiveOne") }) {
		t.Fatal("did not receive the live notify frame")
	}
}

func scanForLine(sc *bufio.Scanner, match func(string) bool) bool {
	for sc.Scan() {
		if match(strings.TrimRight(sc.Text(), "\r")) {
			return true
		}
	}
	return false
}

func TestSanitizeURL(t *testing.T) {
	cases := map[string]string{
		"":                      "",
		"/services/icaly":       "/services/icaly",
		"https://example.com/x": "https://example.com/x",
		"http://example.com":    "http://example.com",
		"//evil.com":            "", // protocol-relative jump to another origin
		"/\\evil.com":           "", // backslash normalised to // by browsers
		"javascript:alert(1)":   "",
		"data:text/html,x":      "",
		"ftp://host/x":          "",
		"relative/path":         "",
	}
	for in, want := range cases {
		if got := sanitizeURL(in); got != want {
			t.Errorf("sanitizeURL(%q)=%q, want %q", in, got, want)
		}
	}
}

func TestSanitizeLevel(t *testing.T) {
	for _, in := range []string{"info", "success", "warning", "error"} {
		if got := sanitizeLevel(in); got != in {
			t.Errorf("sanitizeLevel(%q)=%q, want it unchanged", in, got)
		}
	}
	if got := sanitizeLevel("WARNING"); got != "warning" {
		t.Errorf("sanitizeLevel(WARNING)=%q, want warning (case-normalised)", got)
	}
	for _, in := range []string{"", "critical", "debug", "  ", "notalevel"} {
		if got := sanitizeLevel(in); got != "info" {
			t.Errorf("sanitizeLevel(%q)=%q, want the info fallback", in, got)
		}
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("hello", 10); got != "hello" {
		t.Errorf("truncate under limit=%q, want hello", got)
	}
	if got := truncate("hello", 3); got != "hel" {
		t.Errorf("truncate=%q, want hel", got)
	}
	// Rune-aware: a multibyte rune is never split.
	if got := truncate("héllo", 2); got != "hé" {
		t.Errorf("truncate multibyte=%q, want hé", got)
	}
}
