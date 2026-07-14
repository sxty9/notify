// Command notifyd is the holistic notification service daemon. It exposes an HTTP surface under
// /api/services/notify/, validates the shared holistic session (a signed JWT in the h_access
// cookie) without any RPC to the holistic backend, and is the platform-wide notification
// interface: any service daemon posts to POST internal/emit with the shared secret, and the
// shell's notification centre lists/streams them per user. Runs unprivileged behind Caddy.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"notify/internal/api"
	"notify/internal/auth"
	"notify/internal/push"
	"notify/internal/store"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8778", "address to listen on")
	flag.Parse()

	secret, err := auth.LoadSecret()
	if err != nil {
		log.Fatalf("notifyd: %v", err)
	}
	// Admin = membership in this group (the single Linux source of truth). The systemd unit sets
	// NOTIFY_ADMIN_GROUP; the verifier defaults to "sudo" when it is empty.
	v := auth.NewVerifier(secret, os.Getenv("NOTIFY_ADMIN_GROUP"))

	dataRoot := getenv("NOTIFY_DATA", "/var/lib/notify")
	st, err := store.Open(dataRoot)
	if err != nil {
		log.Fatalf("notifyd: open store: %v", err)
	}
	defer st.Close()
	hub := push.New(st) // subscribes to the store's change stream; drives the SSE stream

	// The shared secret authenticates the machine-to-machine emit endpoint. Absent ⇒ emit is
	// disabled (fail closed) and only the session-authenticated read/stream endpoints work.
	internalSecret := readSecret("NOTIFY_INTERNAL_SECRET", "NOTIFY_INTERNAL_SECRET_FILE")
	if internalSecret == "" {
		log.Print("notifyd: warning: no internal secret — POST internal/emit disabled (set NOTIFY_INTERNAL_SECRET_FILE)")
	}

	srv := &http.Server{
		Handler:           api.New(v, st, hub, internalSecret, os.Getenv("NOTIFY_ENUM_GROUP")).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Bind synchronously so an "address in use" surfaces here, not in a goroutine.
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("notifyd: listen %s: %v", *listen, err)
	}
	go func() {
		log.Printf("notifyd listening on %s (data=%s)", *listen, dataRoot)
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("notifyd: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Trim old, already-read notifications once a day (keeps at least the recent 100 per user).
	go compactionLoop(ctx, st)

	<-ctx.Done()

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	log.Print("notifyd stopped")
}

// compactionLoop trims read notifications older than 30 days once a day.
func compactionLoop(ctx context.Context, st *store.Store) {
	t := time.NewTicker(24 * time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := st.Compact(time.Now().AddDate(0, 0, -30)); err != nil {
				log.Printf("notifyd: compaction: %v", err)
			}
		}
	}
}

func getenv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// readSecret returns a secret from the env var, else from the file named by fileEnv.
func readSecret(env, fileEnv string) string {
	if v := strings.TrimSpace(os.Getenv(env)); v != "" {
		return v
	}
	if path := strings.TrimSpace(os.Getenv(fileEnv)); path != "" {
		if b, err := os.ReadFile(path); err == nil {
			return strings.TrimSpace(string(b))
		}
	}
	return ""
}
