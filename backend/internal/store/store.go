// Package store is notify's persistence layer: a single embedded pure-Go SQLite table of
// per-user notifications with a monotonic autoincrement seq. That seq is the one spine that
// drives the SSE stream id (so a reconnecting client replays exactly what it missed via
// Last-Event-ID) and the unread badge. Writes are serialised by one mutex (SQLite max-open-
// conns is 1); committed rows are fanned out to live subscribers by the push hub.
package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned for a missing notification.
var ErrNotFound = errors.New("not found")

// Notification is one entry as it is stored, emitted over SSE and returned to the shell. Seq is
// the monotonic id; User routes it (never serialised — a client only ever sees its own).
type Notification struct {
	Seq     int64     `json:"seq"`
	ID      string    `json:"id"`
	User    string    `json:"-"`
	Service string    `json:"service"`
	Title   string    `json:"title"`
	Body    string    `json:"body,omitempty"`
	URL     string    `json:"url,omitempty"`
	Icon    string    `json:"icon,omitempty"`
	Level   string    `json:"level"`
	Created time.Time `json:"created"`
	Read    bool      `json:"read"`
	// Dedupe is an optional idempotency key (never serialised to the client): a second Emit with
	// the same (user, Dedupe) is a no-op, so an overlapping reminder tick fires exactly once.
	Dedupe string `json:"-"`
}

const schema = `
CREATE TABLE IF NOT EXISTS notifications(
  seq      INTEGER PRIMARY KEY AUTOINCREMENT,
  id       TEXT NOT NULL,
  user     TEXT NOT NULL,
  service  TEXT NOT NULL,
  title    TEXT NOT NULL,
  body     TEXT NOT NULL DEFAULT '',
  url      TEXT NOT NULL DEFAULT '',
  icon     TEXT NOT NULL DEFAULT '',
  level    TEXT NOT NULL DEFAULT 'info',
  dedupe   TEXT NOT NULL DEFAULT '',
  created  INTEGER NOT NULL,
  read_at  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS notif_user ON notifications(user, seq);
`

// Store owns the SQLite database and the change observers.
type Store struct {
	db *sql.DB
	mu sync.Mutex // serialises mutations (one writer)

	subMu sync.RWMutex
	subs  []func(Notification)
}

// OnChange registers an observer invoked (synchronously, after commit) for every new
// notification. The push hub uses this to fan out to SSE clients; observers must not block.
func (s *Store) OnChange(fn func(Notification)) {
	s.subMu.Lock()
	s.subs = append(s.subs, fn)
	s.subMu.Unlock()
}

func (s *Store) emit(n Notification) {
	s.subMu.RLock()
	subs := s.subs
	s.subMu.RUnlock()
	for _, fn := range subs {
		fn(n)
	}
}

// Open initialises the data root and the SQLite index.
func Open(root string) (*Store, error) {
	if root == "" {
		root = "/var/lib/notify"
	}
	for _, d := range []string{root, filepath.Join(root, "index")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "index", "notify.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func randID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Emit stores a new notification and fans it out to live subscribers. When dedupe is non-empty
// and a notification with the same (user, dedupe) already exists, no new row is created: the
// existing one is returned with created=false and nothing is pushed (idempotent — a reminder
// scheduler that overlaps a tick fires each occurrence exactly once).
func (s *Store) Emit(n Notification) (Notification, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if n.Dedupe != "" {
		var seq int64
		switch err := s.db.QueryRow(`SELECT seq FROM notifications WHERE user=? AND dedupe=? LIMIT 1`, n.User, n.Dedupe).Scan(&seq); err {
		case nil:
			n.Seq = seq
			return n, false, nil
		case sql.ErrNoRows:
			// fall through to insert
		default:
			return Notification{}, false, err
		}
	}

	n.ID = randID()
	if n.Created.IsZero() {
		n.Created = time.Now()
	}
	res, err := s.db.Exec(
		`INSERT INTO notifications(id,user,service,title,body,url,icon,level,dedupe,created,read_at) VALUES(?,?,?,?,?,?,?,?,?,?,0)`,
		n.ID, n.User, n.Service, n.Title, n.Body, n.URL, n.Icon, n.Level, n.Dedupe, n.Created.Unix(),
	)
	if err != nil {
		return Notification{}, false, err
	}
	if seq, err := res.LastInsertId(); err == nil {
		n.Seq = seq
	}
	s.emit(n)
	return n, true, nil
}

// List returns the caller's most recent notifications (newest first, capped at limit) and the
// count of unread ones.
func (s *Store) List(user string, limit int) ([]Notification, int, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.Query(
		`SELECT seq,id,service,title,body,url,icon,level,created,read_at FROM notifications WHERE user=? ORDER BY seq DESC LIMIT ?`,
		user, limit,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []Notification
	for rows.Next() {
		var n Notification
		var created, readAt int64
		if err := rows.Scan(&n.Seq, &n.ID, &n.Service, &n.Title, &n.Body, &n.URL, &n.Icon, &n.Level, &created, &readAt); err != nil {
			return nil, 0, err
		}
		n.Created = time.Unix(created, 0)
		n.Read = readAt != 0
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	var unread int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM notifications WHERE user=? AND read_at=0`, user).Scan(&unread); err != nil {
		return nil, 0, err
	}
	return out, unread, nil
}

// MarkRead marks the given notification ids (scoped to the caller) as read.
func (s *Store) MarkRead(user string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`UPDATE notifications SET read_at=? WHERE user=? AND id=? AND read_at=0`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, id := range ids {
		if _, err := stmt.Exec(now, user, id); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// MarkAllRead marks every unread notification for the caller as read.
func (s *Store) MarkAllRead(user string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`UPDATE notifications SET read_at=? WHERE user=? AND read_at=0`, time.Now().Unix(), user)
	return err
}

// MaxSeq returns the caller's highest notification seq (0 if none) — the SSE "hello" cursor.
func (s *Store) MaxSeq(user string) (int64, error) {
	var seq sql.NullInt64
	if err := s.db.QueryRow(`SELECT MAX(seq) FROM notifications WHERE user=?`, user).Scan(&seq); err != nil {
		return 0, err
	}
	return seq.Int64, nil
}

// ChangesSince returns the caller's notifications with seq greater than the given cursor, oldest
// first — the Last-Event-ID replay for a reconnecting SSE client.
func (s *Store) ChangesSince(user string, seq int64) ([]Notification, error) {
	rows, err := s.db.Query(
		`SELECT seq,id,service,title,body,url,icon,level,created,read_at FROM notifications WHERE user=? AND seq>? ORDER BY seq ASC LIMIT 200`,
		user, seq,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Notification
	for rows.Next() {
		var n Notification
		var created, readAt int64
		if err := rows.Scan(&n.Seq, &n.ID, &n.Service, &n.Title, &n.Body, &n.URL, &n.Icon, &n.Level, &created, &readAt); err != nil {
			return nil, err
		}
		n.Created = time.Unix(created, 0)
		n.Read = readAt != 0
		out = append(out, n)
	}
	return out, rows.Err()
}

// Compact trims history: it deletes read notifications older than `before`, and always keeps at
// least the most recent 100 per user (a floor so a burst isn't lost). Called on a daily ticker.
func (s *Store) Compact(before time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`DELETE FROM notifications WHERE read_at<>0 AND created < ?
		 AND seq NOT IN (SELECT seq FROM notifications n2 WHERE n2.user=notifications.user ORDER BY seq DESC LIMIT 100)`,
		before.Unix(),
	)
	return err
}
