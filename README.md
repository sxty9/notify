# holistic-notify (`notifyd`)

The **platform-wide notification service** for the Holistic ecosystem. Every Holistic user (a
Linux account — the single source of truth) always sees their own notifications; the only gated
capability is broadcasting to everyone. A Go daemon stores per-user notifications in an embedded
SQLite index and serves an HTTP API; **notify has no sidebar tab of its own** — its UI is the
top-bar notification bell the shell renders from `@holistic/ui` (`NotificationCenter`), shared by
every service. Any service daemon posts a notification on a user's behalf through one
machine-to-machine endpoint, so the whole platform speaks to the user through a single hub.

```
 icaly · maild · hosuto · …  ─ POST internal/emit (shared notify-secret) ─┐
                                                                          ▼
 Browser ─https─► Caddy (holistic.local) ───────────────────┐     notifyd (127.0.0.1:8778)
   ├─ /                        → holistic SPA (top-bar bell) │      • SQLite index (per-user, SoT)
   ├─ /api/*                   → holistic backend    (8770)  │      • admin broadcast → all users
   └─ /api/services/notify/*   → notifyd             (8778) ─┘      • live SSE stream (Last-Event-ID)
```

- **Single sign-on:** the daemon validates the same holistic session (HS256 JWT in the
  `h_access` cookie, secret `/etc/holistic/jwt-secret`) — no separate login.
- **Identity = Linux (single source of truth):** usernames are Linux accounts; admin = `sudo`.
  A machine-to-machine emit may only target a real Linux account.
- **Least privilege:** runs as the unprivileged `notify` system user, sandboxed by systemd;
  performs no escalation. Absent the shared emit secret, `internal/emit` fails closed.

## Prerequisites

The [holistic](https://github.com/sxty9/holistic) repo must be present **as a sibling**
(`../holistic`) with the dashboard installed — it provides the shared JWT secret and the SPA
(whose `@holistic/ui` package ships the `NotificationCenter` bell that renders this service).

## Quickstart

```bash
cd notify
sudo ./service setup     # build notifyd, wire systemd + Caddy, declare rights, provision the emit secret
```

`setup` links **no** UI plugin and does **not** rebuild the SPA — notify's UI already lives in
the shell (`@holistic/ui` + app shell). After `setup`, (re)build and deploy the dashboard SPA to
ship the notification centre. Other commands: `service build`, `service start|stop|restart`,
`service status`, `service update`, `service uninstall [--purge]`.

## Rights (privleg)

Notifications are inherently per-user (you always see your own), so the only declared right is the
admin one. Declared in `permissions/notify.json`, backed 1:1 by an `hp_*` Linux group and enforced
as `isAdmin || group ∈ user.groups`.

| Right | Group | Default | Gates |
|---|---|---|---|
| Broadcast announcements | `hp_notify_admin` | false (`dangerous`) | send one notification to every user |

`default:false` = admin-only until privleg grants the group to a non-admin user.

## API (`/api/services/notify/`)

| Method | Path | Access | Purpose |
|---|---|---|---|
| GET | `health` | none | liveness |
| GET | `notifications[?limit=]` | signed-in | your notifications (newest first) + unread count |
| POST | `notifications/read` | signed-in + CSRF | mark the given ids read (`{"ids":[…]}`) |
| POST | `notifications/read-all` | signed-in + CSRF | mark all your notifications read |
| GET | `notifications/stream` | signed-in (cookie) | live SSE stream; replays from `Last-Event-ID` |
| POST | `notifications/broadcast` | `hp_notify_admin` + CSRF | one notification to every user |
| POST | `internal/emit` | **notify-secret** | a service emits on a user's behalf (machine-to-machine) |

The stream is cookie-authenticated with no CSRF (an `EventSource` cannot set headers) and needs
Caddy's `flush_interval -1` — set by the generated route.

## Emitting from another service

`internal/emit` is the single access point for producing a notification; a service never writes to
another service's store. Authenticate with the shared secret and target a real Linux user:

```
POST /api/services/notify/internal/emit
X-Notify-Internal-Secret: <contents of /etc/holistic/notify-secret>
Content-Type: application/json

{ "user": "alice", "service": "icaly", "title": "Meeting in 10 min",
  "body": "Standup — Room 3", "url": "/services/icaly", "level": "info",
  "dedupe": "icaly:reminder:evt-123" }
```

`dedupe` is an optional idempotency key: a second emit with the same `(user, dedupe)` is a no-op,
so an overlapping reminder tick fires exactly once. `level` is one of `info` · `success` ·
`warning` · `error`. `url` must be a root-relative in-app path or an `http(s)` URL. The secret is
group-readable by the `holistic` group, so any service daemon can post. See
`sxty9/hosuto` (`internal/notify/`) for a worked client.

## Storage

A single embedded pure-Go SQLite index at `/var/lib/notify/index/notify.db` (WAL): one
`notifications` table keyed by a monotonic `seq` per user — the same spine that drives the SSE
stream id and the unread badge. Read notifications older than 30 days are compacted daily, always
keeping at least the most recent 100 per user. No history lives in the push hub.

## Integration (env)

| Var | Meaning |
|---|---|
| `NOTIFY_DATA` | data root (default `/var/lib/notify`) |
| `NOTIFY_INTERNAL_SECRET[_FILE]` | shared secret guarding `internal/emit` (absent = emit disabled) |
| `NOTIFY_ENUM_GROUP` | Linux group enumerating all users for a broadcast (default `smbusers`) |
| `NOTIFY_ADMIN_GROUP` | Linux group that confers admin (default `sudo`) |

## Local development

```bash
(cd backend && go build ./... && go vet ./... && go test ./...)
```

notify ships **no** local `ui/` package: its web surface is the `NotificationCenter` component in
`@holistic/ui` (`frontend/packages/ui/src/notificationcenter.tsx` in the holistic repo), rendered
by the app shell. All user-facing strings live in the shared i18n catalog there — never in this
service. This is the only Holistic service whose UI is contributed to the shell rather than as a
per-service plugin.

## Layout

```
service                       single-file CLI: setup / build / lifecycle / update
permissions/notify.json       rights manifest (drop-in for privleg)
backend/                      Go daemon (notifyd)
  cmd/notifyd/                  entry point — 127.0.0.1:8778
  internal/auth/                shared-JWT validation + live group/admin resolution + CSRF (reused)
  internal/rights/              the hp_notify_admin group constant
  internal/api/                 HTTP routes incl. the internal/emit ingest + SSE stream
  internal/store/               embedded SQLite index (emit/list/read/compact) + change stream
  internal/push/               in-process live hub → per-user SSE fan-out (drop + overflow-replay)
(ui lives in @holistic/ui — the shell's top-bar NotificationCenter, not a local plugin)
```

## License

MIT — see [LICENSE](LICENSE).
