// Package auth validates the holistic dashboard session (a shared HS256 JWT in the
// h_access cookie) WITHOUT any RPC to the holistic backend, then resolves the caller's
// groups and admin status from the single Linux source of truth (the OS group database).
// This file is service-agnostic — it is the same glue every holistic service needs.
package auth

import (
	"crypto/hmac"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

const (
	accessCookie = "h_access"
	csrfCookie   = "h_csrf"
	csrfHeader   = "X-CSRF-Token"
)

// ErrNoSession means the request carried no valid, unexpired access token.
var ErrNoSession = errors.New("not authenticated")

// User is the resolved identity for a request. Admin status is read live from the OS,
// never trusted from the token (the access token carries only {sub, type, exp}).
type User struct {
	Username string   `json:"username"`
	Groups   []string `json:"groups"`
	IsAdmin  bool     `json:"isAdmin"`
}

// Can reports whether the user holds a fine-grained right under the holistic rights
// standard: admins implicitly hold every right, otherwise membership in the backing
// Linux group decides. Additive — on a host without privleg the hp_* groups are empty,
// so this reduces to admin-only (identical to pre-rights-standard behaviour).
func (u *User) Can(group string) bool {
	return u.IsAdmin || contains(u.Groups, group)
}

// Verifier holds the shared signing secret and the group that confers admin.
type Verifier struct {
	secret     []byte
	adminGroup string
}

// LoadSecret reads the shared JWT secret the same way the holistic backend does:
// HOLISTIC_SECRET_FILE (default /etc/holistic/jwt-secret), else HOLISTIC_SECRET.
func LoadSecret() ([]byte, error) {
	path := os.Getenv("HOLISTIC_SECRET_FILE")
	if path == "" {
		path = "/etc/holistic/jwt-secret"
	}
	if b, err := os.ReadFile(path); err == nil {
		if s := strings.TrimSpace(string(b)); s != "" {
			return []byte(s), nil
		}
	}
	if env := os.Getenv("HOLISTIC_SECRET"); env != "" {
		return []byte(env), nil
	}
	return nil, errors.New("no JWT secret: set HOLISTIC_SECRET_FILE or HOLISTIC_SECRET")
}

// NewVerifier builds a verifier. adminGroup defaults to "sudo" when empty.
func NewVerifier(secret []byte, adminGroup string) *Verifier {
	if adminGroup == "" {
		adminGroup = "sudo"
	}
	return &Verifier{secret: secret, adminGroup: adminGroup}
}

// User extracts and validates the session from the request.
func (v *Verifier) User(r *http.Request) (*User, error) {
	c, err := r.Cookie(accessCookie)
	if err != nil || c.Value == "" {
		return nil, ErrNoSession
	}
	tok, err := jwt.Parse(c.Value, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return v.secret, nil
	}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired())
	if err != nil || !tok.Valid {
		return nil, ErrNoSession
	}
	claims, ok := tok.Claims.(jwt.MapClaims)
	if !ok {
		return nil, ErrNoSession
	}
	// Reject refresh tokens — only access tokens authenticate API requests.
	if t, _ := claims["type"].(string); t != "access" {
		return nil, ErrNoSession
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return nil, ErrNoSession
	}
	// Reject a token whose backing Linux account no longer exists (parity with
	// holistic, which 401s on a deleted account). Admin is derived live from groups.
	groups, exists := resolveGroups(sub)
	if !exists {
		return nil, ErrNoSession
	}
	return &User{Username: sub, Groups: groups, IsAdmin: contains(groups, v.adminGroup)}, nil
}

// CheckCSRF enforces the double-submit guard: header X-CSRF-Token must equal the
// readable h_csrf cookie. Required on mutating requests; not on GETs.
func (v *Verifier) CheckCSRF(r *http.Request) bool {
	c, err := r.Cookie(csrfCookie)
	if err != nil || c.Value == "" {
		return false
	}
	return hmac.Equal([]byte(r.Header.Get(csrfHeader)), []byte(c.Value))
}

// resolveGroups reads the user's Linux groups live from the OS, with a shell fallback
// for NSS sources the pure-Go os/user path may miss. The bool reports whether the
// account exists at all (false => unknown account => unauthenticated).
func resolveGroups(username string) ([]string, bool) {
	exists := false
	var groups []string
	if u, err := user.Lookup(username); err == nil {
		exists = true
		if gids, err := u.GroupIds(); err == nil {
			for _, gid := range gids {
				if g, err := user.LookupGroupId(gid); err == nil {
					groups = append(groups, g.Name)
				}
			}
		}
	}
	if len(groups) == 0 {
		if out, err := exec.Command("id", "-nG", username).Output(); err == nil {
			groups = strings.Fields(string(out))
			exists = true
		}
	}
	return groups, exists
}

// UserExists reports whether a Linux account with this name exists — used to validate the
// on-behalf-of target of a machine-to-machine emit (parity with maild's internal-send).
func UserExists(username string) bool {
	_, exists := resolveGroups(username)
	return exists
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
