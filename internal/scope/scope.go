// Package scope answers permission questions against a dauth permission
// token, including permissions granted under constraints (the
// scoped_permissions claim).
//
// The claim encoding is fail-closed by construction: a permission granted
// under constraints appears ONLY in scoped_permissions, never in the flat
// permissions array, so code that reads only the flat claim refuses scoped
// grants outright. This package is the one place that deliberately opens
// scoped grants back up — and only alongside their constraints. Every helper
// here treats a constraint it cannot interpret as an absent grant.
package scope

import (
	"fmt"
	"time"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
)

// constraintsFor returns the constraint atoms under which tok holds the named
// permission: (nil, true) for an unconditional grant, (atoms, true) for a
// scoped one, (nil, false) when the permission is not held at all.
func constraintsFor(tok *tokenclaims.Token, name string) ([]tokenclaims.Constraint, bool) {
	if tok == nil {
		return nil, false
	}
	for _, p := range tok.Permissions {
		if p == name {
			return nil, true
		}
	}
	for _, sp := range tok.ScopedPermissions {
		if sp.Name == name {
			return sp.Constraint, true
		}
	}
	return nil, false
}

// Holds reports whether tok holds the named permission at all, conditionally
// or not. Use it for possession checks whose data access is separately
// window-checked; a surface with no window enforcement must use Unscoped
// instead.
func Holds(tok *tokenclaims.Token, name string) bool {
	_, ok := constraintsFor(tok, name)
	return ok
}

// Unscoped reports whether tok holds the named permission unconditionally.
// Surfaces that cannot enforce constraints must gate on this.
func Unscoped(tok *tokenclaims.Token, name string) bool {
	cs, ok := constraintsFor(tok, name)
	return ok && len(cs) == 0
}

// AllowsRange reports whether tok holds the named permission for every
// instant in the half-open interval [from, to). Unconditional grants allow
// any range; a grant whose constraints cannot be interpreted allows nothing.
func AllowsRange(tok *tokenclaims.Token, name string, from, to time.Time) bool {
	cs, ok := constraintsFor(tok, name)
	if !ok {
		return false
	}
	allowed, err := tokenclaims.AllowsInterval(cs, from, to)
	return err == nil && allowed
}

// AllowsAt reports whether tok holds the named permission for data recorded
// at the instant t.
func AllowsAt(tok *tokenclaims.Token, name string, t time.Time) bool {
	cs, ok := constraintsFor(tok, name)
	if !ok {
		return false
	}
	allowed, err := tokenclaims.AllowsAt(cs, t)
	return err == nil && allowed
}

// Describe renders the constraints under which tok holds the named permission,
// for error messages ("granted for 2026-04-01T00:00:00Z <= recordedAt < ...").
// Empty for permissions held unconditionally or not at all.
func Describe(tok *tokenclaims.Token, name string) string {
	cs, ok := constraintsFor(tok, name)
	if !ok || len(cs) == 0 {
		return ""
	}
	lower, upper, err := tokenclaims.RecordedAtWindow(cs)
	if err != nil {
		return "granted under constraints this service cannot interpret"
	}
	s := "granted for data recorded"
	if lower != nil {
		op := ">"
		if lower.Inclusive {
			op = ">="
		}
		s += fmt.Sprintf(" %s %s", op, lower.Time.Format(time.RFC3339))
	}
	if lower != nil && upper != nil {
		s += " and"
	}
	if upper != nil {
		op := "<"
		if upper.Inclusive {
			op = "<="
		}
		s += fmt.Sprintf(" %s %s", op, upper.Time.Format(time.RFC3339))
	}
	return s
}
