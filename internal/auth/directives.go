package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
)

const (
	subjectArg = "subject"
	fromArg    = "from"
	toArg      = "to"
)

// UnauthorizedError is returned by directive handlers when a request is not authorized.
type UnauthorizedError struct {
	message string
	err     error
}

func (e UnauthorizedError) Error() string {
	if e.message != "" {
		if e.err != nil {
			return fmt.Sprintf("unauthorized: %s: %s", e.message, e.err)
		}
		return fmt.Sprintf("unauthorized: %s", e.message)
	}
	if e.err != nil {
		return fmt.Sprintf("unauthorized: %s", e.err)
	}
	return "unauthorized"
}

func (e UnauthorizedError) Unwrap() error {
	return e.err
}

func newError(msg string, args ...any) error {
	return UnauthorizedError{message: fmt.Sprintf(msg, args...)}
}

// Checker holds the directive handlers. Now is the clock a field with no time
// range in scope is checked at; nil means time.Now.
type Checker struct {
	Now func() time.Time
}

func (c *Checker) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// VehicleTokenCheck is @requiresVehicleToken: the token must hold a grant for
// the query's subject, and on a field with a from/to range the range is
// clamped to the union of the windows the token holds for that subject
// (spec §11.4). A range with nothing covered is refused, and a range that
// straddles a gap in coverage is refused naming the pieces, so the caller
// splits the query rather than dq interpolating across the gap.
func (c *Checker) VehicleTokenCheck(ctx context.Context, _ any, next graphql.Resolver) (any, error) {
	subject, err := getArg[string](ctx, subjectArg)
	if err != nil {
		return nil, UnauthorizedError{err: err}
	}
	claim, err := getDQClaim(ctx)
	if err != nil {
		return nil, UnauthorizedError{err: err}
	}
	if !claim.Covers(subject) {
		return nil, newError("token does not cover subject %s", subject)
	}

	fc := graphql.GetFieldContext(ctx)
	from, fromOK := fc.Args[fromArg].(time.Time)
	to, toOK := fc.Args[toArg].(time.Time)
	if fromOK && toOK {
		if ws := windowedCoverage(&claim.Token, subject); ws != nil {
			pieces := ws.Clamp(from, to)
			switch len(pieces) {
			case 0:
				return nil, newError("token covers nothing for %s between %s and %s", subject, from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339))
			case 1:
				fc.Args[fromArg] = *pieces[0].Start
				fc.Args[toArg] = *pieces[0].End
			default:
				return nil, newError("requested range straddles a gap in coverage; query these ranges separately: %s", describe(pieces))
			}
		}
	}
	return next(ctx)
}

// AllOfPrivilegeCheck is @requiresAllOfPrivileges: the token must hold every
// ability for the subject in scope, over the whole time range in scope.
func (c *Checker) AllOfPrivilegeCheck(ctx context.Context, _ any, next graphql.Resolver, abilities []string) (any, error) {
	claim, subject, err := claimAndSubject(ctx)
	if err != nil {
		return nil, err
	}
	for _, ability := range abilities {
		if err := c.check(ctx, &claim.Token, subject, ability); err != nil {
			return nil, err
		}
	}
	return next(ctx)
}

// OneOfPrivilegeCheck is @requiresOneOfPrivilege: at least one of the
// abilities must pass AllOfPrivilegeCheck's test.
func (c *Checker) OneOfPrivilegeCheck(ctx context.Context, _ any, next graphql.Resolver, abilities []string) (any, error) {
	claim, subject, err := claimAndSubject(ctx)
	if err != nil {
		return nil, err
	}
	var errs []string
	for _, ability := range abilities {
		err := c.check(ctx, &claim.Token, subject, ability)
		if err == nil {
			return next(ctx)
		}
		errs = append(errs, err.Error())
	}
	return nil, newError("requires one of %v: %s", abilities, strings.Join(errs, "; "))
}

func claimAndSubject(ctx context.Context) (*DQClaim, string, error) {
	claim, err := getDQClaim(ctx)
	if err != nil {
		return nil, "", UnauthorizedError{err: err}
	}
	subject, ok := subjectInScope(ctx)
	if !ok {
		return nil, "", newError("no subject in scope for the ability check")
	}
	return claim, subject, nil
}

// check is the per-ability rule: held for the subject, and if the ability
// carries windows, either the range in scope lies within them (after the
// vehicle check clamped it) or, with no range in scope, the current time does.
func (c *Checker) check(ctx context.Context, tok *tokenclaims.Token, subject, ability string) error {
	ws, ok := tok.Holds(subject, ability)
	if !ok {
		return newError("token does not hold %s for %s", ability, subject)
	}
	if ws == nil {
		return nil
	}
	if from, to, ranged := rangeInScope(ctx); ranged {
		pieces := ws.Clamp(from, to)
		if len(pieces) != 1 || !pieces[0].Start.Equal(from) || !pieces[0].End.Equal(to) {
			return newError("%s for %s covers only %s within the requested range", ability, subject, describe(pieces))
		}
		return nil
	}
	if !ws.Contains(c.now()) {
		return newError("%s for %s is not covered at the current time", ability, subject)
	}
	return nil
}

// windowedCoverage unions the windows of every windowed ability the token
// holds for subject; nil when it holds none, which leaves nothing to clamp by.
func windowedCoverage(tok *tokenclaims.Token, subject string) tokenclaims.Windows {
	var abilities []string
	for _, g := range tok.Grants {
		if g.Subject == subject && g.Windows != nil {
			abilities = append(abilities, g.Abilities...)
		}
	}
	if len(abilities) == 0 {
		return nil
	}
	var ws tokenclaims.Windows
	for _, a := range abilities {
		w, _ := tok.Holds(subject, a)
		if w == nil {
			// Held windowed and unwindowed at once: nothing to clamp by.
			return nil
		}
		ws = append(ws, w...)
	}
	// Holds normalises what it returns; re-normalise the union through a
	// throwaway token so the pieces are sorted and merged.
	union := tokenclaims.Token{Grants: []tokenclaims.Grant{{Subject: subject, Abilities: []string{"union"}, Windows: ws}}}
	merged, _ := union.Holds(subject, "union")
	return merged
}

// subjectInScope finds the subject argument on the field or an ancestor, so
// a directive on a signal field under signals(subject: ...) knows the vehicle.
func subjectInScope(ctx context.Context) (string, bool) {
	for fc := graphql.GetFieldContext(ctx); fc != nil; fc = fc.Parent {
		if s, ok := fc.Args[subjectArg].(string); ok {
			return s, true
		}
	}
	return "", false
}

// rangeInScope finds from/to on the field or an ancestor, as the vehicle
// check left them.
func rangeInScope(ctx context.Context) (time.Time, time.Time, bool) {
	for fc := graphql.GetFieldContext(ctx); fc != nil; fc = fc.Parent {
		from, fromOK := fc.Args[fromArg].(time.Time)
		to, toOK := fc.Args[toArg].(time.Time)
		if fromOK && toOK {
			return from, to, true
		}
	}
	return time.Time{}, time.Time{}, false
}

func describe(ws tokenclaims.Windows) string {
	if len(ws) == 0 {
		return "nothing"
	}
	parts := make([]string, 0, len(ws))
	for _, w := range ws {
		start, end := "-", "-"
		if w.Start != nil {
			start = w.Start.UTC().Format(time.RFC3339)
		}
		if w.End != nil {
			end = w.End.UTC().Format(time.RFC3339)
		}
		parts = append(parts, "["+start+", "+end+")")
	}
	return strings.Join(parts, " ")
}

func getArg[T any](ctx context.Context, name string) (T, error) {
	var resp T
	fCtx := graphql.GetFieldContext(ctx)
	if fCtx == nil {
		return resp, errors.New("no field context found")
	}
	val, ok := fCtx.Args[name]
	if !ok {
		return resp, fmt.Errorf("no argument named %s", name)
	}
	resp, ok = val.(T)
	if !ok {
		return resp, fmt.Errorf("argument %s had type %T instead of expected %T", name, val, resp)
	}
	return resp, nil
}
