package auth

import (
	"context"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	carA = "did:dimo:carA"
	carB = "did:dimo:carB"
)

func ts(s string) *time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return &t
}

// ctxFor builds a context with the token and a field context chain: an outer
// query field with the given args, and when child is set, a nested field
// under it with no args (a signal field under signals(...)).
func ctxFor(tok *tokenclaims.Token, args map[string]any, child bool) (context.Context, *graphql.FieldContext) {
	ctx := context.Background()
	if tok != nil {
		ctx = context.WithValue(ctx, DQClaimContextKey{}, &DQClaim{Token: *tok})
	}
	root := &graphql.FieldContext{Args: args}
	ctx = graphql.WithFieldContext(ctx, root)
	if child {
		ctx = graphql.WithFieldContext(ctx, &graphql.FieldContext{Parent: root, Args: map[string]any{}})
	}
	return ctx, root
}

func pass(context.Context) (any, error) { return "ok", nil }

var token = &tokenclaims.Token{Grants: []tokenclaims.Grant{
	{Subject: carA, Abilities: []string{tokenclaims.AbilityTelemetryRead}, Windows: tokenclaims.Windows{
		{Start: ts("2026-01-01T00:00:00Z"), End: ts("2026-02-01T00:00:00Z")},
		{Start: ts("2026-03-01T00:00:00Z"), End: ts("2026-04-01T00:00:00Z")},
	}},
	{Subject: carA, Abilities: []string{tokenclaims.AbilityLocationPrecise}, Windows: tokenclaims.Windows{
		{Start: ts("2026-01-10T00:00:00Z"), End: ts("2026-01-20T00:00:00Z")},
	}},
	{Subject: carA, Abilities: []string{tokenclaims.AbilityCommandUnlock}},
}}

func TestVehicleTokenCheck(t *testing.T) {
	c := &Checker{}

	t.Run("no token", func(t *testing.T) {
		ctx, _ := ctxFor(nil, map[string]any{"subject": carA}, false)
		_, err := c.VehicleTokenCheck(ctx, nil, pass)
		assert.Error(t, err)
	})
	t.Run("other subject", func(t *testing.T) {
		ctx, _ := ctxFor(token, map[string]any{"subject": carB}, false)
		_, err := c.VehicleTokenCheck(ctx, nil, pass)
		assert.ErrorContains(t, err, "does not cover")
	})
	t.Run("no range: passes", func(t *testing.T) {
		ctx, _ := ctxFor(token, map[string]any{"subject": carA}, false)
		out, err := c.VehicleTokenCheck(ctx, nil, pass)
		require.NoError(t, err)
		assert.Equal(t, "ok", out)
	})
	t.Run("range inside coverage is untouched", func(t *testing.T) {
		ctx, root := ctxFor(token, map[string]any{"subject": carA, "from": *ts("2026-01-05T00:00:00Z"), "to": *ts("2026-01-25T00:00:00Z")}, false)
		_, err := c.VehicleTokenCheck(ctx, nil, pass)
		require.NoError(t, err)
		assert.Equal(t, *ts("2026-01-05T00:00:00Z"), root.Args["from"])
		assert.Equal(t, *ts("2026-01-25T00:00:00Z"), root.Args["to"])
	})
	t.Run("range is clamped to the union of windows", func(t *testing.T) {
		ctx, root := ctxFor(token, map[string]any{"subject": carA, "from": *ts("2025-12-01T00:00:00Z"), "to": *ts("2026-01-25T00:00:00Z")}, false)
		_, err := c.VehicleTokenCheck(ctx, nil, pass)
		require.NoError(t, err)
		assert.Equal(t, *ts("2026-01-01T00:00:00Z"), root.Args["from"])
		assert.Equal(t, *ts("2026-01-25T00:00:00Z"), root.Args["to"])
	})
	t.Run("range with nothing covered", func(t *testing.T) {
		ctx, _ := ctxFor(token, map[string]any{"subject": carA, "from": *ts("2026-02-01T00:00:00Z"), "to": *ts("2026-03-01T00:00:00Z")}, false)
		_, err := c.VehicleTokenCheck(ctx, nil, pass)
		assert.ErrorContains(t, err, "covers nothing")
	})
	t.Run("range straddling a gap", func(t *testing.T) {
		ctx, _ := ctxFor(token, map[string]any{"subject": carA, "from": *ts("2026-01-01T00:00:00Z"), "to": *ts("2026-04-01T00:00:00Z")}, false)
		_, err := c.VehicleTokenCheck(ctx, nil, pass)
		assert.ErrorContains(t, err, "straddles a gap")
		assert.ErrorContains(t, err, "[2026-01-01T00:00:00Z, 2026-02-01T00:00:00Z) [2026-03-01T00:00:00Z, 2026-04-01T00:00:00Z)")
	})
	t.Run("only live abilities: no clamp", func(t *testing.T) {
		live := &tokenclaims.Token{Grants: []tokenclaims.Grant{{Subject: carA, Abilities: []string{tokenclaims.AbilityCommandUnlock}}}}
		ctx, root := ctxFor(live, map[string]any{"subject": carA, "from": *ts("2025-01-01T00:00:00Z"), "to": *ts("2027-01-01T00:00:00Z")}, false)
		_, err := c.VehicleTokenCheck(ctx, nil, pass)
		require.NoError(t, err)
		assert.Equal(t, *ts("2025-01-01T00:00:00Z"), root.Args["from"])
	})
}

func TestAbilityChecks(t *testing.T) {
	now := *ts("2026-01-15T00:00:00Z")
	c := &Checker{Now: func() time.Time { return now }}
	telemetry := []string{tokenclaims.AbilityTelemetryRead}
	location := []string{tokenclaims.AbilityLocationPrecise}

	t.Run("held over the range, on a nested field", func(t *testing.T) {
		ctx, _ := ctxFor(token, map[string]any{"subject": carA, "from": *ts("2026-01-05T00:00:00Z"), "to": *ts("2026-01-25T00:00:00Z")}, true)
		_, err := c.AllOfPrivilegeCheck(ctx, nil, pass, telemetry)
		assert.NoError(t, err)
	})
	t.Run("not held", func(t *testing.T) {
		ctx, _ := ctxFor(token, map[string]any{"subject": carA}, true)
		_, err := c.AllOfPrivilegeCheck(ctx, nil, pass, []string{tokenclaims.AbilityRawRead})
		assert.ErrorContains(t, err, "does not hold raw:read")
	})
	t.Run("held but not over the whole range", func(t *testing.T) {
		ctx, _ := ctxFor(token, map[string]any{"subject": carA, "from": *ts("2026-01-05T00:00:00Z"), "to": *ts("2026-01-25T00:00:00Z")}, true)
		_, err := c.AllOfPrivilegeCheck(ctx, nil, pass, location)
		assert.ErrorContains(t, err, "covers only [2026-01-10T00:00:00Z, 2026-01-20T00:00:00Z)")
	})
	t.Run("all of: every ability must pass", func(t *testing.T) {
		ctx, _ := ctxFor(token, map[string]any{"subject": carA, "from": *ts("2026-01-12T00:00:00Z"), "to": *ts("2026-01-18T00:00:00Z")}, true)
		_, err := c.AllOfPrivilegeCheck(ctx, nil, pass, append(telemetry, location...))
		assert.NoError(t, err)
		_, err = c.AllOfPrivilegeCheck(ctx, nil, pass, append(telemetry, tokenclaims.AbilityRawRead))
		assert.Error(t, err)
	})
	t.Run("one of: any passing ability suffices", func(t *testing.T) {
		ctx, _ := ctxFor(token, map[string]any{"subject": carA, "from": *ts("2026-01-05T00:00:00Z"), "to": *ts("2026-01-25T00:00:00Z")}, true)
		_, err := c.OneOfPrivilegeCheck(ctx, nil, pass, []string{tokenclaims.AbilityLocationApproximate, tokenclaims.AbilityLocationPrecise, tokenclaims.AbilityTelemetryRead})
		assert.NoError(t, err)
		_, err = c.OneOfPrivilegeCheck(ctx, nil, pass, []string{tokenclaims.AbilityLocationApproximate, tokenclaims.AbilityLocationPrecise})
		assert.ErrorContains(t, err, "requires one of")
	})
	t.Run("no range: the current time must be covered", func(t *testing.T) {
		ctx, _ := ctxFor(token, map[string]any{"subject": carA}, true)
		_, err := c.AllOfPrivilegeCheck(ctx, nil, pass, telemetry)
		assert.NoError(t, err)
		later := &Checker{Now: func() time.Time { return *ts("2026-02-15T00:00:00Z") }}
		_, err = later.AllOfPrivilegeCheck(ctx, nil, pass, telemetry)
		assert.ErrorContains(t, err, "not covered at the current time")
	})
	t.Run("live ability has no time constraint", func(t *testing.T) {
		ctx, _ := ctxFor(token, map[string]any{"subject": carA, "from": *ts("2020-01-01T00:00:00Z"), "to": *ts("2030-01-01T00:00:00Z")}, true)
		_, err := c.AllOfPrivilegeCheck(ctx, nil, pass, []string{tokenclaims.AbilityCommandUnlock})
		assert.NoError(t, err)
	})
	t.Run("no subject in scope", func(t *testing.T) {
		ctx, _ := ctxFor(token, map[string]any{}, true)
		_, err := c.AllOfPrivilegeCheck(ctx, nil, pass, telemetry)
		assert.ErrorContains(t, err, "no subject in scope")
	})
}
