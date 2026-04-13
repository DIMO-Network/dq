package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/99designs/gqlgen/graphql"
)

const didArg = "did"

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

// NewVehicleTokenCheck returns a directive handler that verifies the "did" query argument
// matches the Asset DID string in the JWT claim. No contract address or chain ID needed.
func NewVehicleTokenCheck() func(context.Context, any, graphql.Resolver) (any, error) {
	return func(ctx context.Context, _ any, next graphql.Resolver) (any, error) {
		requestedDID, err := getArg[string](ctx, didArg)
		if err != nil {
			return nil, UnauthorizedError{err: err}
		}
		claim, err := getDQClaim(ctx)
		if err != nil {
			return nil, UnauthorizedError{err: err}
		}
		if claim.Asset != requestedDID {
			return nil, newError("DID in query does not match token claim")
		}
		return next(ctx)
	}
}

// AllOfPrivilegeCheck verifies the claim includes ALL of the required privilege strings.
func AllOfPrivilegeCheck(ctx context.Context, _ any, next graphql.Resolver, requiredPrivs []string) (any, error) {
	claim, err := getDQClaim(ctx)
	if err != nil {
		return nil, UnauthorizedError{err: err}
	}
	for _, priv := range requiredPrivs {
		if !slices.Contains(claim.Permissions, priv) {
			return nil, newError("missing required privilege %s", priv)
		}
	}
	return next(ctx)
}

// OneOfPrivilegeCheck verifies the claim includes AT LEAST ONE of the required privilege strings.
func OneOfPrivilegeCheck(ctx context.Context, _ any, next graphql.Resolver, requiredPrivs []string) (any, error) {
	claim, err := getDQClaim(ctx)
	if err != nil {
		return nil, UnauthorizedError{err: err}
	}
	for _, priv := range requiredPrivs {
		if slices.Contains(claim.Permissions, priv) {
			return next(ctx)
		}
	}
	return nil, newError("requires at least one of the following privileges %v", requiredPrivs)
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
