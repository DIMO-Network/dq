// Package coverage carries, through a request, the data-timestamp windows the
// access token allows for the subject being read (design spec §11.4), so that
// every layer that reads data can hold to them: the directives clamp a
// requested range, but reads with no range (latest values, snapshots,
// summaries) and reads that look past their range (trip detection, location
// gap-fill) need the windows themselves.
package coverage

import (
	"context"
	"time"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
)

type ctxKey struct{}

// With returns ctx carrying the windows the token holds for the subject. A nil
// Windows means no restriction: a caller that never set any.
func With(ctx context.Context, ws tokenclaims.Windows) context.Context {
	return context.WithValue(ctx, ctxKey{}, ws)
}

// Windows returns the windows in ctx, and whether any were set.
func Windows(ctx context.Context) (tokenclaims.Windows, bool) {
	ws, ok := ctx.Value(ctxKey{}).(tokenclaims.Windows)
	return ws, ok
}

// Contains reports whether data timestamped t may be read. With no windows in
// ctx everything may.
func Contains(ctx context.Context, t time.Time) bool {
	ws, ok := Windows(ctx)
	if !ok {
		return true
	}
	return ws.Contains(t)
}

// Unbounded reports whether ctx allows every data timestamp.
func Unbounded(ctx context.Context) bool {
	ws, ok := Windows(ctx)
	return !ok || (len(ws) == 1 && ws[0].Start == nil && ws[0].End == nil)
}

// Bounds returns the window containing t, as the earliest and latest data
// timestamps a read around t may reach: nil ends are open. ok is false when t
// is outside every window. With no windows in ctx, both ends are open.
func Bounds(ctx context.Context, t time.Time) (floor, ceiling *time.Time, ok bool) {
	ws, set := Windows(ctx)
	if !set {
		return nil, nil, true
	}
	for _, w := range ws {
		if (w.Start == nil || !t.Before(*w.Start)) && (w.End == nil || t.Before(*w.End)) {
			return w.Start, w.End, true
		}
	}
	return nil, nil, false
}
