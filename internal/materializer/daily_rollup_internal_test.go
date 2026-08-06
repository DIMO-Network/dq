package materializer

import (
	"testing"
	"time"
)

// TestDailyRefreshBoundary pins the due-ness math: boundaries are UTC
// midnights, become due only `delay` after they pass, and a behind watermark
// (missed days) still targets the single most recent settled boundary — the
// fold covers the whole gap in one window.
func TestDailyRefreshBoundary(t *testing.T) {
	day := func(d int, hhmm string) time.Time {
		base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d)
		hm, err := time.Parse("15:04", hhmm)
		if err != nil {
			t.Fatalf("bad hhmm %q: %v", hhmm, err)
		}
		return base.Add(time.Duration(hm.Hour())*time.Hour + time.Duration(hm.Minute())*time.Minute)
	}
	delay := 3*time.Hour + 30*time.Minute
	cases := []struct {
		name      string
		now       time.Time
		watermark time.Time
		want      time.Time
	}{
		{"inside the settle delay nothing is due", day(1, "02:00"), day(0, "00:00"), time.Time{}},
		{"past the delay the rollover is due", day(1, "03:30"), day(0, "00:00"), day(1, "00:00")},
		{"evening, already refreshed today", day(1, "20:00"), day(1, "00:00"), time.Time{}},
		{"no watermark seeds to the settled boundary", day(1, "04:00"), time.Time{}, day(1, "00:00")},
		{"no watermark inside the delay seeds to yesterday", day(1, "01:00"), time.Time{}, day(0, "00:00")},
		{"missed days target the latest settled boundary", day(3, "04:00"), day(0, "00:00"), day(3, "00:00")},
		{"watermark ahead of the boundary is not due", day(1, "01:00"), day(1, "00:00"), time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := dailyRefreshBoundary(tc.now, tc.watermark, delay)
			if !got.Equal(tc.want) {
				t.Fatalf("dailyRefreshBoundary(%s, %s) = %s, want %s", tc.now, tc.watermark, got, tc.want)
			}
		})
	}
}

func TestParseDailyRollupMode(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want DailyRollupMode
		ok   bool
	}{
		{"", DailyRollupOff, true},
		{"off", DailyRollupOff, true},
		{"shadow", DailyRollupShadow, true},
		{"on", DailyRollupOff, false}, // step 4; must not parse yet
		{"bogus", DailyRollupOff, false},
	} {
		got, ok := ParseDailyRollupMode(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Errorf("ParseDailyRollupMode(%q) = (%v, %v), want (%v, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}
