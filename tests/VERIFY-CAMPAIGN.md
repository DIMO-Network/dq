# Verification campaign — #1c pagination + #5b incremental rollup (2026-07-07)

10 adversarial loops. Contract every loop: incremental `signals_latest` == full `RecomputeRollup`, and base rows exactly-once. Result: **10/10 PASS, zero product defects.**
(Loop 1's first run failed on a harness assumption — location signals are not NULL `value_number` — the differential itself passed; product correct.)

| # | Vector | Where |
|---|--------|-------|
| 1 | location signal (value_number/loc fold) | TestVerify01 |
| 2 | intermittent location — loc_ts stays newest despite later older fix | TestVerify02 |
| 3 | fat single snapshot, many (subject,name) | TestVerify03 |
| 4 | pagination driven by the BYTE budget (row cap off) | TestVerify04 |
| 5 | crash at the LAST intermediate window → restart exact | TestVerify05 |
| 6 | span mixing decodable + non-decodable (wrong-chain) rows | TestVerify06 |
| 7 | 100-day out-of-order arrival | TestVerify07 |
| 8 | RecomputeRollup interleaved with incremental folds (self-healing) | TestVerify08 |
| 9 | THREE concurrent writers, paginated fat snapshot (real PG) | TestVerify09_PG |
| 10 | one writer crashes mid-span + supervisor restart, two race it (real PG) | TestVerify10_PG |

Embedded loops run in `go test ./tests/`; PG loops need `PG_CATALOG_DSN`. `-race` clean.
