# M1 bridge — 256 MiB large-request work: report

Clone `/var/tmp/bridge-m1-20261104`, branch `m1-256mib`, base `b482f71`. Local only: no push, no tag.
Spec: `large-request-capacity` (design D1, D3, D4, D5 "Bridge", D9; tasks 2.1).
This revision answers the review of `27fd5d8` (`review-logs/REVIEW.md`, CHANGES_REQUESTED).

## Commits (b482f71..HEAD)

| commit | subject |
|---|---|
| 90f0b32 | fix(bridge): explicit error event before every failure close (D9) |
| b15c988 | feat(bridge): 256 MiB cap with anonymous disk spools (D3) |
| a2cc586 | feat(bridge): D4 ticket lease and per-hop cgroup headroom refusal |
| e9bc45d | feat(bridge): count fragmented and inflated message bytes ourselves |
| caa2351 | fix(bridge): name the hop and code when the upstream request fails |
| c2d23b7, 07f7e70, fed0737, d702843 | test(bridge): conditions instead of timers; early upstream error |
| 27fd5d8 | docs(bridge): M1 report (reviewed revision) |
| 869358b | refactor(bridge): drop the soft cgroup headroom check (review M4) |
| ccd7bea | feat(bridge): split the work budget into memory and spool disk (review M5) |
| b244208 | fix(bridge): keep reading the socket after a reader refusal (review B2, m2) |
| e3280da | fix(bridge): lease the ticket before the first spooled byte (review B1, M3) |
| 756d060 | fix(bridge): drop spool pages on every read-side pass (review M1) |
| 0606396 | fix(bridge): review minors m1, m3-m6, m8 |

## Review disposition

| finding | status | what changed |
|---|---|---|
| B1 lease vs spool lifetime | **fixed** | Contract: keeper writes `ai_gateway_capacity_ticket` as the **first** frame member. The reader parses it from a 512-byte in-memory prefix and takes the blocking `LOCK_SH` (`O_RDONLY`, `st_nlink > 0`) before it creates the spool. The lease rides on the queued message and drops after the spool closes. The worker's full scan must find the same ticket; a ticket that is not first is `capacity_ticket_invalid`. A frame without a usable ticket is refused as it crosses 64 MiB, never spooled in full. Every session-ending path frees the queued spools before the error event and the close frame. |
| B2 close not observed after a reader refusal | **fixed** | After a refusal (or a closed FIFO) the reader drains the message and later ones into a 4 KiB buffer, without spooling, and calls `dead()` on a read error. The library read limit is off (`-1`); the bridge counts every message itself. |
| M1 read-side page cache | **fixed in code, not measured** | Frame scan, event walks, flat-error copy, upstream-error walk and prefix all read through `cacheDroppingReader` (DONTNEED every 8 MiB and at EOF). Measuring it needs ext4: see M2. |
| M2 acceptance on ZFS | **open: M3 CI acceptance item** | Local `/var/tmp` is ZFS (`file_max = 0` in every run, `fadvise` a no-op). CI must re-run 16/64/256 MiB with the spool on (loop-mounted) ext4, record `file` from `memory.stat`, and pass `memory.peak ≤ 256m − 32 MiB`. Not faked here. |
| M3 ticket guard fails open | **fixed** | With `CAPACITY_LEDGER_DIR` unset, 64 MiB or more is refused: `capacity_ticket_missing` without a ticket, `capacity_ticket_invalid` ("CAPACITY_LEDGER_DIR is not configured") with one. WS frames, `Content-Length` bodies and chunked bodies alike. |
| M4 soft cgroup check | **fixed (removed)** | `cgroup.go`, `memHeadroom` and `errInsufficientMemory` are gone. |
| M5 one budget for disk and memory | **fixed** | `WORK_MAX_BYTES` is now the memory budget only (default 64 MiB: all windows, scans, inflater, session, queue metadata). `SPOOL_MAX_BYTES` is the spool disk budget (default 4 × (cap + body)). Disk exhaustion is 503 `spool_budget_exhausted`. |
| m1 upstream error write hangs | **fixed** | Failures after `conn.Writer` opened are `errMidMessage`: close only. |
| m2 panic path | **partly fixed** | The active turn's spool and leases are released in a deferred call, so a panic frees them. **Open:** a panic still ends with a bare close and no error event; writing one could block on a writer held by the panicking frame. |
| m3 HTTP hop / chunked bodies | **fixed** | HTTP error bodies carry `hop` (and `limit`). A chunked native body without a lease fails its upload as it crosses 64 MiB, answered 503 `capacity_ticket_missing`. |
| m4 events without code/hop | **fixed** | `unsupported_frame_type`, `invalid_json`, `json_too_deep`, `unsupported_event_type`, `frame_unreadable`, `upstream_error_too_large`, `upstream_error_truncated`, each with the hop. |
| m5 limit only in the text | **fixed** | `request_too_large` (and `upstream_error_too_large`) carry `limit`. |
| m6 README stale | **fixed** | Defaults, both budgets, spool dir rules, ledger, ticket contract, error codes. |
| m7 1777 spool volume | **open: deployment task** | No code change. In the M3 window, recreate the named volume `responses-ws-sse-bridge-spool` (AI-gateway `compose.yaml`), or `chmod 0700` it, before the new image starts; `checkSpoolDir` refuses 1777. |
| m8 `DISK_RESERVE_BYTES=0` | **fixed** | The loader rejects a non-positive reserve. |
| m9 hygiene | **open** | `a2cc586` still mixes the lease with the (now removed) cgroup check; history is not rewritten. `report.md` stays at the root because the brief asks for it here; drop it before any upstream PR. |
| m10 tests hang instead of failing | **open, by decision** | Tests wait on conditions; `go test -timeout` is the only guard (parent decision). |

## Tests

- `go vet ./...` and `gofmt -l .`: clean.
- `go test -count=1 ./...`: **106 top-level tests pass, 34 subtests pass, 1 skip** (`TestMemLoad`, opt-in), 0 fail.
- `go test -count=1 -race ./...`: the same 106/34/1, 0 fail, 0 `DATA RACE`.
- Stress: `-race -count=20` over `TestM1*`, `TestReaderRefusal*`, `TestRegressionQueued*`, `TestClientClose*`: pass.
- New or repurposed tests (every wait is on a condition; a leak hangs until `go test -timeout`):
  - `TestM1QueuedTicketedTurnLeasedUntilFreed`: the reviewer's probe. A ticketed turn 2 is queued while turn 1
    is in flight; ticket B is not `LOCK_EX`-able while it is queued. Turn 1 fails (its next event cannot be
    charged); a raw client that never answers the close frame then sees 0 open spools and ticket B free at the
    moment the close frame arrives. Red when `incoming.close()` follows `failTurn`.
  - `TestReaderRefusalThenClientCloseReleasesTurn` (repurposes the queue-full test): a refusal while the worker
    is blocked in a real transport write, then a client TCP close. It waits for the reader to enter
    `drainSocket`, then for the handler and the upstream to end, and checks 0 memory, 0 disk, 0 spools. Red
    (hang) when the reader stops reading.
  - `TestM1TicketLeasedBeforeSpooling`: while keeper holds `LOCK_EX`, the reader waits in `flock` with 0 spools
    open. Red on `27fd5d8`.
  - `TestM1UnticketedFrameRefusedAt64MiB`: no ticket, and ticket without a ledger; the bridge reads less than
    64 MiB + one read window before the refusal. Red on `27fd5d8`.
  - `TestM1TicketNotFirstMemberRefused`, `TestM1NativeChunkedUnticketedRefusedAt64MiB`,
    `TestM1LocalEventsCarryCodeHopLimit`, `TestM1ZeroDiskReserveRejected`.
  - Removed with M4: `TestM1CgroupHeadroomRefusal`, `TestM1CgroupRefusalOnTurnIsExplicit`.
- Production timers: only the base `healthcheck` client `Timeout: 2s` and `ReadHeaderTimeout: 10s`, both
  unchanged. No timer, expiry, count cap, `TryLock`/`LOCK_NB` or global lock is added; both budgets are CAS byte
  budgets.

## Local acceptance (M0 harness, re-run on 0606396)

**The spool was on ZFS** (`stat -f /var/tmp` = `zfs`): `file_max` is 0 in every run, so these numbers show
that anonymous memory is flat. They do not prove the D3 page-cache bound (M2: M3 CI item on ext4).

Same runner as before (`/var/tmp/bridge-m1-work/acc.py`, `m0.start`/`m0.one`, a fresh container per request,
50 ms sampler, `mockslow` at 64 MiB/s), binary built from this clone with the Dockerfile flags (local go1.27.3).
Two harness changes for the new contract, both outside the repo:
- `mkframe.py` now writes the ticket as the first member; the `f-*` frames were regenerated.
- `wsclient` is a copy that sends a frame that leads with the ticket member verbatim (the M0 client prepends
  `"type":"response.create"` to any frame that does not start with it); `acc.py` sets `m0.W` to use it.

| config | body (frame bytes) | result | peak MiB (idle) | anon max MiB | disk peak | disk after / open spool fds |
|---|---|---|---|---|---|---|
| m1-default-ws-256m | f-image-16 (16777216) | success | 14.9 (6.0) | 4.4 | 1.0x | 0 / 0 |
| m1-default-ws-256m | f-text-16 (16777216) | success | 16.1 (5.7) | 6.4 | 1.0x | 0 / 0 |
| m1-default-ws-256m | f-image-64 (67108864) | success | 17.3 (5.4) | 6.4 | 1.0x | 0 / 0 |
| m1-default-ws-256m | f-text-64 (67108864) | success | 17.5 (5.9) | 6.4 | 1.0x | 0 / 0 |
| m1-default-ws-256m | f-image-256 (268435456) | success | 17.2 (6.2) | 6.4 | 1.0x | 0 / 0 |
| m1-default-ws-256m | f-text-256 (268435456) | success | 17.2 (6.4) | 6.4 | 1.0x | 0 / 0 |
| m1-default-ws-256m | image-256 (268435481, no ticket) | error-event capacity_ticket_missing | 9.6 (7.3) | 6.4 | 33456640 | 0 / 0 |
| m1-default-ws-256m | f-image-64-noticket (67108864) | error-event capacity_ticket_missing | 9.6 (8.2) | 6.4 | 31457792 | 0 / 0 |
| bridge-raised-ws-5g | image-16 (16777241) | success | 15.2 (6.2) | 6.4 | 1.0x | 0 / 0 |
| bridge-raised-ws-5g | text-16 (16777241) | success | 17.1 (6.2) | 6.4 | 1.0x | 0 / 0 |
| bridge-raised-ws-5g | image-64 (67108889) | error-event capacity_ticket_missing | 6.9 (6.2) | 4.4 | 24216064 | 0 / 0 |
| bridge-raised-ws-5g | text-64 (67108889) | error-event capacity_ticket_missing | 7.1 (5.7) | 4.4 | 58950144 | 0 / 0 |
| bridge-raised-ws-5g | image-256 (268435481) | error-event capacity_ticket_missing | 9.4 (8.2) | 6.4 | 58851840 | 0 / 0 |
| bridge-raised-ws-5g | text-256 (268435481) | error-event capacity_ticket_missing | 10.1 (8.2) | 6.4 | 59441664 | 0 / 0 |

- Ticketed frames of 16/64/256 MiB pass at the production 256m limit; peak 14.9–17.5 MiB, anon ≤ 6.4 MiB,
  disk 1.0x, upstream bytes = frame minus the `type` and ticket members.
- Unticketed frames of 64 MiB or more are refused as they cross 64 MiB: the sampled disk peak stays below
  64 MiB (24–63 MB, 50 ms sampling). `image-256` was `request_too_large` on `27fd5d8`; the ticket check now
  fires first, at 64 MiB.
- `bridge-raised-ws-5g` has no ledger, so with M3 it now fails closed at 64 MiB. Its 16 MiB runs still pass.
- After every run the spool dir is empty and no spool fd is open. No run ended in 1009 or a bare close.

## Deviations from the spec

1. **The spool is closed at turn end**, not before the response status (tasks 2.1, accepted 2026-11-04).
   With B1 fixed the precondition holds: the ticket lease covers the spool from before its first byte until
   after it closes. Invariant it depends on: no caller retries a ticketed turn through the bridge while the
   bridge's previous spool for that ticket is open (otherwise the bridge `k_disk` must be 2).
2. **Ticket position is a contract with keeper** (parent decision B1): the ticket must be the first member.
   D4 "Propagation" says only "top-level member"; the spec text needs the same amendment, and keeper must
   write it first.

## Residual risks / not checked

- M2: no ext4 measurement; the read-side page-cache bound (M1) is unproven until CI runs it.
- Not run behind keeper (`chain_ws.sh`) or in production; keeper's release waiter is a test `LOCK_EX`.
- The reader blocks in `LOCK_SH` while keeper holds `LOCK_EX`; during that wait it does not read the socket.
  The wait ends when keeper releases or unlinks the ticket (no timer by design).
- A chunked native body is refused at 64 MiB only if the upstream has not answered yet; an upstream that
  answers early sees a truncated upload.
- m2 (panic event), m7 (volume mode), m9 (history), m10 (hangs) remain open as listed above.
