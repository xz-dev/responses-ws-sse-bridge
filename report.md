# M1 bridge — 256 MiB large-request work: report

Clone `/var/tmp/bridge-m1-20261104`, branch `m1-256mib`, base `b482f71`. Local only: no push, no tag.
Spec: `large-request-capacity` (design D1, D3, D4, D5 "Bridge", D9; tasks 2.1).

## Commits (b482f71..HEAD)

| commit | subject |
|---|---|
| 90f0b32 | fix(bridge): explicit error event before every failure close (D9) |
| b15c988 | feat(bridge): 256 MiB cap with anonymous disk spools (D3) |
| a2cc586 | feat(bridge): D4 ticket lease and per-hop cgroup headroom refusal |
| e9bc45d | feat(bridge): count fragmented and inflated message bytes ourselves |
| caa2351 | fix(bridge): name the hop and code when the upstream request fails |
| c2d23b7 | test(bridge): M1 tests wait on conditions, not timers |
| 07f7e70 | test(bridge): wait on conditions, not short deadlines |
| fed0737 | test(bridge): long-turn test waits on the work counter, not a deadline |
| d702843 | test(bridge): upstream error before body read does not stall the turn |

The brief said `bridge.go` was uncommitted. It was not: the tree was clean, and the previous
worker's change was already committed as `caa2351`, which makes an unreachable upstream an explicit
`upstream_request_failed` event that names the hop. I reviewed it and kept it.

## Tests

- `go vet ./...`: clean. `gofmt -l .`: clean.
- `go test -count=1 ./...`: **101 top-level tests pass, 32 subtests pass, 1 skip**
  (`TestMemLoad`, which runs only with `MEMLOAD=1`), 0 fail.
- `go test -count=1 -race ./...`: the same 101/32/1, 0 fail, 0 `DATA RACE`.
- Stress: `-race -count=50 -run TestM1` (800 pass); `-race -count=20` over the edited lifetime, close,
  accounting and lease tests (520 pass).

### Timer dependence in tests (item 3)

Rule applied: a test must not pass or fail because a clock ran out. If it waits for something, it
blocks on a condition: a channel, a handler exit, a blocking `flock`, the work counter or the open
spool count. `go test -timeout` is the only hang guard.

- M1 tests: the poll with a 2 s deadline became a blocking `LOCK_EX`. The 50 ms sleep before the
  unlink became a check for a blocked flock waiter in `/proc/locks`. The read, dial and raw-conn
  deadlines that M1 added are gone.
- Base tests: these "must happen within X" checks became blocking waits:
  - `bridge_test` (cancel, session lease);
  - `p2_lifetime` (7 s timer);
  - `p2_streaming`;
  - `reader_error_cancel` (200 ms);
  - `regress` queued spool (the 1 s polls and the 20 ms sleep, now a handler-exit wait);
  - `transparency_accounting` (1 s).

  In `reader_error_cancel` I also removed a 50 ms goroutine-dump "observation". It asserted nothing,
  and the queue-full reader error it looked for never occurs on the current code, so a blocking
  version hung.
- Still present, by design:
  - context timeouts in the shared helpers (`dial`, `readEvent`, `writeFrame`) as I/O hang guards;
  - waits that prove there is no expiry: `controlled_write_red` holds 32 s, and the regress mocks
    delay 120 ms and send 15 ms heartbeats. These waits sit in the mock upstream; slowness cannot
    fail them;
  - `TestHealthcheckTimeout`, which checks the base `healthcheck` subcommand's 2 s client timeout
    (Docker HEALTHCHECK);
  - `mem_test`, opt-in only.
- Production timers: only the base `healthcheck` client `Timeout: 2s` and the server
  `ReadHeaderTimeout: 10s`. Both predate M1 and are unchanged. M1 adds no timer or expiry, no count
  cap and no soft accounting.

## Local acceptance (M0 harness)

Runner: `/var/tmp/bridge-m1-work/acc.py`. It imports `m0.py` and `hops.py` unchanged and uses
`m0.start`/`m0.one`: a fresh container per request, a `memory.peak` reset, a 50 ms anon/file/disk
sampler, `fdscan.py` for unlinked spool fds, and `mockslow` reading at 64 MiB/s. It changes only:
- the binary: this clone, built with the Dockerfile flags;
- the scratch root: `/var/tmp/bridge-m1-work/acc`, so the M0 tree is not touched;
- the spool dir: chmod 0700, which D3 enforces.

After each request it also counts `(deleted)` fds that stay open in the container. The client is the
real `wsclient` (coder/websocket, read limit off, one text frame per turn).

- `bridge-raised-ws-5g` is the M0 config as is (raised env, 5g) with the M0 bodies.
  `wsclient` adds 25 bytes of `{"type":"response.create",`.
- `m1-default-ws-256m` uses the M1 defaults: the 256 MiB cap, a read-only ledger with one ticket,
  and the production 256m limit with no swap. Its `f-*` frames come from `mkframe.py`: the M0 body,
  re-padded to an exact frame size, carrying `ai_gateway_capacity_ticket` at 64 MiB or more.

| config | body (frame bytes) | result | peak MiB (idle) | anon max MiB | disk peak | disk after / open spool fds |
|---|---|---|---|---|---|---|
| bridge-raised-ws-5g | image-16 (16777241) | success | 16.3 (8.0) | 6.4 | 1.0x (16777241) | 0 / 0 |
| bridge-raised-ws-5g | text-16 (16777241) | success | 14.8 (6.0) | 6.4 | 1.0x (16777241) | 0 / 0 |
| bridge-raised-ws-5g | image-64 (67108889) | success | 15.8 (6.1) | 6.4 | 1.0x (67108889) | 0 / 0 |
| bridge-raised-ws-5g | text-64 (67108889) | success | 15.1 (6.2) | 6.4 | 1.0x (67108889) | 0 / 0 |
| bridge-raised-ws-5g | image-256 (268435481) | success | 15.5 (3.8) | 4.4 | 1.0x (268435481) | 0 / 0 |
| bridge-raised-ws-5g | text-256 (268435481) | success | 18.7 (6.5) | 6.4 | 1.0x (268435481) | 0 / 0 |
| m1-default-ws-256m | f-image-16 (16777216) | success | 15.2 (6.4) | 6.4 | 1.0x (16777216) | 0 / 0 |
| m1-default-ws-256m | f-text-16 (16777216) | success | 16.9 (5.9) | 6.4 | 1.0x (16777216) | 0 / 0 |
| m1-default-ws-256m | f-image-64 (67108864) | success | 15.0 (6.3) | 6.4 | 1.0x (67108864) | 0 / 0 |
| m1-default-ws-256m | f-text-64 (67108864) | success | 15.7 (6.3) | 6.4 | 1.0x (67108864) | 0 / 0 |
| m1-default-ws-256m | f-image-256 (268435456) | success | 17.3 (5.7) | 6.4 | 1.0x (268435456) | 0 / 0 |
| m1-default-ws-256m | f-text-256 (268435456) | success | 17.4 (8.2) | 6.4 | 1.0x (268435456) | 0 / 0 |
| m1-default-ws-256m | image-256 (268435481) | error-event request_too_large | 8.5 (6.7) | 4.4 | 0.94x (253001728) | 0 / 0 |
| m1-default-ws-256m | f-image-64-noticket (67108864) | error-event capacity_ticket_missing | 7.2 (5.8) | 4.4 | 1.0x (67108864) | 0 / 0 |

- No run ended in 1009 or a bare close. `grep 1009` over every `.resp` finds nothing.
  - Successes got `response.completed`.
  - The refusals got `{"type":"error","status":413,"error":{"code":"request_too_large","hop":"responses-ws-sse-bridge",...268435456-byte...}}`
    and `{"type":"error","status":503,"error":{"code":"capacity_ticket_missing",...}}`.

  The close code after an error event is covered by the unit tests (`readCloseAfter`), because
  `wsclient` stops at the terminal event.
- Memory is flat. Peak does not grow with N: 14.8–18.7 MiB from 16 to 256 MiB, and anon max is
  6.4 MiB in every run (M0: 14.3–17.7 MiB peak, 6.4 MiB anon). At 256m the 256 MiB frame peaks at
  17.4 MiB, far below the spec bound of 256m − 32 MiB = 224 MiB.
- The disk spool is 1.0x the frame. After every run the spool dir is empty and no spool fd is open.
- Upstream bytes: the frame minus `"type":"response.create",` (and the ticket member on M1), with
  `stream` forced. The ticket reaches Sub2API only as the header (unit test).

## Deviations from the spec

1. **The spool is closed at turn end, not "before committing the response status"** (tasks 2.1,
   D2/D3). Supervisor decision B: no code change.
   - Reason: disk credit sits on the ticket for the whole turn (bridge `k_disk` = 1), and keeper
     releases the ticket only after every hop drops its lease, so an earlier close frees no credit.
     Waiting for the transport to finish sending the body before the first event would also stall
     on an upstream that answers early (an error before it reads the body, or a full-duplex peer).
   - Covered by `TestM1UpstreamEarlyErrorDoesNotStallTurn`: an 8 MiB frame gets an upstream 413
     before the upstream reads it. The test asserts the explicit error event, the spool closed and
     the socket usable for the next turn.
   - **Proposed tasks.md 2.1 wording:** "spool is closed at turn end; disk is accounted by the
     ticket for the whole turn".
2. **Peak memory at one point is above the M0 figure.** One `text-256` run on the raised config
   peaked at 18.7 MiB, over M0's ≤17 MiB (M0 max 17.7). Re-runs gave 16.0 and 17.6 MiB, and anon is
   unchanged at 6.4 MiB, so this is sampling noise around a flat line, not growth.
3. **A body of exactly 256 MiB inside a frame is over the cap.** The M0 `image-256` body plus the
   25-byte `response.create` prefix is 268,435,481 bytes, so M1 refuses it with `request_too_large`.
   The cap applies to the frame (D1), and an exact 268,435,456-byte frame passes.

## Residual risks / not checked

- Only the bridge was checked, locally against the mock. It was not tested behind keeper
  (`chain_ws.sh`) and not on production. Tasks 2.2 (APISIX) is not in scope.
- Keeper's release waiter was simulated: it is a test `LOCK_EX`, not keeper code.
- The `reader_error_cancel` test no longer reaches the queue-full reader error path it was named for
  (see above). Its cancel and handler-exit assertions remain.
