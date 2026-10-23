# responses-ws-sse-bridge — worker receipt

HEAD `8a2e6f9` on `git init` repo at `/var/tmp/ws-sse-bridge-20261022`
(round-2 review `REVIEW2.md` → CHANGES REQUIRED, all findings fixed below;
round-1 review `REVIEW.md` fixed at `5a1d0ca`). Module
`github.com/xz-dev/responses-ws-sse-bridge`, Go 1.26.

## Position

```
client → APISIX → Sub2API (passthrough, auth only) → openai-api-cache-keeper
  → THIS BRIDGE → inner APISIX selector (model "/" → CPA, else → AISIX)
```

Stateless converter from Responses WebSocket mode to streaming `POST` + SSE.
Exists because the inner APISIX selector can only classify model-bearing
HTTP JSON bodies, not WebSocket.

## Dependencies

```
github.com/coder/websocket v1.8.15     — WS termination, fragmentation,
                                       control frames, read limits
github.com/tmaxmax/go-sse   v0.11.0    — WHATWG-§9.2 streaming SSE decoder
github.com/tidwall/gjson    v1.20.0    — test-only: benchmark comparison
                                       baseline for scan.go (not in binary)
(+ tidwall/match, tidwall/pretty — gjson transitive)
```

## Files

- `main.go` — env config, `/healthz`, `healthcheck` subcommand.
- `bridge.go` — `bridge` (transparent `httputil.ReverseProxy` for plain HTTP
  and non-Responses upgrades), `serveWS` (reader goroutine spools each frame
  to bounded disk while servicing control frames + this turn worker),
  `handleTurnStream`, `runUpstream`, `sendUpstreamError`, `prewarmEvents`,
  `acquire` (global working-byte budget).
- `sse.go` — `relaySSE` on `sse.Read` (each event's `data:` → one WS text
  frame; metadata-only blocks not dispatched; pending-at-EOF discarded;
  flat upstream error → exactly one WS error envelope; `response.failed`
  vs `error` frame on broken streams; terminal events end the turn without
  waiting for EOF; byte-level idle watchdog covering header wait),
  `endToEndHeaders` (RFC 9110 §7.6.1 nominated-header stripping),
  `eventHead`, `finishErr`, `adaptFlatError`, `pullIter`.
- `scan.go` — `inspectFrameStream` (streaming `json.Decoder` validation +
  routing fields), `rewriteBodyStream` (member-exact rewrite, `type` and
  WS-only `generate` stripped, `stream` forced to exactly one `true`),
  `memberIter`/`scanString`/`skipValue` (verbatim member copy after
  validation), `rewriteBody` wrapper, `findTopLevelMember`.
- `spec_test.go` — spec-conformance tests (see table).
- `bridge_test.go`, `bench_test.go`, `mem_test.go`, `Dockerfile`
  (golang-alpine builder → scratch, USER 65534, HEALTHCHECK), `README.md`.

## Design decisions

1. **Plain HTTP** → transparent proxy (`FlushInterval:-1`, no buffering,
   status/headers preserved, no retry). Non-Responses WS upgrades tunnelled.
2. **`previous_response_id` non-empty → `error` frame** `400 /
   invalid_request_error / previous_response_not_found /
   param=previous_response_id`, socket kept open. Non-string non-null values
   and duplicate routing fields reject the same way — the boundary cannot be
   bypassed by type confusion or key duplication. Bridge is stateless; the
   keeper expands every resolvable parent upstream of it and strips the id,
   so any id reaching this bridge is foreign/expired.
3. **`generate:false` → answered locally** (`response.created` +
   `response.completed`, `resp_` id, empty output, zero usage, model echoed),
   no upstream call (same approach as Sub2API PR #7668 for its http_bridge).
   Keeper contract in README §generate:false — the keeper records that local
   turn, so the returned `resp_` id resolves as a keeper hit next turn.
4. **`response.create` → POST** with `type` and `generate` stripped,
   `stream:true` forced to exactly one member; end-to-end headers forwarded
   (`Authorization` etc.); hop-by-hop, `Sec-WebSocket-*` and
   Connection-nominated headers dropped.
5. **Each SSE event `data:` → one text frame**, in order, as soon as its
   blank line arrives. Nothing collected.
6. **Turns sequential** per socket; one frame may queue behind the in-flight
   turn; beyond → `error` frame (turn-overload).
7. **Upstream non-2xx → `error` frame** carrying `status` + upstream error
   object verbatim; socket stays usable.
8. **Broken stream** → `response.failed` (same public id) if `created` /
   `in_progress` seen, else `error` frame.
9. **Client close cancels the in-flight upstream.** A reader goroutine
   sits in `conn.Reader` for the socket's whole life — it is the only path
   that observes a close frame or TCP drop and fires `socketDone` to cancel
   the turn context, and the only path that answers pings. It spools each
   frame to a bounded temp file before handing it to the worker, so
   `conn.Reader` is called again immediately and control frames keep being
   serviced during long turns. `r.Context()` alone is insufficient (only
   fires when the HTTP handler returns). Verified by
   `TestClientCloseCancelsUpstream`.

   *Regression history:* the pre-refactor design had the turn worker drive
   `conn.Reader` itself — that left no serviced `Read` while a turn ran, so
   a client close during an in-flight turn was never observed and the test
   hung. Restored the dedicated reader; the worker never calls `Read`
   itself. A second trap found in review-fix: reader ctx must be
   `context.Background()` — the library force-closes the socket when a
   *read* context ends, which would kill close-status propagation.
10. **Logging** carries no credentials or bodies.
11. **Streaming posture.** Incoming frame → disk spool (reader) → one
    streaming validation/routing pass (`json.Decoder`, one member resident)
    → one resident copy charged to `WORK_MAX_BYTES` → rewritten body →
    second disk spool → POST with known `Content-Length`. Upstream SSE
    events stream out one by one; only per-event `MaxEventSize` is
    bounded — no cumulative stream cap.
12. **Overload.** One frame queues behind the in-flight turn; every further
    request is drained and gets its own `working_set_unavailable` error
    frame — replies are never coalesced.
13. **Protocol edges.** Invalid UTF-8 text → close 1007. No redirect
    following (`http.ErrUseLastResponse`). `TURN_IDLE_TIMEOUT_SECONDS` is a
    reset-on-data watchdog. `[DONE]` sentinel passes verbatim; non-JSON
    event payloads fail the turn with the declared upstream-failure shape.

## Libraries (reuse decision)

| Component | Choice | Why |
|---|---|---|
| SSE decode | `tmaxmax/go-sse` | WHATWG-§9.2-compliant, streams event-by-event, bounded `MaxEventSize`. Replaced ~120 lines of hand-written parser. |
| WS | `coder/websocket` | Already a project dep; provides Reader/Writer, read limit (1009), fragmentation, control-frame handling. |
| HTTP passthrough | `net/http/httputil.ReverseProxy` | stdlib, streaming. |
| Top-level JSON validate + peek | `encoding/json` `Decoder` + `memberIter` | `inspectFrameStream` does full validation streaming; the byte-exact scanner only runs on pre-validated frames. |
| JSON body rewrite | `rewriteBody` in `scan.go` | `gjson`/`sjson` can't delete a member + force `stream:true` by verbatim byte copy without re-encoding. |

## Spec-conformance tests (all pass)

| Spec | Rule | Test |
|---|---|---|
| RFC 6455 §5.4 | fragmentation reassembled | `TestRFC6455_FragmentedMessageReassembled`, `TestRFC6455_FragmentedMessageWhilePing` |
| RFC 6455 §5.4 | control frames interleaved | delegated to `coder/websocket` read loop; between-message ping proven by `TestRFC6455_PingBetweenTurnsAnsweredPong` |
| RFC 6455 §7.1 | close handshake | `TestRFC6455_CloseHandshakeCompletes` |
| RFC 6455 §7.4.1+§5.2 | 1009 for oversize | `TestOversizeFrame` |
| RFC 6455 §8.1 | UTF-8 text frames | `TestRFC6455_InvalidUTF8TextFrameRejected` |
| RFC 6455 §5.5.2/3 | ping→pong | `TestRFC6455_PingBetweenTurnsAnsweredPong` |
| WHATWG §9.2 | CR/LF/CRLF | `TestSSE_LineEndings_CR_LF_CRLF` |
| WHATWG §9.2 | BOM | `TestSSE_LeadingBOM_Stripped` |
| WHATWG §9.2 | comment lines | `TestSSE_CommentLinesIgnored` |
| WHATWG §9.2 | multi-line data, no-colon field, single space, event/id/retry | `TestSSE_FieldParsing`, `TestSSE_FieldNoColon` |
| WHATWG §9.2 | dispatch only on blank line, EOF partial discarded | `TestSSE_DispatchOnBlankLine_TrailingPartialDiscarded` |
| OpenAI | unknown events relayed | `TestSSE_UnknownEventRelayed` |
| OpenAI | `[DONE]` passthrough | `TestSSE_DoneMarkerPassthrough` |
| OpenAI | `sequence_number` preserved | `TestOpenAI_SequenceNumberPreserved` |
| OpenAI | `response.failed` shape | `TestTruncatedSSEEndsFailed`, `TestOpenAI_IncompleteEndsTurn` |
| OpenAI | `previous_response_not_found` shape | `TestPreviousResponseIDRejectedNoUpstreamCall` |
| OpenAI | upstream error frame shape | `TestUpstreamErrorBecomesErrorFrameSocketSurvives` |
| Anthropic | named events + ping, byte-for-byte | `TestAnthropicHTTPPassthrough_NamedEventsAndPing` |
| chat/cmp | `[DONE]` HTTP passthrough | `TestHTTPPassthroughSSE` |

## Validation

- `go test ./...` — **53 pass** (29 base + 20 regression probes + new).
- `go test -race ./...` — clean.
- `go vet ./...` — clean. `go build` — clean.
- All 16 previously-failing reviewer probes now pass as permanent
  `TestRegression*` tests (regress_test.go).

## Memory / CPU (hard criterion)

### Benchmarks (`go test -bench -benchmem`)

| Benchmark | ns/op | throughput | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkInspectFrame` | **632** | **2932 MB/s** | 32 | 2 |
| `BenchmarkInspectFrameUnmarshal` (full decode baseline) | 7968 | 233 MB/s | 5385 | 91 |
| `BenchmarkInspectGjson` | 1968 | — | 48 | 2 |
| `BenchmarkInspectDecoderToken` | 4802 | — | 4960 | 91 |
| `BenchmarkRewriteBody` | **480** | **3857 MB/s** | **0** | **0** |
| `BenchmarkRelayTurn1MiB` (50 ev, 1 MiB) | 3.7 ms | 281 MB/s | 8.1 MB | 1668 |
| `BenchmarkRelayTurn100Events` | 0.84 ms | 48 MB/s | 346 KB | 1822 |
| `BenchmarkHTTPPassthrough` | 0.89 ms | 1176 MB/s | 1.1 MB | 235 |

`inspectFrame` ≈ 12× faster / ~170× fewer allocs than full `json.Unmarshal`;
gjson is 3× slower and can't rewrite. `rewriteBody` is zero-alloc.

### RSS — independent process-isolated measurement
(re-run of the reviewer's `measure_rss.py` after the fix; fixture server and
load client in separate processes; `/proc` VmRSS)

| Phase | VmRSS |
|---|---|
| idle | 8,980 KiB |
| 64 idle sockets | 11,520 KiB |
| 192 × 64 KiB requests (16 conc) | 19,144 KiB |
| 64 × 1 MiB requests (16 conc) | 48,344 KiB |
| sampled peak | 62,264 KiB |
| after all sockets closed | 22,964 KiB |

The review's 142 MiB peak on the same 1 MiB workload dropped to ~48–62 MiB
with the disk-spool + global-budget design. Bridge CPU: 1.76 s over ~5.2 s
wall (includes fixture waits; not a saturation benchmark).

### CPU profile top hotspots

`net/http.serve` + `relaySSE` dominate (real I/O). Inside relay,
`tmaxmax/go-sse` parser `NewlineIndex` + `bufio.Scanner.Scan` ≈ 26 % —
that's spec-correct parsing work, not overhead. Kernel/runtime
(`memmove`/`systemstack`/GC) accounts for the rest. No bridge-logic
hotspots.

### Per-socket cost

**3 goroutines** per socket (reader + turn worker + one `pullIter` during
the upstream relay only). Idle socket allocates nothing beyond the
library's own state. Turn working bytes are charged against
`WORK_MAX_BYTES` (default 256 MiB, ~4× max frame).

### Suggested limits

- `GOMEMLIMIT`: **256 MiB**
- Container memory: **512 MiB**
- CPU: **0.5 core** (1 core only if >64 concurrent streaming turns)

## Open risks / notes

- `TURN_IDLE_TIMEOUT_SECONDS` defaults `0` (disabled); inner APISIX already
  enforces 60 s stream-idle. Bridge-side bound is implemented (reset-on-
  data idle watchdog) and tested; production timing untested.
- `MAX_MESSAGE_BYTES` bounds one incoming WS frame (16 MiB); the upstream
  bound `MAX_BODY_BYTES` bounds per-event SSE data (go-sse `MaxEventSize`)
  and error bodies — the relayed stream itself has no cumulative cap.
- `WORK_MAX_BYTES` bounds resident rewritten frames (default 256 MiB).
- `AcceptOptions.OriginPatterns:["*"]` accepts any Origin — bridge sits
  behind auth'd APISIX/Sub2API and is not browser-exposed.
- `previous_response_not_found` message text mirrors the current Sub2API
  contract; shape stays valid if upstream wording changes.
- Request bodies reach upstream as `Content-Length` POSTs spooled on disk;
  upstreams that cannot serve plain HTTP/1.1 POST bodies were never
  in-scope. No live integration test against real keeper/selector —
  covered by the compose/acceptance run.

## Review-fix mapping

| # | Fix | Regression test | Commit |
|---|---|---|---|
| 1 | disk-spool frame + streamed rewrite + global budget | rss-results.json (142→62 MiB) | eef062f, 990b33d, f35281a |
| 2 | per-request overload replies | `TestRegressionQueueOverloadAnswersEveryRequest` | eef062f |
| 3 | `inspectFrameStream` full JSON validation | `TestRegressionRejectMalformedJSON` | 3696eab |
| 4 | prev-id type/duplicate strictness | `TestRegression{NonString,Duplicate}Parent*` | 3696eab |
| 5 | close 1007 inbound; UTF-8/JSON-valid outbound | `TestRegressionUTF8Requires1007`, `TestRegressionOutboundUTF8Valid` | eef062f, 9095c62 |
| 6 | `turnIdle` reset-on-data watchdog | `TestRegressionIdleTimeoutEnforced` | 9095c62 |
| 7 | cumulative stream cap removed | `TestRegressionInfiniteStreamNoCumulativeCap` | 9095c62 |
| 8 | terminal/error ends turn without EOF; flat-error envelope | `TestRegressionTerminalDoesNotWaitEOF`, `TestRegressionWSErrorTerminatesTurn` | 9095c62 |
| 9 | malformed upstream payload → declared failure | `TestRegressionMalformedSSEPayloadIsNotRelayed` | 9095c62 |
| 10 | `sequence_number` on synthetic events | `TestRegressionPrewarmSequences` | 9095c62 |
| 11 | `CheckRedirect: ErrUseLastResponse` | `TestRegressionNoRedirectFollowing` | eef062f |
| 12 | Connection-nominated header stripping | `TestRegressionConnectionNominatedHeadersStripped` | 9095c62 |

---

## Round-2 fix table (REVIEW2.md → commit)

| # | Finding | Fix | Test | Commit |
|---|---------|-----|------|--------|
| 1 | Queued spool abandoned on disconnect (1,818 files / 113.7 MiB) | teardown cancels reader then drains `incoming`, releasing every queued spool | `TestRegressionQueuedSpoolRemovedOnDisconnect` | `6f92f32` |
| 2 | Scratch image has no writable tmp | `SPOOL_DIR` env (default `os.TempDir`), Dockerfile ships non-root `/spool` + `ENV` | startup `os.Stat` on config | `6f92f32` |
| 3 | Full input materialised (10.5 MB / 2 MiB) | byte-level `streamScan` validates + records member spans, no value retention; POST body = `io.MultiReader` over spool spans, single spool, no second buffer | `BenchmarkInspectFrameStream2MiB` 5.0 ms/3.3 MB/49 alloc vs 3.35 ms/10.5 MB | `6f92f32`+`8a2e6f9` |
| 4 | Metadata-only SSE → 502; pending-at-EOF dispatched | `Data==""` → skip (WHATWG no-dispatch); `eofMarkingReader` maps EOF→`errStreamEOF` so parser reports pending at EOF instead of delivering | `TestRegressionSSENoDataEventsIgnored`, `TestRegressionSSEEOFWithoutBlankLineDiscarded` | `6f92f32` |
| 5 | Flat upstream error → two WS messages | inspect `type` before send; flat error → one `{type:error,status,error}` envelope, socket survives | `TestRegressionFlatErrorSingleEnvelope` | `6f92f32` |
| 6 | Idle timeout ignores heartbeats + header wait | `idleWatch` starts at request issue, resets on every body byte (comments included); body pumped through `io.Pipe` signalling activity | `TestRegressionIdleHeartbeatIsData`, `TestRegressionIdleBeforeHeaders` | `6f92f32` |
| 7 | JSON `null` accepted as event | `eventHead` requires non-nil object | `TestRegressionNullSSEPayloadRejected` | `6f92f32` |
| 8 | Synthetic Response objects miss schema fields | shared `responseObject` emits required fields per openai-python `response.py`: id, created_at, object, model, output, parallel_tool_calls, tool_choice, tools + status/usage/error | `TestRegressionGeneratedResponseRequiredFields` | `6f92f32` |
| adv | Duplicate routing fields last-wins | `type`/`generate`/`model`/`previous_response_id` duplicates → reject as ambiguous | covered by inspect error path | `8a2e6f9` |

## Round-2 measurements

Separate-process five-minute churn (reviewer's `measure_rss.py`, rebuilt
binary, TMPDIR under review dir): **spool files 1,818 → 0**, FDs **7 → 7**,
VmRSS 8.9 MiB idle → 9.8 MiB / 64 idle sockets → VmHWM 64.9 MiB → **15.1 MiB
after churn**, bridge CPU ≈ 9.6 s / 300 s wall. Chain e2e
`/var/tmp/chain-e2e-20261022/run.sh` **36/36 PASS** (stub→bridge→keeper,
HTTP+WS chains, msg_ rewrite, prewarm, store:false, reconnect, mid-stream
close, 16×5 concurrency, Anthropic named events + `[DONE]` byte-equal).

Benchmarks (`-benchmem`): `InspectFrameStream` 2 MiB 5.0 ms / 3.3 MB / 49
allocs (bounded 256 KiB window + read chunks — was 10.5 MB materialised);
`RewrittenBody` 2 MiB 30 µs / 34 KB / 26 allocs; `RelayTurn1MiB` 4.4 ms /
8.3 MB / 2121 allocs; `HTTPPassthrough` 0.8 ms / 1.1 MB / 234 allocs.

## Round-4 fix table (REVIEW3.md → commit)

| # | Finding | Fix | Test | Commit |
|---|---|---|---|---|
| 1 | Unbounded parser recursion → 16–32 MiB stack on 200 KB deep input (B6) | `skipComposite` rewritten as an explicit-stack iterative validator; `maxJSONDepth = 128`, over-limit → `errTooDeep` → `invalid_request_error` frame before any upstream effect | `TestReview4NestedInputMemoryBound`, `TestReview4DepthLimitBoundary` | `936350d` |
| 2 | `data:\n\n` discarded as metadata; WHATWG §9.2.6 requires dispatching an empty-data event (B4) | `blockPeek` raw-stream tracker records, per dispatched block, whether a `data` field was present; empty-Data event with `hasData` → declared malformed-event rule, without → metadata-only skip | `TestReview4EmptyDataBlockIsNotMetadata`, `TestReview4MetadataOnlyStillNotDispatched` | `f680572` |
| 3 | Synthetic `usage` missing required nested `input_tokens_details`/`output_tokens_details` (B2/B5, ResponseUsage schema) | `responseObject` emits full zero-usage incl. `{cached_tokens:0}` and `{reasoning_tokens:0}` (openai-python `response_usage.py`) | `TestReview4SyntheticUsageSchema` | `e0be9db` |
| adv | Non-blocking: top-level trailing comma repaired then forwarded | `inspectFrameStream` rejects `}` when a key is owed after `,` | reviewer's `TestReview3TopLevelTrailingComma` (now passes) | `713559b` |

## Round-4 validation

- `go test -count=1 .`: **73 tests pass** (incl. 5 new `TestReview4*`).
- `go test -race -count=1 .`: PASS; `go vet ./...`: clean.
- Reviewer overlay, all probes unmodified
  (`overlay-all.json` = round-1 + compat round-2 + round-3 probes):
  **50/50 pass**, including `TestReview3NestedInputMemoryBound`,
  `TestReview3EmptyDataBlockIsNotMetadata`,
  `TestReview3SyntheticUsageSchema`, and the previously non-blocking
  `TestReview3TopLevelTrailingComma`.
- Round-1/2 probe overlay (`overlay.json`): **46/46 pass**, race pass.
- Chain e2e `/var/tmp/chain-e2e-20261022/run.sh` at HEAD `713559b`:
  **9/9 scenario groups, 29 checks, 0 failures**.
- Iterative validator stack use is O(128) `frameState` entries
  (~2 KiB), no per-depth goroutine frames.
