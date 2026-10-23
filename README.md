# responses-ws-sse-bridge

Stateless converter from the OpenAI Responses **WebSocket mode** to streaming
HTTP (`POST` + SSE). It exists because the inner APISIX model selector can only
classify model-bearing **HTTP JSON bodies** (`/` in the model id → CPA, else →
AISIX) and cannot route WebSocket.

## Position

```
client → public APISIX → keepalive → openai-api-cache-keeper
  → THIS BRIDGE → Sub2API (HTTP/SSE only; account WS mode off)
  → inner APISIX selector → AISIX | CPA
```

The single-upstream keeper retains history and expands every
`previous_response_id` it owns into a full `input` before the request reaches
this bridge. Set `IGNORE_CLIENT_STORE_FALSE=true` for clients such as Codex.
Sub2API never handles WebSocket. See the [topology decision](https://github.com/xz-dev/AI-gateway/blob/main/docs/websocket-termination.md)
for the tested Sub2API passthrough and owner-gate incompatibilities.

## Behaviour

- **Plain HTTP** (any method/path, not a WS upgrade): transparent
  `httputil.ReverseProxy` to `UPSTREAM_URL`. Bodies and headers stream
  untouched (`FlushInterval: -1`); the upstream status is preserved.
- **WS upgrade on `/v1/responses` or `/responses`**: terminated. Every text
  frame must be a `response.create` event.
  - `type: response.create` → `type` stripped, `stream:true` forced, POSTed
    upstream. Client end-to-end headers (incl. `Authorization`) are forwarded;
    hop-by-hop and `Sec-WebSocket-*` are dropped.
  - Every upstream SSE event's `data:` payload is relayed as **one text
    frame**, in order.
  - `previous_response_id` non-empty → `error` frame `{status: 400, error:
    {type: invalid_request_error, code: previous_response_not_found, param:
    previous_response_id}}`. The bridge is stateless; the keeper already
    expanded every known parent, so this can only be a foreign/expired id.
    The socket stays open.
  - `generate: false` (Codex prewarm) → answered locally with
    `response.created` + `response.completed` (`resp_` id, full Response
    schema, model echoed). No upstream call: HTTP Responses has no prewarm
    semantics (same approach as Sub2API PR #7668).

    **Keeper contract:** the keeper upstream of this bridge records that
    local turn like any other, so the `resp_` id it returns becomes a valid
    `previous_response_id` for the client's next turn — a keeper hit that
    expands to the (empty) prewarm history. This bridge itself is stateless
    and would reject that same id with `previous_response_not_found`; only
    the keeper makes prewarm ids resolvable.
  - Other frame types → `error` frame naming the unsupported type.
  - Upstream non-2xx → `error` frame carrying `status` and the upstream error
    object; socket stays open.
  - Truncated/malformed upstream stream → `response.failed` frame if a
    response had started, else an `error` frame.
- **Turns are sequential** per socket; at most one frame queues behind the
  in-flight turn, anything beyond gets an explicit overload `error` frame.
- Control frames are handled by the library (ping→pong). Client close cancels
  the in-flight upstream request.
- Nothing is retained between turns or across sockets.

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `UPSTREAM_URL` | *(required)* | Sub2API base URL, `http(s)`, no query/userinfo |
| `LISTEN_ADDR` | `:8080` | Listen address |
| `PORT` | *(unset)* | Optional healthcheck-only override: probe `127.0.0.1:$PORT` instead of `LISTEN_ADDR`. Does not change the server listener. |
| `MAX_SOCKETS` | `64` | Live WebSockets; overflow handshake → 503 |
| `MAX_MESSAGE_BYTES` | `16777216` | Max incoming WS frame bytes |
| `MAX_BODY_BYTES` | `16777216` | Per-event bound: max bytes of one upstream error body or one SSE event |
| `WORK_MAX_BYTES` | `268435456` | Global working-byte budget for in-flight request spools |
| `SPOOL_DIR` | `$TMPDIR` (`/tmp`; `/spool` in the image) | Directory for bounded request spools; must be writable by the runtime user |
| `TURN_IDLE_TIMEOUT_SECONDS` | `0` | Upstream **inactivity** timeout; `0` disables (upstream has its own 60s idle) |

## Idle-timeout semantics

`TURN_IDLE_TIMEOUT_SECONDS` bounds **upstream inactivity**, never total
turn length. The timer starts when the POST is issued — so it also covers
waiting for response headers — and resets on every byte read from the
upstream body, including SSE comment/heartbeat lines and partial event
data that the parser does not surface as events. On fire it cancels the
upstream request and ends the turn with the declared failure terminal
(`response.failed` if a response had started, else an `error` frame).

## Request path

A client `response.create` frame is spooled to a bounded file under
`SPOOL_DIR` as it is read, then validated in a single streaming pass that
records each top-level member's value byte span without materialising
values. The POST body is an `io.MultiReader` of literal segments plus
`SectionReader` spans over that spool — a known `Content-Length` with no
second copy. Queued spools are released on every disconnect path.

## Healthcheck

The `healthcheck` command sends `GET /healthz` and succeeds only for HTTP 200,
without following redirects. Its total timeout is 2 seconds; it does not use
HTTP proxy environment variables. It uses the server's `LISTEN_ADDR` and the
same `:8080` default, so `LISTEN_ADDR=127.0.0.1:8086` needs no separate port
setting. An empty or wildcard host probes loopback (`127.0.0.1` for IPv4,
`::1` for IPv6).

An explicit `PORT` retains the old override contract: probe
`127.0.0.1:$PORT`, regardless of `LISTEN_ADDR`. Keep overrides aligned with the
bridge listener in shared network namespaces. This checks local HTTP liveness,
not upstream health, inference, or cached continuation; `UPSTREAM_URL` is not
required by the command.

## Published deployment and healthcheck caveat

The AI-gateway deployment uses implementation `6af0a3ef95c6a6f1e1cd8fe353b974196fdc2d44`, image `ghcr.io/xz-dev/responses-ws-sse-bridge:6af0a3ef95c6a6f1e1cd8fe353b974196fdc2d44@sha256:3b23fd0963d415d32beefa783fc50d83d03342d3c95d2551554afbc5dd006caa`. Documentation commits after that revision do not imply a newer image has been deployed.

**For this image use `LISTEN_ADDR=127.0.0.1:8086` together with `PORT=8086`.** Its healthcheck reads `PORT`, not `LISTEN_ADDR`, and checks a TCP listener, not HTTP readiness. In a shared network namespace, omitting `PORT` can falsely report another service on 8080 as healthy. On the 2026-10-23 cold boot this caused a dependency deadlock: keepalive was waiting for keeper, keeper for bridge health, while bridge incorrectly checked keepalive's port. A successful `/healthz` GET on the bridge's actual port is the independent application check. See the gateway decision/incident record for recovery status. The source-level healthcheck fix above has not been deployed as part of this change; no VM reboot drill has been performed.

Before initial publication a binary/log cleanup was required. Current reachable history contains no generated binary (largest blob 22,001 bytes at implementation revision); an initial mistaken push was force-with-lease replaced, so GitHub may retain unreachable objects until GC. No credentials were embedded in the removed binary.

## Notes

- The bridge never writes `previous_response_id` upstream and never invents
  history; expansion is solely the keeper's job.
- The bridge rejects **every** `previous_response_id` with
  `previous_response_not_found`: it holds no state, so it can resolve none.
  Only ids the keeper expands reach it — and those arrive with the field
  already stripped.
- `store` is forwarded verbatim. The keeper advertises `store:true` only on
  turns it actually retained.
- Logs carry connection/turn metadata only — never credentials or bodies.
