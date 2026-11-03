# Bridge actual-image resource acceptance — PREPARED_NOT_EXECUTED

Actual-image test: **NOTRUN**. No engine inventory, image lookup/pull, container creation, SSH, registry/provider or production request performed during preparation. Reference SSE harness read only, never executed; its image/result cannot count as bridge evidence. Bridge source remains mutable/unapproved; helper compiles against its existing dependency metadata, not its implementation.

## Artifacts and local validation

- `main.go` / `probe`: standalone synthetic HTTP/SSE backend and WS driver; no app code linked.
- `accept.py`: future authorized rootless local Podman runner, positional immutable ref/source/approval only.
- `build.sh`: offline build/vet with `-mod=readonly`, `GOPROXY=off`, `GOSUMDB=off`, `GOTOOLCHAIN=local`, isolated disposable caches under this directory; verifies reference go.mod/go.sum unchanged.
- `test_runner.py`: mocked engine and workload-oracle tests; never invokes runtime. Rejects floating/mismatched references, missing authorization/source/CI/filesystem approval, stale deployment measurement, remote/rootful/unknown engine. Eight tests include earlier-normal-only blocked-spool rejection, qualifying blocked sample acceptance, sample boundary/missing-timestamp rejection, and total TCP cleanup >200ms rejection.
- `preparation.json` and `SHA256SUMS`: preparation evidence and exact artifact hashes. Selftests are not image acceptance.

Rebuild locally: `sh ./build.sh`. Installed compiler used; no dependencies installed. Build output is static Linux executable. Private parent mode 0700; files 0600 except helper 0755 under that parent.

## Future invocation — parent authority only; NOT authorized by this document

1. Obtain approved narrow repair source SHA, successful exact-source CI run, and published immutable image. Parent independently validates CI and source approval; local approval JSON is an attestation, not remote CI verification.
2. Ensure image already exists in intended local rootless Podman engine. Runner never pulls. Required ref form is `REGISTRY/IMAGE:FULL_40_HEX_SOURCE_SHA@sha256:FULL_64_HEX_DIGEST`; tag must equal supplied full source SHA. No default, old, digest-only or floating ref accepted.
3. Freshly measure actual deployment **volume filesystem**, not CI tmpfs or container root filesystem. Record Statfs magic type (hex, as `stat -f -c %t`), blocks and block size; timestamp UTC under 24 hours old. Parent/operator checks type, capacity/headroom and suitability. Runner compares type/block size, rejects tmpfs and insufficient test-volume headroom, records test type/blocks/free blocks/block size. Matching magic alone does not prove equivalent storage behavior. Parent must compare recorded test/deployment Statfs after run before acceptance.
4. Under existing conditional authority, create private `approval.json` within this directory with real values and explicit authorization. Example schema (placeholders intentionally invalid):

```json
{
  "source_approved": true,
  "actual_image_run_authorized": true,
  "source_sha": "<approved-full-40-hex-SHA>",
  "ci_source_sha": "<same-full-SHA>",
  "ci_conclusion": "success",
  "ci_url": "https://<exact-approved-source-CI-run>",
  "image": "<registry/image>:<same-full-SHA>@sha256:<64-hex-digest>",
  "filesystem_comparison_approved": true,
  "deployment_statfs": {
    "type": "<fresh-deployment-volume-statfs-magic-hex>",
    "blocks": 1,
    "block_size": 4096,
    "measured_at": "<fresh-UTC-ISO8601>"
  }
}
```

5. Future authorized invocation only:

```sh
python3 ./accept.py 'REGISTRY/IMAGE:FULL_SOURCE_SHA@sha256:DIGEST' 'FULL_SOURCE_SHA' ./approval.json
```

Runner unsets implicit remote connection environment, always passes `podman --remote=false`, and requires rootless/local engine proof. Local image inspection must match revision label and RepoDigest. Observed engine image `Id` is recorded directly; container `.Image` must match. OCI config digest is never substituted as engine ID. Supported Podman schema, readable cgroup v2 counters and rootless-userns `/proc` observation are mandatory; unknown/unavailable observations fail closed rather than weaken proof.

## Actual runtime envelope prepared

App uses **unmodified immutable image**, own cgroup: RAM 256MiB; total RAM+swap 256MiB (`memory.swap.max=0`); GOMEMLIMIT 192MiB; CPU 0.5; PIDs 64; user 65534:65534; root read-only; capabilities ALL dropped; no-new-privileges; no ports published; network none. `/spool` is fresh nonce-labeled **private named volume**, never tmpfs. Runner changes ownership/mode only inside that freshly owned volume via rootless user namespace. Spool root 0700; regular spool files 0600 checked while live, including opened/deleted spool file descriptors. No system/global ownership changes.

Fixture/driver uses same runtime image only as executable carrier with helper entrypoint, separate 192MiB cgroup, shares app's isolated loopback-only netns. Actual distinct cgroups, matching app/driver netns and different host netns checked live. App memory/pids samples apply only to real app cgroup, not driver/backend allocations. No external upstream, proxy, auth, real body or inference.

Explicit app limits: MAX_MESSAGE_BYTES=16MiB, MAX_BODY_BYTES=16MiB, WORK_MAX_BYTES=256MiB. These are recorded, not claimed as internal counter observations.

## Required workload and oracle

1. 65 real `response.create` small turns complete with exact output and `response.completed`, retaining all prior WS sockets. Exactly one lifetime reader per connection remains blocked in WS Read between turns, consumes control frames, and all 65 Ping calls complete **after** 65th turn. Not old one-shot-reader/Ping fixture.
2. 1MiB delta event completes normally on unrelated B.
3. Separate actual slow receiver starts 1024 SSE events of 64KiB delta each (64MiB delta total, greater than configured 16MiB body limit and working windows; JSON overhead additional). Source progress must plateau while source unfinished; arbitrary sleep alone cannot pass. B completes before drain. Backend then must show unchanged progress continuously for **32 seconds**, still unfinished. B completes again while blocked. Concurrent runtime samples must show bounded event spool (8MiB logical/on-open-fd ceiling; max 140 files/open spool FDs for this fixed workload), memory, pids and health state. Plateau phase markers and samples preserved.
4. Drain every event: exact text type, sequence and full bytes, then exact terminal; same WS completes next small turn.
5. Second genuinely blocked source uses 1MiB delta events (64-event upper bound). Observe unfinished progress plateau, complete B, close selected WS with TCP `CloseNow`, require backend handler and selected persistent reader joins within **one total 200ms deadline starting immediately before CloseNow**, then complete B again. Both waits use the same absolute deadline; elapsed nanoseconds (including CloseNow) must be <=200,000,000, independently rechecked by runner. Record actual elapsed TCP cleanup; do not call it protocol close grace.
6. All client readers close/join; health after workload; actual app stop latency under 12s with 10s stop timeout, clean exit. OOM/restarts fail closed throughout. App cgroup memory.current/peak, pids, cpu/swap config, FDs and spool counts/sizes recorded at roughly 250ms plus inspection overhead; sampling is not continuous tracing. Large blocked event spool of at least 1MiB must actually be observed **inside the selected large-blocked plateau before cancellation**, or run fails inconclusive. Earlier normal-large samples cannot qualify. Driver records explicit plateau start/end nanoseconds and event timestamps with Linux `CLOCK_MONOTONIC`; runner brackets each actual app sample with the same clock and requires the entire sample interval inside that plateau. Host/app/driver time namespace equality is verified before workload; missing bounds or no qualifying sample fails closed. Existing 8MiB/file-FD ceilings still apply to every capture.

Slow sockets intentionally do not read data until drain; WebSocket control frames behind unread payload cannot be consumed during that deliberate backpressure pause. Their one lifetime reader resumes for drain/next turn; TCP cancellation reader joins after close. Idle/small connections keep active control readers throughout. No payload copies enter app cgroup from fixture; backend/driver expected-event buffers live in separate cgroup.

## Optional resource-only CI template — not installed or executed

`resource-acceptance.workflow.yml.template` is one reviewed-input candidate for parent to install on **maintenance/bridge-resource-acceptance only**. Parent must place harness sources under `resource-acceptance/bridge/` on that separate maintenance branch; no main/publish trigger, Git write, branch creation or workflow dispatch occurred here. Checkout and artifact actions pinned by commit. Template uses approved app SHA `b482f7128b5f904c50a123889a22030aaa890d8a`, exact-source CI run `41602112775`, and published image digest `c7c010be8e71f9ebc5365af315ce0b7323a1403593d9661d3c1ba0586ba40c35`; parent must bind actual image repository and sanitized CI/approval evidence. App image unchanged: no app image build/push.

Ubuntu 24.04 runner uses preinstalled Podman (reported 4.9.3), preinstalled compatible Go, rootless local engine and actual measured ext4 volume. Unsupported compiler/runtime/cgroup/proc/time-namespace/filesystem conditions fail closed; no sudo, apt, global install, rootful Docker or authentication fallback. Dependencies downloaded normally into task-scoped runner-temp module cache from immutable approved go.mod/go.sum, whose hashes/modes remain read-only; helper build/selftests then offline via existing build.sh. Image pulled through normal public configured Podman method, never auth fallback. All generated synthetic logs/receipts uploaded with limited retention. Files are private locally; artifact access follows repository policy and parent must approve upload.

Default parent-attestation input is intentionally invalid: authorization false, no approved image repository, zero filesystem blocks and invalid fresh measurement timestamp. The provided 08:32 UTC ext4/4096 deployment measurement is context only, not fabricated into reusable approval. Parent must bind a fresh sanitized measurement/source-CI attestation at dispatch; runner rechecks freshness and records comparison evidence. Controller ZFS and read-only /boot ext4 are unsuitable; no production stress or controller runtime run prepared as fallback. CI remains future intended environment, not verified by this preparation.

## Evidence, cleanup and residual gates

Every future run writes private `run-NONCE/receipt.json`, app/driver logs, final states, errors, resource samples, exact identity/configuration, phases, helper hash and cleanup/recovery names. Only names whose nonce ownership label matches may be removed; no prune, broad cleanup or removal of pre-existing resources. Cleanup failures force FAILED_CLOSED and retain recovery coordinates. If runner is interrupted/killed before finally, parent uses those coordinates and matching label only; inspect ownership before any cleanup.

Success status is deliberately `WORKLOAD_PASSED_NOT_DEPLOYMENT_ACCEPTANCE`, never production acceptance. Parent must independently review exact CI/source, immutable identity, all oracle phases/resource samples and fresh actual deployment volume filesystem evidence. Runtime acceptance, engine schema, userns/proc visibility, measured target filesystem equivalence and actual bridge resource behavior remain untested now. Proper WS Close handshake/5-second grace (<=7-second external bound) **not tested**; no claim made. Bounded legitimate runtime/idle-pool FDs may remain; no global-FD-zero or internal-credit-zero assertion.
