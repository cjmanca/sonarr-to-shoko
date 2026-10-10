# shoko-sonarr-linker

Links unrecognized Shoko files to the correct Shoko episodes using Sonarr's
file-to-episode records and Anime-Lists TVDB↔AniDB mappings. Read-only against
Sonarr; Shoko writes are gated. See [PROJECT.md](PROJECT.md) for requirements,
[AGENTS.md](AGENTS.md) for development rules, and
[docs/deployment.md](docs/deployment.md) for the live-only operator runbook.

**Current state: M7-b commands + M9-e2e mocked suite.** The command surface,
durable SQLite state, the daemon cycle engine, the Anime-Lists cache, the safety
attestation lease (`attestation status|record|invalidate`), the backlog campaign
(`campaign prepare|approve|apply|verify|report|pause|resume|revoke`), and the
read-only existing-link audit (`audit`) are wired with offline tests. GHCR
publishing (M8) and delta-fetch light cycles (M9) remain open.

## Install

```powershell
uv sync            # creates/syncs .venv with runtime and dev dependencies
```

## Run

```powershell
$env:SONARR_API_KEY = "<key>"   # API key values live only in the environment
$env:SHOKO_API_KEY  = "<key>"   # the config file stores only the variable NAMES
uv run shoko-sonarr-linker validate config.example.yaml
uv run shoko-sonarr-linker config config.example.yaml --check
uv run shoko-sonarr-linker mappings status config.example.yaml --json
uv run shoko-sonarr-linker mappings refresh --force config.example.yaml
uv run shoko-sonarr-linker attestation status config.example.yaml --json
uv run shoko-sonarr-linker once --dry-run config.example.yaml --max-links 20
uv run shoko-sonarr-linker daemon --cycles 1 config.example.yaml
uv run shoko-sonarr-linker audit config.example.yaml --limit 25 --json
uv run shoko-sonarr-linker report config.example.yaml --json
uv run shoko-sonarr-linker health config.example.yaml
uv run python -m pytest -q
```

## Commands

| Command | What it does | Network |
| --- | --- | --- |
| `validate` | Offline config validation (`--live-read-only` is not implemented in this build) | none |
| `config [path] [--check]` | Resolved config path; `--check` loads and validates | none |
| `once [--dry-run] [--apply] [--policy P] [--max-links N] [--json]` | One reconciliation cycle. Dry-run by default. `--max-links N` caps **POST submissions** (not enumerated files); default `sync.max_new_per_cycle`. `--apply` requires the write gate **and** a valid attestation lease | Sonarr/Shoko reads; writes only through the gated path |
| `daemon [--cycles N] [--once-signal] [--policy P] [--json]` | The polling loop: interval/backoff/jitter from config, apply only when the gate **and** lease open, SIGINT/SIGTERM stop after the current cycle (an idle daemon wakes from the **interruptible** sleep immediately; a signal during a cycle is honoured once that cycle completes, never mid-write). Cycle metadata persists after every cycle, so health/cadence stay fresh. `--cycles N` (and `--once-signal`) bound the loop for tests/CI. Full/light cadence: light cycles poll the same inventories with halved recovery/apply work budgets; `full_reconcile_interval_hours` forces a full reconcile | same as `once` |
| `report [--json]` | Read-only rollup: state counts by state/cohort, detail roll-up, mapping generation + affected candidates, safety fields (attestation verdict, pause, writes-disabled), campaign summaries, outstanding jobs, coverage % with explicit denominators ("based on last cycle inventory"), liveness / reconciliation-ready / apply-eligible levels | none |
| `health [--json]` | Cheap liveness: `alive`, `last_cycle_iso`, `cycle_ok`, `mapping_age_hours`, counts (state/meta + cache only) | none |
| `mappings status [--json]` | Active generation: revision/content/effective/structure digests, age, last attempted/success + outcome, next due, source availability, mapping-affected candidate counts from the state DB. An absent generation is reported with exit 0 (informational) | none |
| `mappings refresh [--force] [--json]` | Due-gated conditional GET of the Anime-Lists source only. Exit 0 for `activated`/`unchanged` (304)/`not_due`; exit 4 on failure (the last-known-good generation stays active) | mapping source only |
| `attestation status [--scope S] [--json]` | Evaluate the configured safety lease **offline** (no network): `valid`/`absent`/`invalid`/`expired`/`stale_version`, the record fields, digest, seconds-to-expiry and the warn-lead flag. Scope is `daemon` (default) or `campaign:<id>` | none |
| `attestation record --endpoint URL --version VER --mounts ITEMS --settings FLAGS [--hours H] [--scope S] --note TEXT --confirm [--json]` | Operator builds the lease from the facts **they physically inspected** (read-only mounts, Shoko version, safety settings flags). Expiry is capped by `safety.attestation_validity_hours`; 0600 is attempted where the platform supports it | none |
| `attestation invalidate --note TEXT --confirm [--json]` | Move the lease aside to `…​.revoked` (the audit trail is never overwritten) and record the action. Applies stay closed until a new lease is recorded | none |
| `campaign prepare [--limit N] [--deadline-hours H] --out FILE [--json]` | Full **dry-run** reconciliation (zero writes) → canonical plan file of backlog candidates with resolved mappings. Prints the **approval digest** (canonical SHA-256 of the plan file) | Sonarr/Shoko reads only |
| `campaign approve --plan FILE --digest HEX [--json]` | Approve the campaign by supplying the digest **separately from the file**; a mismatch refuses (exit 2) and prints both digests. Persists the approved campaign row + per-file bindings | none |
| `campaign apply --plan FILE [--batch-size N] [--json]` | One bounded batch (default/cap `campaign.max_batch_size`) of **approved** candidates through the guarded path. Repeated invocations consume the one approval. Campaign-state refusals (not-approved/revoked/paused/deadline/max-submissions/plan drift/attestation/gate/eligibility) exit 3 with zero POSTs; a malformed plan file exits 2. A queue blocked beyond `sync.queue_blocked_tolerance` **defers** every candidate (zero POSTs, no quarantine) | guarded path for approved files only |
| `campaign verify --campaign-id ID [--json]` | Re-poll verification of campaign sends (read-only cycle; zero POSTs) | XRef GETs only |
| `campaign report --campaign-id ID [--plan FILE] [--json]` | Progress (sent/verified/quarantined/remaining), digests, deadline, pause reason; `--plan` also re-checks the plan file digest. `remaining` = plan files with **no** persisted send progress: any file with send progress counts as consumed, including sends that ended in `conflict` (operator-review rows are never counted as "still to send") | none |
| `campaign pause --campaign-id ID [--reason TEXT] --note TEXT --confirm [--json]` | Persist a campaign pause with a reason | none |
| `campaign resume --campaign-id ID --note TEXT --confirm [--json]` | Resume after re-checking the attestation lease (campaign scope) **and** the write gate | none |
| `campaign revoke --campaign-id ID --note TEXT --confirm [--json]` | Revoke: queued planned/mapped rows return to `waiting` with a reason; rows past `planned` are never rewound; future applies refuse | none |
| `audit [--series-anidb N] [--limit N] [--max-requests N] [--json]` | Bounded **read-only** existing-link audit (GETs only, zero writes): classifies already-linked Shoko files against the verified mapping as `linked_consistent` (exact identity-set equality only), `partial_link`, `existing_link_mismatch`, `wrong_first_episode`, `ambiguous_match`, plus `no_sonarr_counterpart`/`audit_incomplete` buckets | Shoko/Sonarr GETs within the request budget |
| `resolve-send --file-id N --fingerprint FP (--release\|--block) --note TEXT --confirm [--json]` | Operator close of a live terminal-review send (releases the budget slot). Refuses verified/closed rows. A closed send authorizes only a **fresh** re-plan fingerprint | none |
| `clear-requires-rebaseline --note TEXT --confirm [--json]` | Operator clear of the persisted source-discontinuity flag | none |
| `steal-apply-lock --note TEXT --confirm [--json]` | Take over a **dead** owner's apply lease: it requires the OS advisory lock (`apply.lock`) to be **free**, so a live owner (kernel-held lock) refuses and names the holder. An expired meta lease with a free OS lock is audit-recorded and the OS lock is released | none |
| `resume-writes --note TEXT --confirm [--json]` | Clear the stopped-writes marker and the write pause | none |
| `pause-writes --reason TEXT [--hours H] --note TEXT --confirm [--json]` | Persist a write pause with a reason | none |

Operator commands mutate durable state, so they require `--confirm` plus a `--note`;
they refuse (exit `5`) while state writes are stopped, except `resume-writes`.
`--json` output is nonsecret everywhere: reports carry API-key environment-variable
**names**, never values, and never raw Shoko settings dumps. In `--json` mode stdout is
**exactly one JSON document**: the human config summary and the `daemon` startup preamble
are suppressed, so `json.loads(stdout)` parses cleanly (diagnostics go to the log stream).

Configuration is YAML (`config.example.yaml` is the full example; every accepted
field is documented there).

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success — completed cycle, mapping `activated`/`unchanged` (304)/`not_due`, informational `mappings status`/`report`/`health`/`audit` (an absent generation/state is reported, not an error), a confirmed operator action, a clean `daemon` exit, a campaign `prepare`/`approve`/`apply`/`verify`/`report`/`pause`/`resume`/`revoke` that completed |
| `2` | Configuration/request invalid (unknown policy, unconfirmed operator command, bad flags, a malformed plan file — on **any** campaign command including `apply`, plan-file parse/digest-format errors are invalid input —, an `approve` digest mismatch — the refusal prints **both** digests; the message names the failed field, never a secret) |
| `3` | Apply-gate refusal (`--apply`/campaign `apply` with the default-deny gate shut) **and** campaign-state apply refusals: campaign not approved, revocation, campaign pause, deadline, max-submissions, plan drift, attestation, eligibility |
| `4` | Runtime/network failure (mapping-source fetch failure, transport-failed cycle, an audit whose Shoko listing failed, `validate --live-read-only` not implemented in this build) |
| `5` | State error (the state store refused: illegal transition, held apply lock, stopped writes, record/budget refusals) |

## Write safety: gate **and** attestation

Live Shoko writes require **all** of:

1. `sync.apply_changes: true` in the config, **and**
2. the operator environment variable `SHOKO_LINKER_ALLOW_APPLY=1`, **and**
3. a **valid safety attestation lease** (`safety.attestation_file`) whose endpoint
   identity matches the configured Shoko URL, whose scope matches the caller
   (`daemon` for `once --apply`/`daemon`, `campaign:<id>` for a campaign apply),
   that has not expired, and that matches the configured version hint when one
   is supplied.

No command-line flag opens the gate by itself.

### Safety attestation lease (lifecycle)

The lease is an **operator-created** YAML/JSON record (`attestation record … --confirm`)
of the physical facts the software cannot observe: the Shoko endpoint identity, the
inspected Shoko version, read-only-mount summaries, and the four safety settings
flags (`FileQualityFilterEnabled`, `RenameOnImport`, `MoveOnImport`, `MyListAddFiles`
— recorded values only, never a raw settings dump). Its integrity is a canonical
SHA-256 digest: **the trust anchor is the operator**, so the digest is tamper
*evidence*, not a security boundary. Keep the file readable only by the operator
(0600 where the platform supports it; the tool attempts it and warns when it cannot).

Lifecycle:

1. Inspect the deployment (see [Deployment safety](#deployment-safety)), then
   `attestation record --endpoint … --version … --mounts … --settings … --hours N --scope daemon --note … --confirm`.
   `--hours` may not exceed `safety.attestation_validity_hours` (default 168). When
   `safety.attested_shoko_version` is configured, the lease's `--version` must equal it
   (a Shoko upgrade therefore requires re-inspection + a new lease, never an inherited one).
2. `attestation status` evaluates the lease offline; a valid lease expiring within
   `safety.attestation_warn_seconds` (default 3600) reports `warn_before_expiry` and
   the daemon logs a warning — apply still proceeds.
3. When the lease is absent/invalid/expired/scope-mismatched/stale-version, **every**
   apply is suppressed: the daemon persists exactly one write pause
   (`attestation_expired`) per valid→invalid transition, warns, and **stays alive
   reporting read-only** (never a restart loop). A re-recorded lease clears only a
   pause whose reason is `attestation_expired`.
4. `attestation invalidate --confirm` moves the lease to `…​.revoked` (audit trail
   preserved) and records the operator action; applies stay closed.

**Documented tradeoff (PLAN M7):** a completely unattended *indefinite* writable
daemon is incompatible with an expiring physical-safety attestation. The daemon
therefore degrades to read-only reporting after expiry; writes resume only when a
human re-attests. Unattended campaign apply has the same bound: a campaign
apply needs a lease scoped `campaign:<id>`.

### Existing-link audit

`audit` is bounded and read-only (`--limit` linked files, `--max-requests` Shoko GETs,
default 100 each; zero POSTs by construction). It **includes already-recognized
files** (the discovery path deliberately excludes them) and classifies observed
AniDB cross-references against the verified mapping generation. `linked_consistent`
fires **only on exact identity-set equality** — the existence of a link never marks
a link correct. Findings are informational (exit 0); only a transport
failure exits 4. The audit never unlinks, never repairs, consumes no state DB and no
budget slots.

### Backlog campaign walkthrough

One approval, many bounded batches (all mutating writes flow through the same
M5 guarded path and the M6 durability machine):

```powershell
# 1. Dry-run reconciliation → canonical plan + approval digest (zero writes)
uv run shoko-sonarr-linker campaign prepare config.yaml --limit 500 --out plan.json --json

# 2. Canary review of the plan file (dry-run only, zero writes): inspect plan.json —
#    the exact file list, paths/sizes, AniDB identities, and the approval digest.
#    There is no canary `campaign apply` at this point: nothing is approved yet, so
#    apply would refuse (the campaign is not in the state DB).

# 3. Approve with the digest supplied SEPARATELY from the plan file
uv run shoko-sonarr-linker campaign approve --plan plan.json --digest <approval-digest> --json

# 4. Record the campaign-scope lease, then run the bounded canary batch (one file)
uv run shoko-sonarr-linker attestation record --endpoint http://shoko:8111 --version 5.3.3 `
  --mounts "/anime read-only (host-inspected)" `
  --settings "FileQualityFilterEnabled=false,RenameOnImport=false,MoveOnImport=false,MyListAddFiles=false" `
  --hours 24 --scope "campaign:<campaign-id>" --note "canary window" --confirm
uv run shoko-sonarr-linker campaign apply --plan plan.json --batch-size 1 --json

# 5. Verification + progress for the canary batch (read-only, zero POSTs)
uv run shoko-sonarr-linker campaign verify --campaign-id <campaign-id> --json
uv run shoko-sonarr-linker campaign report --campaign-id <campaign-id> --plan plan.json --json

# 6. Remaining bounded batches (repeat; the one approval is consumed, never re-approved)
uv run shoko-sonarr-linker campaign apply --plan plan.json --json   # ≤ campaign.max_batch_size files
```

Rules that the apply path enforces, with persisted evidence:

- **Plan drift** (the plan file's digest no longer equals the stored approval
  digest) → campaign paused (`plan_drift`), **zero POSTs**.
- **Candidate drift** (path/size/fingerprint changed) → the file is **quarantined**
  (`campaign_quarantine` reason; row goes to `blocked_safety`/`waiting`) —
  never silently replaced, never auto-approved.
- **Pending targets**: a file approved with an empty `shoko_episode_ids`
  list and preapproved AniDB identities resolves its Shoko IDs at apply time; the
  resolved set must equal the preapproved set **exactly** (mismatch → quarantine
  `target_drift`, zero POSTs for it). The approved set **never expands**: a file the
  plan file does not name is never processed, and a newly resolvable mapping for a
  non-approved file never enters the campaign.
- Deadline passed / `max_submissions` reached → refuse (the budget one auto-pauses).
- Global churn, mapping drift, writes-disabled, source discontinuity, lease expiry →
  campaign pause persisted + report.
- Pause/resume/revoke are operator commands: revoke returns queued planned/mapped
  rows to `waiting` with a reason and refuses future applies.
- `campaign report`'s `remaining` counts plan files with **no** persisted send progress,
  so every file with any send progress is excluded — including `conflict`-state sends
  (operator-review rows are never re-counted as still-to-send).

## Safety defaults

- **Dry-run is the default.** `once`/`daemon`/`campaign prepare` run the full read
  cycle and persist local state, and make no remote change; Shoko writes happen only
  in apply mode, only through `linking.submit_link`, only after `preflight` and the
  durable `prepare_send` → `mark_sent` ordering.
- **Live writes are default-deny** and open only through `shoko_linker.config.apply_gate`:
  both `sync.apply_changes: true` in the config **and** the operator environment variable
  `SHOKO_LINKER_ALLOW_APPLY=1`, **and** a valid in-scope attestation lease. `once --apply`
  refuses with exit `3` otherwise; no CLI flag enables writes by itself.
- **Secrets are environment-only.** Config holds the variable name; values are read at
  access time, never stored in settings, reports, `repr`, and scrubbed from logs by a
  redaction filter **and** a redacting formatter (tracebacks included) registered at
  client creation. Redaction is literal-substring scrubbing of registered values:
  values shorter than 8 characters are not registered (scrubbing them would erase
  ordinary words) — client creation logs a warning naming only the length — real
  Sonarr/Shoko keys are long, and request logging must keep keys out of URLs entirely.
- **Scope fails closed.** `sonarr.allowed_roots`, `shoko.allowed_roots` and
  `paths.mappings` must be non-empty. Root matching is path-component-aware
  (`/anime` never matches `/anime-old`), paths are compared case-sensitively, and nested
  or conflicting mappings are rejected at load time.
- **No automatic retries.** Clients are built with `follow_redirects=False`,
  `retries=0`, and explicit timeouts. Redirects are followed only by
  `http.resolve_and_send`, which caps hops, and drops auth headers when a redirect
  target's origin differs. Non-idempotent requests are never replayed (HTTP 303 after a
  POST is refused).
- **No media file access.** This tool never opens, renames, moves, or deletes media.
- **A blocked Shoko queue suppresses apply.** Every apply consults the read-only
  `GET /Queue` snapshot: `BlockedCount` above `sync.queue_blocked_tolerance`
  (default **0** — any blocked work) refuses all submissions (`queue_blocked`, zero
  POSTs, reported per cycle; campaign candidates are **deferred**, never quarantined),
  and a queue read failure refuses apply too (`queue_unknown`). Delivery of the
  async `ManualLinkJob` chain cannot be trusted on a blocked queue.
  **Freshness bound:** the snapshot is taken once per daemon apply **cycle** and once
  per campaign apply **batch** (plus each send's inline verification window), so a queue
  that becomes blocked *after* the snapshot is not seen until the next cycle/batch —
  the guard bounds the exposure window, it is not a race-free guarantee (documented M5
  design limit).
- **New files get a grace window before their first submit.** In apply cycles (`once --apply`
  and `daemon` apply) a `new_arrival`-cohort file is deferred with the `grace_period` reason
  while `now − first_seen_iso < sync.new_file_grace_period_seconds` (default **300**; `0`
  disables the window). The row stays `planned`, consumes **no** budget slot, and is
  never quarantined; the first cycle that starts after the window closes submits it.
  Shoko's own post-arrival import/XRef state settles during the window, so submitting in
  the arrival cycle is not attempted. Backlog and campaign files are **not** grace-gated:
  the daemon never auto-submits them, and a campaign apply is the operator's act on aged,
  plan-approved inventory.

## Configuration budgets (M7-a/M7-b)

`sync` carries the cycle/budget knobs (defaults shown in `config.example.yaml`):
`max_new_per_cycle` (default 20; also bounds pending file requests), `rolling_new_limit`
(200), `rolling_window_hours` (168), `max_pending_episode_jobs` (10),
`max_cycle_seconds` (300), `max_recovery_rows_per_cycle` (100),
`verification_timeout_seconds` (3600), `apply_lock_lease_seconds` (3600),
`jitter_ratio` (0.1, max 0.5), `mapping_churn_limit` (500),
`new_file_grace_period_seconds` (300 — daemon-apply grace window for `new_arrival`
files; deferrals are reason `grace_period`, no budget consumed; `0` disables it, see
[Safety defaults](#safety-defaults)) and
`queue_blocked_tolerance` (0 — fail-closed Shoko queue guard, see below).

`safety` carries the attestation lease: `attestation_file` (null = no lease → every
apply refused `attestation_invalid`), `attestation_validity_hours` (168, the hard
lease-lifetime cap), `attestation_warn_seconds` (3600, the pre-expiry warning lead) and
`attested_shoko_version` (null = no version pin; when set, a lease recorded for a
different Shoko version evaluates `stale_version` and every apply is suppressed until
re-inspection).
`campaign` carries `max_campaign_files` (7000, the plan bound and default
`prepare --limit`) and `max_batch_size` (10, the per-invocation apply cap). These
ceilings never bypass the write gate, the lease, the state budgets, or the plan
approval digest.

Out-of-range values (NaN/inf, negatives where nonsense, zero where > 0, non-whole
numbers) are refused at load time with exit `2`.

`apply_lock_lease_seconds` is an **audit/visibility** window only, never a lock timeout:
exclusive apply ownership is fenced by an OS advisory lock file (`apply.lock`, a sibling
of the state DB) that is held for the whole apply operation and freed by the kernel
when the owning process exits. A live apply operation therefore **never** loses ownership
because a fixed lease elapsed — a second live process (a restart race, an operator
campaign) is refused, and `steal-apply-lock` refuses too. The supported deployment is
one container on a local (or cache-backed) volume state directory; like the SQLite caveat,
`flock`/byte-range locks are unreliable on SMB/NFS.

`report` coverage percentages are computed from **state-DB inventory counts**
(files tracked vs verified, with explicit denominators) and note that they reflect the
last cycle inventory.

## Deployment runbook

The live-only operator procedure is in
[docs/deployment.md](docs/deployment.md): read-only preflight, Shoko safety
inspection, attestation lease, one-file canary, campaign canary, backup,
rollback, and live acceptance checks. The mocked M9 end-to-end suite is offline
evidence only; live validation remains operator work.

## Deployment safety

The daemon never mounts media, and that stays true for a reason: **Shoko's
`ManualLinkJob` chain runs server-side after an accepted link** and can trigger
file-quality-filter deletions, renames/moves, plugin reactions, and AniDB MyList
writes (docs §2). Before recording any attestation lease, inspect the **Shoko
container's own mounts on the host**:

```powershell
docker inspect --format "{{json .Mounts}}" <shoko-container>   # verify media mounts are :ro
```

Checklist the lease records (operator-inspected, human-verified):

- Shoko's import-folder media mounts are **read-only** on the host; the Shoko-side
  `ImportFolder` list has no drop folders (`DropFolderType: None` for Sonarr-owned
  paths).
- Shoko settings: `FileQualityFilterEnabled=false` (quality filter deletes
  non-linked files), `Plugins.Renamer.RenameOnImport=false`,
  `Plugins.Renamer.MoveOnImport=false` (rename/move physically relocates files),
  `AniDb.MyList_AddFiles=false` (every link writes to AniDB MyList upstream).
  Record the values you inspected — never a raw settings dump.
- Sonarr is read-only: the Sonarr API key used here has read scope only.
- `/data` persistence: the SQLite state DB and the Anime-Lists cache live in the
  `/data` volume. Keep it on a **local** disk/cache mount (SQLite locking is
  unreliable on SMB/NFS; Unraid: use an appdata/local-cache path, not a network share).
- UID/GID: the image runs as a non-root user and writes `/data` as UID/GID 1000 —
  set the share owner (`chown 1000:1000 <appdata path>`) instead of running as root.
- Networks: join the existing Sonarr/Shoko Docker network so the configured URLs
  resolve (see [compose.example.yaml](compose.example.yaml)); no ports are exposed.

### Backup and restore

Back up with the tool stopped (`daemon`/`once` closed): copy
`/data/state.sqlite3*` (include the `-wal`/`-shm` siblings when present) and the
derived `mappings/` directory. Restoring to a fresh volume is just the copy back —
the schedule, budgets, campaign rows, and last-known-good generation all live in
those files. Never edit the SQLite file by hand.

### Rollback

Writes are reversible in *state* only — Shoko-side links are never unlinked by
this tool (out of scope): `pause-writes --reason … --confirm` (stops all apply),
`attestation invalidate --confirm` (applies closed), `campaign revoke --confirm`
(queued approved rows return to `waiting`), `resume-writes --confirm` to re-open.
A sent-but-unverified file is verified/held, never resubmitted; close it with
`resolve-send --release --confirm` after inspection.

## Images (GHCR)

Publishing is defined by [`.github/workflows/publish.yml`](.github/workflows/publish.yml)
and the publishing policy in PROJECT.md §6: an immutable **SHA-tagged image for every
pushed commit** (including commits inside multi-commit pushes), `latest` only for the
newest successfully built default-branch revision, never from untrusted pull requests,
least-privilege `GITHUB_TOKEN`.

> **EXTERNAL/UNVERIFIED:** the workflow is validated offline (enumeration tests,
> YAML structural tests, permission gates). No GHCR publication has been performed from
> this session — no pushes were made and publishing was never exercised against GitHub.

**Publishing semantics.** The `plan` job runs `scripts/enumerate_introduced.py`
over the push event (`before..after`; first-push from the first-buildable boundary
`aa6b85e` inclusive, older history skipped; force-push/non-ancestor falls back to the
merge-base; branch pushes enumerate from `merge-base(main, head)` so shared history
is never re-tagged). Each enumerated SHA becomes one matrix leg: checkout that SHA,
**re-run the full check list for that commit** (`scripts/ci_checks.sh` — same commands
as CI, per commit, never a cached pass), then build & push
`ghcr.io/<owner>/shoko-sonarr-linker:sha-<full-sha>` (provenance on, labels point at
the commit, registry cache scoped per SHA so builds can't contaminate each other).
A failing intermediate SHA skips only its own tag and is reported in the job summary;
the rest of the push publishes normally. PR events run the read-only test job only —
publish/promote jobs are gated to push/schedule/dispatch and fork PRs get a read-only
token, so untrusted code can never publish. SHA-tag publishing is serialized **per
ref** (`concurrency: publish-<ref>` with `cancel-in-progress: false`) so a queued
commit's `sha-` tag is never silently dropped by a cancel. `latest` is promoted **only** from the
default branch by `promote-latest`, serialized (`concurrency: latest-promotion`,
`cancel-in-progress: false`) and guarded: it aborts if the run's SHA is no longer
`git ls-remote`-confirmed head of main, so a queued old run can never revert `latest`.
SHA tags are immutable in practice: names are derived from the commit SHA and backfill
skips already-tagged SHAs, so tags are never overwritten.

**Shell-injection hygiene & trust boundary.** No `${{ }}` expression is expanded inside
a `run:` script body — `github.*`/`inputs.*` values reach the shell only through job/step
`env:` indirection, and dispatch `from_sha`/`to_sha` inputs are validated as 40-char
lowercase hex before use. Dispatch trust boundary: `workflow_dispatch` requires repository
**write access**, and write access is already a code-execution trust boundary (anyone who can
dispatch can run code here), so the dispatch gates are defense-in-depth, not a secret
boundary: the plan additionally refuses any range whose `to_sha` is not an ancestor of the
current main head (`not-ancestor-of-main`). A non-positive `max_shas` fails closed.

**Action pins.** Every `uses:` ref in `publish.yml`/`ci.yml` is pinned to a full commit
SHA with its release tag in a trailing comment (SHAs fetched from the GitHub refs API,
2026-10-09, newest patch of each major). The Dockerfile base image is pinned by digest
and installs strictly from `uv.lock` (`uv sync --frozen --no-dev`, which fails if the
lock is stale, with `uv pip check` verifying the environment).

**Auth.** Only `secrets.GITHUB_TOKEN` with `contents: read` + `packages: write` on the
publish/promote jobs — no repository secrets, no Sonarr/Shoko keys exist in CI.

**Event gap.** GitHub does **not** fire `push` workflows for commits created by
workflows using `GITHUB_TOKEN` (merge queues, auto-commits). This project never
self-commits; if such a commit ever lands, recovery is via the weekly `schedule`
backfill or a dispatch — capped per run (10 by default; `max_shas` must be positive,
non-positive values fail closed), oldest-first, overflow
reported and recovered, never dropped:

```powershell
gh workflow run publish.yml -f backfill=true -f from_sha=<sha> -f to_sha=<sha>
gh workflow run publish.yml -f backfill=true   # whole-history tag-gap scan
```

Pin deployments to the SHA tag (and record the image digest) — `latest` is a moving
label, not a safety boundary:

```yaml
image: ghcr.io/<owner>/shoko-sonarr-linker:sha-<full-sha>   # + record the repo digest
# strongest: pin the digest directly, immune to tag movement:
image: ghcr.io/<owner>/shoko-sonarr-linker@sha256:<digest>
```

## Not implemented in this build

- `validate --live-read-only` refuses immediately with exit `4` and makes no network call.
- Delta-fetching **light** cycles: light cycles poll the same inventories with reduced
  work budgets (halved recovery-row cap and submission cap); true Sonarr/Shoko delta
  endpoints are not invented and remain the M9 optimization.
- GHCR publishing runs in GitHub Actions — publication has not yet been exercised
  live (EXTERNAL/UNVERIFIED); build locally with `docker build -t shoko-sonarr-linker:local .`.
