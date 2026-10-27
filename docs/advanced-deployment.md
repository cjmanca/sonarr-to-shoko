# Advanced deployment safety runbook — live-only operator procedure

This runbook preserves the advanced YAML/apply workflow for existing operator
deployments. The normal service installation is documented in [README](../README.md)
and [deployment](deployment.md). The project suite is mocked and offline; the
commands below make live steps explicit and separate from test evidence.

Do not enable live writes in one action. First complete read-only reconciliation,
then inspect Shoko safety, record an attestation lease, apply a one-file canary,
verify, and only then widen the batch.

## 0. Known live-operation limits

- **No live validation has been run from this workspace.** All test
  evidence in this build is mocked/offline.
- **GHCR publication is defined but unverified.** Build the image locally for the
  first live canary unless a published SHA image has already been validated.
- **The publication pipeline's registry logic (M8 F2/F3) is mocked offline only.**
  The plan job discovers already-published images from the GHCR OCI registry via
  `scripts/ghcr_tags.py` (git refs are independent of container tags, so the
  registry is the publication state); any registry failure class (auth,
  permission, network, unavailable) aborts the run and never publishes against
  unknown state, and an existing `sha-<fullsha>` tag counts as published only
  after a bounded, best-effort manifest/config provenance check
  (`org.opencontainers.image.revision` == full SHA; unverified tags are
  re-scheduled, which is safe because `sha-` tags are content-addressed).
  Publish legs for commits that predate `scripts/ci_checks.sh` validate the
  historical source with the artifact-delivered `scripts/validate_historical.py`
  fallback. The first authorized live workflow run is the external validation
  of all of this; publication stays EXTERNAL/UNVERIFIED.
- **Workflow lint/security scanning is substituted locally.** `actionlint`
  and `zizmor` are not installed on the development host; the structural PyYAML
  assertions in `tests/test_workflows.py` (permissions, gates, PR isolation,
  SHA pins, env-indirection hygiene, image-reference validation, registry
  discovery wiring, historical fallback) plus manual review stand in for them.
- **Campaign apply progresses across many bounded batches (F9).** One approval
  covers the whole finite campaign; each apply cycle processes a bounded batch
  (≤ `campaign.max_batch_size`) through the same guarded path, and durable,
  restart-resilient per-cycle ids mean a restart never replays a completed
  send. A batch that makes zero progress pauses with a persisted reason and zero
  POSTs; `source_discontinuity` is a **named deferral** in that set (the campaign's
  own accepted links shrink the Shoko `Unrecognized` discovery total), not a build
  defect. The automatic daemon drain needs the explicit `campaign authorize-drain`
  confirmation on a settled canary, and re-evaluates the applicable lease before
  every submission (per-submission freshness, F11).
- **True delta-fetch light cycles are not implemented.** Light cycles use the
  same inventories with reduced work budgets.

## 1. Deployment shape

Use a non-root container with one writable path:

- Image: `shoko-sonarr-linker:local` from a local build, or a published
  `sha-<full-sha>` GHCR tag after publication has been verified.
- Writable mount: `/data` for SQLite state and the Anime-Lists cache.
- Config mount: `/config` read-only.
- API keys: environment variables only, e.g. `SONARR_API_KEY` and
  `SHOKO_API_KEY`. Config stores only variable names.
- Network: join the existing Sonarr/Shoko Docker network.
- No ports are exposed.
- No media-library mount is needed by this tool.

Example files:

- [`config.example.yaml`](../config.example.yaml)
- [`compose.example.yaml`](../compose.example.yaml)

## 2. Build the image

From a clean source checkout:

```powershell
docker build -t shoko-sonarr-linker:local .
```

Do not deploy a moving tag. Prefer an image pinned by SHA tag, and record the
repo digest used for the deployment.

## 3. Configure read-only mode

Start from `config.example.yaml`. The safe defaults are:

```yaml
sync:
  apply_changes: false
```

Required choices are deployment-specific:

```yaml
sonarr:
  url: http://sonarr:8989
  api_key_env: SONARR_API_KEY
  allowed_roots:
    - /data/media/anime

shoko:
  url: http://shoko:8111
  api_key_env: SHOKO_API_KEY
  allowed_roots:
    - /anime

paths:
  mappings:
    - sonarr: /data/media/anime
      shoko: /anime

database:
  path: /data/state.sqlite3

safety:
  attestation_file: /data/attestation.yaml
```

Rules:

- Root lists must be non-empty.
- Path mappings must be component-boundary prefixes and unambiguous.
- Shoko live-action/manual-drop roots must be excluded by allowing only the
  Sonarr-owned anime roots.
- `/data` should be local disk/cache storage. Avoid SMB/NFS for SQLite.
- The application writes `/data` as `PUID:PGID` (default `1000:1000`; Unraid
  template `99:100`). Startup adjusts ownership inside the dedicated state
  directory as root, then drops privileges before running the tool. See
  [deployment guidance](deployment.md) for Docker `--user` support.

## 4. Read-only preflight

Set API key env vars in the environment, then run:

```powershell
uv run shoko-sonarr-linker validate config.yaml
uv run shoko-sonarr-linker config config.yaml --check
uv run shoko-sonarr-linker mappings refresh --force config.yaml
uv run shoko-sonarr-linker mappings status config.yaml --json
uv run shoko-sonarr-linker once --dry-run config.yaml --max-links 20 --json
uv run shoko-sonarr-linker audit config.yaml --limit 25 --json
uv run shoko-sonarr-linker report config.yaml --json
uv run shoko-sonarr-linker health config.yaml
```

Expected outcome:

- `mappings refresh --force` exits `0` with outcome `activated` on first run.
- `once --dry-run` makes zero Shoko POSTs.
- `audit` makes GETs only and is informational.
- `report` shows tracked files, cohort counts, mapping generation, safety fields,
  outstanding jobs, and coverage with explicit denominators.
- Existing-link audit findings are reviewed manually; the audit never
  repairs links.

## 5. Shoko safety inspection before an apply lease

The tool never touches media files, but Shoko's server-side
`ManualLinkJob` chain can trigger media side effects after a link is accepted.
This step is human-operated and mandatory before recording any lease.

Check the Shoko container's host mounts:

```powershell
docker inspect --format "{{json .Mounts}}" <shoko-container>
```

Confirm:

- Shoko media mounts are read-only where required by your deployment policy.
- Shoko import folders for Sonarr-owned anime paths have `DropFolderType: None`.
- Shoko settings inspected through Shoko UI/admin only:
  - `FileQualityFilterEnabled=false`
  - `Plugins.Renamer.RenameOnImport=false`
  - `Plugins.Renamer.MoveOnImport=false`
  - `AniDb.MyList_AddFiles=false`
- Sonarr API key used by this tool is read-only.
- Shoko queue is healthy. Apply is fail-closed when queue reads fail
  or when blocked work exceeds `sync.queue_blocked_tolerance` (default `0`).

Record only inspected facts. Never put raw settings dumps, API keys, media
file mutations, or secrets into notes.

## 6. Record the daemon lease

Choose a validity window not above `safety.attestation_validity_hours`. For a
canary, a short window is safer.

```powershell
uv run shoko-sonarr-linker attestation record config.yaml `
  --endpoint http://shoko:8111 `
  --version 5.3.3 `
  --mounts "/anime read-only (host-inspected)" `
  --settings "FileQualityFilterEnabled=false,RenameOnImport=false,MoveOnImport=false,MyListAddFiles=false" `
  --hours 24 `
  --scope daemon `
  --note "live canary window" `
  --confirm

uv run shoko-sonarr-linker attestation status config.yaml --json
```

The lease must be valid for `daemon` apply. If it is absent, invalid, expired,
scope-mismatched, version-stale, or invalidated, applies are suppressed and the
daemon degrades to read-only reporting.

### Daemon and campaign leases are separate attestations

Two independent safety attestations gate writes, each recorded and evaluated
by scope:

- The **daemon lease** is the file at `safety.attestation_file`
  (`/data/attestation.yaml`), scope `daemon`. It gates `once --apply` and the
  daemon's own new-arrival/backlog apply.
- Each **campaign lease** is a sibling in the same directory named
  `<stem>.campaign-<id>.yaml` — for the example above,
  `/data/attestation.campaign-<campaign-id>.yaml` — with scope `campaign:<id>`. It
  gates that campaign's batches: a manual `campaign apply` **and** the daemon's
  automatic drain of the same campaign.

`attestation record`, `status`, and `invalidate` all take `--scope` (default
`daemon`). Recording a campaign lease never clobbers the daemon lease, and
`invalidate --scope "campaign:<id>"` never touches the daemon lease. Each lease
expires and pauses **only its own scope**, and both survive a restart: a campaign
can drain under a valid campaign lease even when the daemon lease is invalid, and
vice versa. Evaluate a scope's verdict explicitly:

```powershell
uv run shoko-sonarr-linker attestation status config.yaml --scope daemon --json
uv run shoko-sonarr-linker attestation status config.yaml --scope "campaign:<campaign-id>" --json
```

### Apply-ownership fence (one apply writer per state directory)

Exclusive apply ownership is fenced by an **OS advisory lock file** `apply.lock`,
created next to the state DB (`settings.database.path` directory). The daemon, a
`campaign apply` batch, and `once --apply` take it before any guard mutation and hold it
for the whole operation; `store.close()` (and the kernel on process exit) releases it.

- A **live** apply operation never loses ownership just because a lease timer elapsed.
  `sync.apply_lock_lease_seconds` is **audit/visibility only**, not a lock timeout: a
  second live process (a restart race, an operator `campaign apply`) is refused with
  reason `apply_lock_contended:<holder>` and mutates nothing.
- A long campaign batch that runs past the lease is safe: the OS lock is held for the
  full batch, so no other live process can overlap it.
- A **crashed** owner's lock is freed by the kernel automatically; the next cycle takes
  ownership normally (an expired lease + a free OS lock is a legitimate takeover of a
  dead owner, recorded in the `apply_lock_steals` audit).
- `steal-apply-lock` now requires the OS lock to be **free**: a live owner refuses (exit 5,
  names the holder), so a running daemon can never be fenced out by the operator command.
  A successful takeover records the `apply_lock_steals` audit and writes an
  **immediately-expired marker lease**, so the next daemon/campaign acquire takes ownership
  without a stall window.

Supported deployment: **one container, local (or cache-backed) volume** state directory. As
with the SQLite caveat, `flock`/byte-range locks are unreliable on SMB/NFS — never place the
state directory on a network share. **Never delete, move, or replace `apply.lock`**: advisory
locks bind to the inode, and recreating the file mid-run silently un-fences the current owner
(treat it exactly like the state DB).

## 7. Enable the write gate for a one-file canary

Two independent controls must both be on:

1. Config:

```yaml
sync:
  apply_changes: true
```

2. Process environment:

```powershell
$env:SHOKO_LINKER_ALLOW_APPLY = "1"
```

Then apply one file:

```powershell
uv run shoko-sonarr-linker once --apply config.yaml --max-links 1 --json
```

Immediately verify:

```powershell
uv run shoko-sonarr-linker campaign verify config.yaml --campaign-id <id> --json
uv run shoko-sonarr-linker report config.yaml --json
uv run shoko-sonarr-linker health config.yaml
uv run shoko-sonarr-linker audit config.yaml --limit 25 --json
```

Acceptance for the canary:

- Exactly the intended number of POSTs is recorded.
- The file reaches `verified` (or stays `verifying` if Shoko's async job
  has not materialized yet; do not resubmit manually).
- No indeterminate send remains open.
- No unexpected existing-link audit drift appears for the canary file.
- Shoko media state is unchanged by inspection.

If any acceptance check fails:

```powershell
uv run shoko-sonarr-linker pause-writes config.yaml --reason "canary review" --note "operator stop" --confirm
uv run shoko-sonarr-linker attestation invalidate config.yaml --note "canary review" --confirm
```

Do not clear state flags until the report is understood.

## 8. Campaign canary procedure

Prepare the **complete intended campaign first**, then canary a small subset from
the *approved* plan. The approved plan is a finite backlog: there is no
un-approved "remainder" — every file the daemon may later drain is already in the
approved plan, and the canary is just the first `--batch-size 1` slice of it.

```powershell
# Prepare the whole finite campaign. No --limit ⇒ campaign.max_campaign_files
# (default 7000); --limit N narrows the plan, --deadline-hours H bounds the
# campaign window (default 168), --out FILE writes the canonical plan. This is a
# zero-write dry-run reconciliation that prints the approval digest.
uv run shoko-sonarr-linker campaign prepare config.yaml --out plan.json --json
# Approve by supplying the printed approval digest SEPARATELY from the file.
uv run shoko-sonarr-linker campaign approve config.yaml --plan plan.json --digest <approval-digest> --json
# Record THIS campaign's own lease (scope campaign:<id>; a sibling of the daemon lease).
uv run shoko-sonarr-linker attestation record config.yaml `
  --endpoint http://shoko:8111 `
  --version 5.3.3 `
  --mounts "/anime read-only (host-inspected)" `
  --settings "FileQualityFilterEnabled=false,RenameOnImport=false,MoveOnImport=false,MyListAddFiles=false" `
  --hours 24 `
  --scope "campaign:<campaign-id>" `
  --note "campaign canary" `
  --confirm
uv run shoko-sonarr-linker attestation status config.yaml --scope "campaign:<campaign-id>" --json
# Apply ONLY a one-file canary subset from the approved campaign.
uv run shoko-sonarr-linker campaign apply config.yaml --plan plan.json --batch-size 1 --json
uv run shoko-sonarr-linker campaign verify config.yaml --campaign-id <campaign-id> --json
uv run shoko-sonarr-linker campaign report config.yaml --campaign-id <campaign-id> --plan plan.json --json
# After the canary settles and physical-media inspection passes, authorize the drain.
uv run shoko-sonarr-linker campaign authorize-drain config.yaml --campaign-id <campaign-id> `
  --note "canary inspected: renames/deletions/MyList side effects reviewed" --confirm
```

The canary is the operator's **first** campaign batch: once it completes and settles — every
submitted campaign file is `verified` (an attempted/unverified, `partial`, `indeterminate_write`,
or `conflict` row refuses the authorization) — and physical-media inspection passes, the operator
records the explicit, digest-bound **drain authorization** above. Only then does the daemon drain
the rest of the approved campaign automatically. Keep the daemon's campaign-scope lease valid for
the drain window (`campaign.daemon_auto_apply: true`, default). The manual `campaign apply` command
remains available for a batch at any time (it never requires the authorization); a daemon batch and
a concurrent CLI `campaign apply` contend for the single apply lock, so exactly one
writer runs. `campaign revoke-drain --confirm` withdraws the authorization later: the daemon
skips the campaign (`drain_revoked` in `campaign report`) with zero further automatic submissions,
while manual applies stay available.

### Campaign deadline and lease renewal

`campaign prepare` warns when the configured rolling submission capacity makes the requested
deadline implausible; it does not reject the plan or silently extend its deadline, the submission
limit, or the safety-attestation lease. The optimistic minimum wait until the last rolling-budget
window is `(ceil(files / rolling_new_limit) - 1) × rolling_window_hours`. With the defaults of 200
submissions per 168-hour window, a 7,000-file campaign needs about 5,712 hours (238 days) before
its final window can start. For example, explicitly choose a longer campaign deadline when preparing
that backlog:

```powershell
uv run shoko-sonarr-linker campaign prepare config.yaml --deadline-hours 6000 --out plan.json --json
```

This is an optimistic lower bound: transient failures, pending Shoko jobs, and verification delays
can extend completion time. A zero rolling submission limit makes completion impossible until the
operator changes the configuration. Safety-attestation expiry is separate; a long campaign may
require periodic physical-safety reinspection and lease renewal even when its campaign deadline is
long enough. Raising `--deadline-hours` does not renew or extend that lease.

Campaign drain rules the daemon enforces every batch (identical to `campaign apply`):

- **Canary first, then authorization.** The daemon drains an approved campaign only after the
  operator's canary settled AND the digest-bound `campaign authorize-drain` confirmation; a
  newly approved (unauthorized) campaign is never drained. A plan fingerprint/digest change
  invalidates the grant (`drain_digest_stale`) and the campaign is skipped until re-authorized.
- The plan file digest must match the approval digest; a changed plan pauses the campaign
  (`plan_drift`) with zero POSTs.
- Candidate drift quarantines the file; it is never silently replaced.
- Deadline passed / `max_submissions` reached → the batch refuses/auto-pauses and the daemon
  skips the campaign thereafter (a new plan/approval is required to continue).
- An expiring campaign lease, a blocked queue, mapping churn/drift, writes-disabled, or a
  source discontinuity stop the batch with a persisted pause and zero POSTs; the daemon stays
  alive reporting read-only.
- Pause/resume/revoke are confirmed operator commands and take effect on the next cycle: a paused
  or revoked campaign is skipped mid-drain; revoke returns queued planned/mapped rows to `waiting`
  and refuses future applies.

Campaign rules:

- The plan file digest must match the approval digest.
- Candidate drift quarantines the file; it is never silently replaced.
- Pause/resume/revoke are confirmed operator commands.
- Revoke returns queued planned/mapped rows to `waiting`; rows past `planned`
  are not rewound.

## 9. Continuous operation

The shipped `compose.example.yaml` starts the ordinary `service`, which uses
onboarding consent. To run this runbook's advanced campaign daemon, create
`compose.advanced.yaml` beside it with the following override:

```yaml
services:
  shoko-sonarr-linker:
    # Built in section 2; alternatively use a verified immutable GHCR SHA tag.
    image: shoko-sonarr-linker:local
    command: ["daemon", "/config/config.yaml"]
    environment:
      SHOKO_LINKER_ALLOW_APPLY: "${SHOKO_LINKER_ALLOW_APPLY:-0}"
    volumes:
      - ./config:/config:ro
```

Place the reviewed advanced YAML at `./config/config.yaml`. Also provide any
referenced overrides file in that directory (or remove the optional
`mappings.overrides_file` setting when unused). Paths and permissions must be
readable by the container's `PUID:PGID`. Relative bind sources resolve from the
base Compose file's directory.

The override inherits the existing network, API-key environment variables and
**the same persistent `/data` mount** from the base file. Keep the original
Compose project name/directory and actual state volume or bind mount; do not
start a fresh project or volume. `database.path` must address the same SQLite
database used for the canary, approvals and drain authorization. Retain the
attestation files at the configured `/data` paths, including campaign-scoped
siblings, so the daemon sees their existing history and leases. Stop any other
writer using that state before switching modes.

Campaign approval stores an absolute plan path. Prepare and approve plans using
container-visible paths, such as `/data/plans/plan.json`, and retain those exact
paths for the drain. Run the earlier CLI steps in this same container environment
and state mount. Use absolute container paths for every config, plan and output
file argument: replace the earlier `config.yaml` with `/config/config.yaml` and
`plan.json` with `/data/plans/plan.json`. For example, this read-only campaign
report checks the plan digest (replace the campaign ID):

```powershell
docker compose -f compose.example.yaml -f compose.advanced.yaml run --rm --no-deps shoko-sonarr-linker `
  campaign report /config/config.yaml --campaign-id "replace-with-campaign-id" `
  --plan /data/plans/plan.json --json
```

A plan approved at a host-only path such as `C:\...\plan.json` cannot be drained just by
copying it into the container. Approval is one-shot: re-approving the same plan
cannot relocate it. Continue its manual workflow, or revoke it and prepare a new
campaign at the container-visible path, with its own approval, canary, lease and
drain authorization. Never edit SQLite to relocate it.

After the canary and applicable safety/authorization checks pass, use both files:

```powershell
docker compose -f compose.example.yaml -f compose.advanced.yaml config --quiet
docker compose -f compose.example.yaml -f compose.advanced.yaml up -d
```

The override forwards `SHOKO_LINKER_ALLOW_APPLY` explicitly and defaults it to
`0` (off). Set it to `1` in the Compose process environment only for an authorized
writable daemon; keep `sync.apply_changes: false` until the canary procedure
requires apply. Both the YAML flag and environment gate, plus the applicable
attestation and campaign approval/drain authorization, are required. Normal-service
onboarding consent does not satisfy these advanced gates. Recreate the container
with the same two-file `up -d` command after changing the environment gate;
`docker compose restart` does not update its environment.

With `campaign.daemon_auto_apply: true` (default) the daemon drains the approved backlog campaign
you canaried above — one bounded batch per apply cycle — with **no further manual invocations**;
progress persists across restarts (durable send rows + fingerprints, never a double-send), oldest
approved campaign first, and pause/resume/revoke stop it on the next cycle. Monitor `report`
(campaign progress, `last_cycle_campaign_*`) and `health` while it drains. Set
`campaign.daemon_auto_apply: false` to keep every batch a manual `campaign apply`.

Monitor:

```powershell
uv run shoko-sonarr-linker report config.yaml --json
uv run shoko-sonarr-linker health config.yaml --json
uv run shoko-sonarr-linker attestation status config.yaml --json
uv run shoko-sonarr-linker mappings status config.yaml --json
```

Restart behavior:

- The daemon stores schedule, budget, baseline/cohort, send ledger, campaign
  state, and mapping refresh state in `/data`.
- A restart does not replay completed sends.
- A sent-but-unverified file is verified/held, never resubmitted.
- A persisted `requires_rebaseline` flag blocks apply until the
  source discontinuity is understood and cleared by the operator command.

Shutdown (SIGTERM / SIGINT, e.g. `docker stop`):

- The inter-cycle sleep is an **interruptible** wait on a shutdown event, so an idle daemon
  exits within a second of the signal — it does not wait out the interval (`interval_seconds`
  plus jitter) and then get SIGKILLed.
- A signal that lands **during** a cycle is honoured only after the current cycle/protected
  unit finishes: no cycle is aborted mid-write, and no new cycle starts after the daemon wakes.
- Cycle metadata (health levels, cycle mode, full/light cadence markers, submitted counts) is
  persisted **after every cycle**, so `report`/`health` stay fresh and a restart resumes the
  cadence from the last completed cycle.
- `compose.example.yaml` sets `stop_grace_period: 360s`, covering nominal cycles
  (`sync.max_cycle_seconds` is 300s and gates new submissions). A worst-case inline
  verification-poll sequence inside the in-flight unit can exceed the grace; then Docker
  escalates to SIGKILL — **recoverable by design**: the durable submission state survives,
  the next start's recovery phase classifies every open row (void/hold/poll/audit), and no
  request is ever blindly resubmitted.

Use the read-only report first. Operator close commands are:

```powershell
uv run shoko-sonarr-linker resolve-send config.yaml --file-id <id> --fingerprint <fp> --release --note "operator close" --confirm
uv run shoko-sonarr-linker clear-requires-rebaseline config.yaml --note "source checked" --confirm
uv run shoko-sonarr-linker resume-writes config.yaml --note "resume after review" --confirm
```

`--release` only after the Shoko-side link, state record, and send fingerprint
have been checked.

## 10. Backup

Back up only with no active daemon:

```powershell
Copy-Item -Path "/mnt/user/appdata/shoko-sonarr-linker/data/state.sqlite3*" -Destination <backup-dir> -Recurse
Copy-Item -Path "/mnt/user/appdata/shoko-sonarr-linker/data/mappings" -Destination <backup-dir> -Recurse
```

Include SQLite `-wal`/`-shm` siblings when present. Restore is a file copy back
to a fresh local volume. Never hand-edit the SQLite database.

## 11. Rollback path

Writes are reversible in local state only. Shoko-side links are not unlinked by
this tool.

```powershell
uv run shoko-sonarr-linker pause-writes config.yaml --reason "incident" --note "operator pause" --confirm
uv run shoko-sonarr-linker attestation invalidate config.yaml --note "incident" --confirm
uv run shoko-sonarr-linker campaign revoke config.yaml --campaign-id <campaign-id> --note "campaign stopped" --confirm
uv run shoko-sonarr-linker resume-writes config.yaml --note "operator resume" --confirm
```

If a campaign plan is no longer trustworthy, revoke it. Do not delete the
plan file; it is part of the audit trail.

## 12. Evidence expectation for live deployment

A live deployment is accepted when the operator has:

1. a clean dry-run cycle with understandable unresolved reasons;
2. one verified controlled batch with zero unexpected writes;
3. no touched media files;
4. Shoko queue and async link jobs in a normal state after the canary;
5. clean `report`/`health` after daemon restart;
6. an auditable attestation lease, campaign approval digest where used, and no
   unexplained indeterminate sends or `requires_rebaseline` flags.

Offline tests do not replace those live observations.
