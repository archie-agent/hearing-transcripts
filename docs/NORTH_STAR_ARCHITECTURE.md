# North-Star Architecture

## North-star summary

- Move from a single process-centric daily run to a durable job model with explicit discovery, processing, and delivery queues.
- Preserve current transcript quality ordering (`govinfo > cspan > isvp > youtube`) while making each stage independently retryable and resumable.
- Separate control plane (scheduling/leases), data plane (hearing stage workers), and ops plane (health, replay, recovery).
- Keep SQLite as the first target backend, but design queue/state schemas so Postgres migration is straightforward.
- Treat digest generation as a first-class downstream pipeline with durable handoff from transcript publication.

## Scope and non-goals

In scope:
- `run.py` discovery and hearing processing lifecycle
- `state.py` durability boundaries (`hearings`, `processing_steps`, cost/health tables)
- transcript publication (`transcripts/*`, `index.json`)
- digest pipeline (`digest.py`) and scheduled operations scripts

Non-goals for this north star:
- changing model/provider choices for cleanup, extraction, or scoring
- redesigning committee taxonomy or source selection policy
- replacing filesystem artifacts with object storage in the first migration phases

## Current-state map (2026-02-21)

```text
launchd / scripts/daily-run.sh
          |
          v
      run.py (single process)
          |
          +--> discover_all()
          |    (youtube/website/govinfo/congress_api/cspan)
          |
          +--> process_hearing() per hearing
          |      captions/isvp/cspan/testimony/govinfo
          |
          +--> publish transcripts/*
          +--> update transcripts/index.json
          +--> write runs/<run_id>/run_meta.json
          +--> record summary in state.db
          |
          +--> check_and_alert()

launchd / scripts/digest-run.sh
          |
          v
      digest.py reads transcripts/index.json
      -> extracts/scores/composes -> sends email
      -> records digest_runs in state.db
```

Current strengths:
- Durable per-hearing step markers in SQLite (`processing_steps`) and idempotent skip/retry behavior by step.
- Atomic writes for key artifacts (`meta.json`, `index.json`, run metadata).
- Good source discovery breadth with basic retry/backoff and scraper health tracking.

Current reliability gaps:
- No durable work queue: progress is implicit in one run process and threadpool execution order.
- No lease/claim model for horizontal workers; parallelism is in-process only.
- Digest trigger boundary is filesystem (`index.json`) rather than durable produced-events queue.
- No dead-letter concept for repeatedly failing hearings/stages.
- Ops tooling is mostly logs/scripts; limited replay/requeue primitives.

## Hearing pipeline north star

```text
Discovery Producer(s)
  run.py --enqueue-discovery
          |
          v
+-----------------------------+
| hearing_work_queue.db       |
| - discovery_jobs            |
| - hearing_jobs              |
| - stage_tasks + leases      |
| - dead_letter_items         |
+---------------+-------------+
                |
                v
Stage Worker(s)
 claim -> run one stage -> checkpoint -> ack
  stages: captions/isvp/cspan/testimony/govinfo/publish
                |
                v
Immutable run artifacts
(runs/<run_id>/hearings/<id>/* + stage manifests)
                |
                v
canonical transcripts/*
                |
                v
+-----------------------------+
| delivery_outbox_items       |
| transcript_published events |
+---------------+-------------+
                |
                v
Digest/notify consumers
```

Target properties:
- Durable per-hearing and per-stage queue with lease timeout and bounded retries.
- Stage-level checkpoint/resume contract independent of process lifetime.
- Explicit dead-letter handling with operator-visible replay commands.
- Digest consumes transcript-published events from outbox, not directory scans alone.
- Cron topology split by role (`producer`, `worker`, `outbox`, `health/maintenance`).

### Plane responsibilities

Control plane:
- Owns discovery scheduling, queue claims/leases, retry accounting, and run lifecycle.
- Supports explicit producer and consumer execution roles.

Data plane:
- Executes stage work (`captions`, `isvp`, `cspan`, `testimony`, `govinfo`, `publish`).
- Persists stage checkpoints and artifacts before acknowledging completion.
- Emits durable delivery events for digest and notifications.

Ops plane:
- Tracks queue depth, lease age, per-stage failures, and DLQ volume.
- Provides requeue/replay and stuck-task remediation commands.

### Invariants

- Durability: stage completion and delivery intent are persisted before side effects are considered done.
- Idempotency: replaying a stage does not corrupt canonical transcript artifacts.
- Resumability: worker interruption yields reclaimable leased tasks.
- Observability: every run/stage has machine-readable status and cost counters.

## Current state vs north star

### Ingest/transcript status

- Partially aligned: stage functions already exist with mostly idempotent behavior.
- Missing durable queue semantics and explicit claim/lease records.
- Publication is durable enough for single-process execution, but not coordinated for multi-worker cutover.

### Digest status

- Functional but loosely coupled: scans `index.json` by date window.
- No durable handoff from transcript publication to digest processing.
- No per-item digest retry/dead-letter visibility.

## Gap matrix

| Gap | Impact | Risk if deferred | Priority | Class |
|---|---|---|---|---|
| No durable stage queue with leases | Crash/restart can strand in-flight work and forces coarse reruns | Throughput and reliability ceiling as volume grows | High | Must-have reliability |
| No stage DLQ/replay workflow | Repeated failures stay noisy but not operationally contained | Growing manual triage burden | High | Must-have reliability |
| Digest fed by `index.json` scan only | Weak exactly-once semantics for downstream delivery | Missed or duplicated digest candidates | High | Must-have reliability |
| In-process parallelism only (`ThreadPoolExecutor`) | Cannot scale workers independently or isolate failure domains | Single host/process remains SPOF | Medium | Must-have reliability |
| Limited run-level SLO instrumentation | Harder to detect regressions proactively | Longer MTTR during source/provider instability | Medium | Must-have reliability |
| SQLite-only deployment path | Fine now, but multi-node contention later | Future migration gets riskier if deferred too long | Low | Later scaling |

## Phased migration plan (with rollback points)

1. Queue scaffolding + run ledger
   - Add queue tables (`hearing_jobs`, `stage_tasks`, leases, retry counters) in SQLite alongside current tables.
   - Keep existing `run.py` flow as source of truth.
   - Rollback: disable queue writes via flag; continue current pipeline unchanged.
2. Dual-write stages
   - Current `process_hearing()` continues execution, but writes stage state/checkpoints to queue schema.
   - Add replay-safe `stage_task` records and terminal-failure reasons.
   - Rollback: ignore queue tables and trust existing `processing_steps`.
3. Producer/worker cutover
   - Split CLI modes: `--enqueue-only` and `--drain-only`; workers claim stage tasks with leases.
   - Keep single-worker fallback mode available.
   - Rollback: return to monolithic mode using existing codepath.
4. Digest handoff via outbox
   - Emit `transcript_published` outbox events on publish.
   - Add digest consumer to claim events and mark completion.
   - Rollback: switch digest back to `index.json` scan mode.
5. Ops hardening and SLOs
   - Add queue admin/health commands: stuck leases, DLQ counts, requeue tools.
   - Add scheduled health checks and on-call playbook updates.
   - Rollback: disable health gates while keeping data-plane behavior unchanged.

## Acceptance criteria by phase

Phase 1:
- `python3 run.py --days 2 --workers 1` completes with unchanged transcript outputs.
- SQLite has queue scaffolding tables with non-empty audit rows for run metadata.
- Failure injection: kill process mid-run; restart still completes without duplicate published transcript directories.

Phase 2:
- For each completed hearing, `processing_steps` and `stage_tasks` show consistent final stage status.
- Re-running same day does not create duplicate terminal stage records.
- Failure injection: force one stage exception and verify retry increments + terminal capture.

Phase 3:
- `python3 run.py --enqueue-only` + `python3 run.py --drain-only --workers 1` yields same artifacts as monolith.
- Lease timeout reclaims abandoned tasks after worker termination.
- Failure injection: terminate worker during `cspan` stage; second worker resumes from claimed stage.

Phase 4:
- Publishing a transcript creates exactly one outbox event and one digest-consumer completion record.
- Digest no longer depends on full index scan for primary workflow.
- Failure injection: break email delivery and confirm retry/backoff then DLQ visibility.

Phase 5:
- Health command reports queue depth, lease age, retry histogram, DLQ count.
- Alerting thresholds tied to measurable SLOs (e.g., max queue age, failure rate).
- Runbooks document replay/requeue commands and expected outcomes.

## Progress snapshot (2026-02-21)

- Phase 1 complete: queue scaffolding tables and queue run audit ledger are in `state.py`.
- Phase 2 complete: stage dual-write is active for `captions`, `isvp`, `cspan`, `testimony`, `govinfo`, and `publish`.
- Phase 3 complete: `run.py` supports `--enqueue-only` and `--drain-only` with leased hearing job claims.
- Phase 4 complete: transcript publish emits durable outbox events; `digest.py` supports `--consume-outbox`.
- Phase 5 complete: `run.py --queue-health`, `--requeue-hearing-job`, and `--requeue-outbox-event` are available.
- Phase 5+ hardening: stage-task dead-letter + replay (`--requeue-stage-task`) and DLQ listing (`--list-dlq`) are now available.
- Phase 6 in progress: scheduler scripts support producer/worker and outbox modes behind feature flags.

## Next actions and docs to update

Immediate implementation order:
1. Define queue schema and CLI mode flags (`enqueue-only`, `drain-only`) in `run.py`.
2. Add queue/lease admin commands in `state.py`-backed tooling.
3. Introduce transcript publish outbox event + digest consumer contract.

Owner-visible docs/runbooks to update:
- `scripts/daily-run.sh`
- `scripts/digest-run.sh`
- launchd plist operational notes currently referenced in `schedule.sh`
- this file and a new queue operations runbook under `docs/`
