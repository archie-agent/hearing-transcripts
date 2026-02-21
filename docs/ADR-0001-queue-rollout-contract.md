# ADR-0001: Queue Rollout Contract

## Status

Accepted on 2026-02-21.

## Context

`docs/NORTH_STAR_ARCHITECTURE.md` defines a migration from monolithic execution
to a durable queue + worker + outbox topology. The rollout needs explicit
runtime contracts so we can ship phase-by-phase without behavior regressions.

## Decision

1. Lease contract
- Stage tasks are claimed with a 15-minute lease timeout.
- Workers renew leases every 60 seconds while work is in progress.

2. Retry contract
- Retry budget is 5 attempts per stage task.
- Backoff uses exponential delay with jitter.
- Tasks that exhaust retry budget move to dead-letter state.

3. Idempotency contract
- Stage task uniqueness key: `(hearing_id, stage, publish_version)`.
- Replays must not produce duplicate canonical publish artifacts.

4. Outbox contract
- `transcript_published` events are immutable once inserted.
- Event payload includes: `event_id`, `hearing_id`, `published_at`,
  `transcript_path`, `committee_key`.
- Consumers acknowledge events explicitly.

5. Feature-flag contract
- `QUEUE_WRITE_ENABLED` defaults on during scaffolding and dual-write phases.
- `QUEUE_READ_ENABLED` defaults off until producer/worker cutover.
- `OUTBOX_DIGEST_ENABLED` defaults off until outbox consumer cutover.

## Consequences

- We can dual-write queue state safely before queue read-path cutover.
- Rollback is a flag flip, not a schema rollback.
- Operator behavior remains monolithic until `QUEUE_READ_ENABLED=1`.
