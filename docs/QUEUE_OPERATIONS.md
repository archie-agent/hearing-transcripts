# Queue Operations Runbook

## Scope

Operational commands for hearing queue workers and digest outbox consumers.
All commands run from the project root with the virtualenv active.

## Health Checks

Print queue and dead-letter metrics:

```bash
python run.py --queue-health
```

Fail health checks on SLO-like thresholds:

```bash
python run.py --queue-health --health-max-queue-age 3600 --health-max-dlq 0
```

Expected keys:
- `hearing_jobs`: counts by status
- `outbox_items`: counts by status
- `stage_tasks`: counts by status
- `stale_leases`: expired running leases
- `max_queue_age_seconds`: oldest pending age
- `retry_histogram`: attempt-count distribution
- `dead_letter_count`: total unresolved DLQ volume

## Producer/Worker Modes

Enqueue one durable discovery job:

```bash
python run.py --enqueue-discovery --days 3
```

Drain discovery jobs (claims discovery leases, discovers hearings, enqueues initial stage tasks):

```bash
python run.py --drain-discovery --max-tasks 10 --lease-seconds 900
```

Legacy producer mode (direct discovery + enqueue in one process):

```bash
python run.py --enqueue-only --days 3 --workers 1
```

Drain stage workers (claim leased stage tasks and process one stage per claim):

```bash
python run.py --drain-only --workers 1 --max-tasks 20 --lease-seconds 900
```

Optional worker identity:

```bash
python run.py --drain-only --worker-id worker-a
```

## Outbox Consumer

Consume transcript publication events from outbox:

```bash
python digest.py --consume-outbox --max-events 20
```

Dry-run consumer:

```bash
python digest.py --consume-outbox --dry-run
```

## Replay / Requeue

Requeue a failed hearing job:

```bash
python run.py --requeue-hearing-job <hearing_id>
```

Requeue a dead-letter outbox event:

```bash
python run.py --requeue-outbox-event <event_id>
```

Requeue a dead-letter stage task:

```bash
python run.py --requeue-stage-task <hearing_id>:<stage>[:version]
```

List unresolved dead-letter items:

```bash
python run.py --list-dlq --dlq-limit 100
```

## Failure Injection Checks

1. Start a drain worker, kill it mid-run.
2. Wait past lease timeout.
3. Run another drain worker.
4. Confirm work resumes and queue health shows lease recovery.

## Rollback

Disable queue read path:

```bash
export USE_LEGACY_MONOLITH=1
```

Force scheduler rollback to legacy digest index scan:

```bash
export DIGEST_USE_LEGACY_INDEX=1
```

Return to monolithic processing:

```bash
python run.py --days 3 --workers 1
```
