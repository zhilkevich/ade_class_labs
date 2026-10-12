# Day 1 — Build and operate the order-event pipeline

## Working agreement

For each numbered step: instructor explains the purpose, executes it, pauses; everyone executes the same statements in their own identical environment; compare the checkpoint; discuss the result. SQL comments are the primary lab narrative. Do not Run All across asynchronous checkpoints. There is no instructor-only data generator.

## Run order and pacing

Run SetUp/00, then 01 → 02 → 03 → 04 → 05 → 06. The Snowpark/deployment exercise is part of Day 1. Run 05 before 06 because acceptance expects ten deliveries; 06 adds a distinct eleventh replay delivery. Use Course-runbook.md for the revised proposed day schedule, including Snowpark and deployment time. Teaching durations have not yet been timed with a class.

## Lab 03 — develop the queries before making them reusable

Start by inspecting Bronze events and creating both known Silver tables: EVENT_INBOX for delivery history and ORDERS_CURRENT for the current-state contract consumed by Gold. With the destination established, develop the processing that populates it. The sequence then populates the inbox inside trial transactions so each conversion, classification and conflict SELECT has real data to inspect. Roll back each trial before creating its view: CREATE VIEW implicitly commits an open transaction. Then inspect the latest-state candidates and run MERGE manually inside a trial transaction. Compare the visible result, roll back and verify that the starting inbox/target counts and stream availability are restored. Finally create APPLY_EVENTS from the verified steps, call it to commit processing, and call it again to check repeat execution. Use the same worksheet/session throughout each trial. A cleanup DELETE after committing would not restore a consumed stream offset.

## Project and expected results

Bronze holds immutable raw event payloads plus file lineage. Silver retains consumed input, classifies quality failures and maintains one latest state per order. Gold groups active order value by region. Cancellation stays in Silver as a tombstone and is excluded from Gold. This is current order value, not recognized revenue.

| Checkpoint | Raw | Inbox | Current orders | Rejects | Active orders / USD |
|---|---:|---:|---:|---:|---|
| After bulk batch1 | 4 | 0 | 0 | 0 | Not built |
| After Snowpipe batch2 | 8 | 0 | 0 | 0 | Not built |
| After APPLY_EVENTS | 8 | 8 | 3 | 1 | 2 / 150 |
| After task update | 10 | 10 | 3 | 1 | 2 / 165 |
| After Snowpark replay | 11 | 11 | 3 | 1 | 2 / 165 |

The duplicate e2 is identical. e3 has invalid amount text and remains a reject even after e6 supplies a later valid state for o3. The late e7 must not overwrite o1 version 2. Duplicate e8 must not double order value. An immutable event-ID payload conflict raises an exception and rolls back stream consumption; production conflict repair is outside this Day 1 implementation.

## If a checkpoint fails

| Symptom | Inspect and act |
|---|---|
| Stage has no files | Confirm role/schema and unload results; inspect LIST before COPY |
| COPY returns no new data | Check exact prefix and previous loads; never reflexively add FORCE |
| Pipe has fewer than eight raw rows | Inspect SYSTEM$PIPE_STATUS and COPY_HISTORY, file age and errors; wait/requery up to five minutes, then stop and record |
| Wrong Silver count | Inspect classified rows, rejection reasons, event IDs and inbox; do not truncate |
| Task privilege error | Verify owner ADV_LAB, EXECUTE TASK grant and warehouse USAGE |
| Child task missing | Confirm AUDIT_TASK resumed before executing root |
| Task submitted but output unchanged | EXECUTE TASK is asynchronous; wait for both task histories, then inspect stream and Gold |
| Transaction failed | Check raw inputs and conflict view; a failed transaction must not advance stream offset |
| Cost continues after warehouse suspension | Inspect scheduled tasks and pipe; serverless services are separate |

### Explicit bulk fallback (only if the Snowpipe branch fails)

Record the Snowpipe result as FAILED/BLOCKED. Pause the pipe, wait for in-flight ingestion to settle, inspect history and raw counts. If the instructor elects to continue, abandon the failed pipe branch for the class, load only missing files with COPY using the same column mapping from lab 02 and stage batch2, without FORCE. Verify exact raw rows and event IDs before Silver. In lab 04 use the same bulk path for update.json.gz instead of REFRESH. Record PIPE versus BULK_FALLBACK in the rehearsal log; completing the transformation chain does not pass the Snowpipe objective. Mixed ingestion can duplicate data; if file-level completion is uncertain use a fresh lab environment rather than guessing.

### Optional AWS auto-ingest experiment

This is not part of the core run. In a separate rehearsal-only stage/table, create AUTO_INGEST=TRUE pipe before adding a new file. Use the same SQL unload technique. Verify arrival without REFRESH or COPY, record latency and source query IDs, then pause the pipe. Only include this branch in the class after all student accounts pass it. Do not describe SQL REFRESH as continuous auto-ingest or schedule repeated REFRESH as a production design.

## Discussion answers

1. Replaying the same file tests load metadata. Replaying the same event in a different file tests business idempotency.
2. Event time and arrival time differ. A versioned latest-state rule protects against late arrivals.
3. A SELECT from a stream does not consume it. Committed consuming DML advances its offset; rollback does not.
4. A successful task can produce incorrect business results; validate both execution history and state.
5. Deleting cancellation tombstones can let older events recreate cancelled orders.
6. The inbox history scan is deliberate for a tiny teaching dataset; Day 2 compares maintained Gold queries and freshness; affected-order inbox processing remains a production extension.

Copyright © 2026 Innovation In Software Corporation. All rights reserved.
