# Lab 06 validation - 2026-10-12

Isolated live Snowflake rehearsal; student ADV_ORDERS was not changed.

- Notebook transformation and rollback cells ran in Snowflake Python 3.12 as a temporary rehearsal stored-procedure wrapper, using the same cell source. Only session acquisition/context were adapted. All assertions passed.
- Unpinned Snowpark dependency resolved at deployment; see execution evidence below for the actual version.
- Initial input/inbox 10; current 3; active value 165; pending 0. Trial scratch writes rolled back.
- Procedure setup/handler rerun passed. Synthetic replay produced raw/inbox 11, scratch events 11, current 3, active value 165 and pending 0. Repeating replay did not add another row.
- Snowpark MERGE restored one deliberately missing order and one deliberately older order state.
- Conflicting e4 payload raised an exception: inbox remained 11, raw 12, pending stream 1, current 3 and active value 165. Transaction rollback preserved stream availability.
- Rehearsal warehouse suspended afterwards.

Not tested: hosted notebook UI import/runtime, optional headless notebook task, and task-graph execution in this rehearsal. These require an account rehearsal; do not present them as passed. Snowpipe was bypassed by SQL-seeded Bronze input in this test.

```json
{
  "status": "PASS",
  "database": "ADE_LAB6_REVIEW_20261012_210812",
  "notebook_code": {
    "NOTEBOOK_CELL_REHEARSAL": "Notebook development cells passed; Snowpark 1.55.1"
  },
  "initial": {
    "RAW_ROWS": 10,
    "INBOX_ROWS": 10,
    "PENDING_ROWS": 0,
    "CURRENT_ORDERS": 3,
    "ACTIVE_AMOUNT": "165.00",
    "SNOWPARK_ROWS": 10
  },
  "after_replay": {
    "RAW_ROWS": 11,
    "INBOX_ROWS": 11,
    "PENDING_ROWS": 0,
    "CURRENT_ORDERS": 3,
    "ACTIVE_AMOUNT": "165.00",
    "SNOWPARK_ROWS": 11
  },
  "conflict_rollback": {
    "RAW_ROWS": 12,
    "INBOX_ROWS": 11,
    "PENDING_ROWS": 1,
    "CURRENT_ORDERS": 3,
    "ACTIVE_AMOUNT": "165.00",
    "SNOWPARK_ROWS": 11
  },
  "limitations": [
    "Notebook UI import and execution not tested; notebook cells ran in an isolated Python stored procedure.",
    "Optional headless notebook task not executed.",
    "Existing task graph modification is retained but not executed in this rehearsal."
  ]
}
```
