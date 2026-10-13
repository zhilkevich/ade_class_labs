/*----------------Snowflake Advanced 3-day class Lab:-------------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
Lab 06 - Notebook development, procedure deployment and orchestration
1) Prepare targets, then develop Snowpark transformations cell by cell in a notebook
2) Compare Snowpark output with the established SQL baseline
3) Deploy the same source and invoke it through the existing task graph
----------------------------------------------------------------------------------*/

-- Diagram: Pipeline-progression.md, Lab 06 - Snowpark implementation and deployment.

USE ROLE ADV_LAB;
USE WAREHOUSE ADV_WH;
USE DATABASE ADV_ORDERS;
USE SCHEMA OPS;

-- Use a common session time zone for classroom checks and LTZ history display.
-- This is session setup, not a conversion of stored event timestamps.
-- Fixture event_time strings already contain Z (UTC), so their TIMESTAMP_TZ
-- interpretation does not depend on this setting. Timestamps without an offset
-- would use the session zone when converted to TIMESTAMP_TZ.

ALTER SESSION SET TIMEZONE = 'UTC';

-- Label statements in this session so we can retrieve them at the end.

ALTER SESSION SET QUERY_TAG = 'ADV_2026_DAY1';

-- ──────────────────────────────────────────────────────────────────────────────
-- PART A: SETUP - prepare destinations before opening the development notebook
-- ──────────────────────────────────────────────────────────────────────────────

-- The first five files establish 3 current orders and active value USD 165.
-- SQL is a useful baseline; Snowpark implements the same contract.
-- Python runs inside Snowflake, with the same setup for every student.

CREATE TABLE IF NOT EXISTS SILVER.REGION_REFERENCE
(
    REGION_CODE  STRING,
    REGION_LABEL STRING
);

CREATE TABLE IF NOT EXISTS SILVER.SNOWPARK_EVENTS
(
    EVENT_ID          STRING,
    ORDER_ID          STRING,
    VERSION           NUMBER(3, 1),
    EVENT_TIME        TIMESTAMP_TZ,
    STATUS            STRING,
    AMOUNT            NUMBER(12, 2),
    REGION            STRING,
    REGION_LABEL      STRING,
    ACTIVE_AMOUNT_USD NUMBER(12, 2),
    REJECT_REASON     STRING,
    SOURCE_FILE       STRING,
    SOURCE_ROW        NUMBER
);

CREATE TABLE IF NOT EXISTS SILVER.SNOWPARK_CANDIDATES LIKE SILVER.ORDERS_CURRENT;

-- The three table definitions above establish the destinations before processing.
-- REGION_REFERENCE enriches region labels; SNOWPARK_EVENTS holds classified
-- deliveries; SNOWPARK_CANDIDATES holds one selected state per order.
-- The handler joins, populates these tables and merges into existing ORDERS_CURRENT.

MERGE INTO SILVER.REGION_REFERENCE t
USING (SELECT column1 AS region_code, column2 AS region_label
       FROM VALUES ('US', 'United States'), ('EU', 'Europe')) s
ON t.region_code = s.region_code
WHEN MATCHED THEN UPDATE SET region_label = s.region_label
WHEN NOT MATCHED THEN INSERT (region_code, region_label)
VALUES (s.region_code, s.region_label);

-- Query package versions available in this account; do not assume a fixed release.
-- Package version is omitted in deployment: resolve latest available for Python 3.12.

SELECT package_name, version
FROM SNOWFLAKE.INFORMATION_SCHEMA.PACKAGES
WHERE language = 'python'
  AND package_name = 'snowflake-snowpark-python'
ORDER BY TRY_TO_NUMBER(SPLIT_PART(version, '.', 1)) DESC,
         TRY_TO_NUMBER(SPLIT_PART(version, '.', 2)) DESC,
         TRY_TO_NUMBER(SPLIT_PART(version, '.', 3)) DESC;

-- STOP HERE. Open 06-Snowpark-Development.ipynb and run it cell by cell.
-- It develops the same functions used below. Return only after all checkpoints pass.

-- ============================================================================
-- PART B: DEPLOY - package the verified functions as a stored procedure.
-- ============================================================================
-- Snowpark-OrderTransform.py and the notebook contain this same reusable source.
-- Unpinned packages resolve at CREATE time; calls reuse that resolved environment.
-- Source: https://docs.snowflake.com/en/sql-reference/sql/create-procedure

CREATE OR REPLACE PROCEDURE SILVER.APPLY_EVENTS_SNOWPARK()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
"""Reusable Snowpark transformations for the Day 1 order-event pipeline.
Copyright © 2026 Innovation In Software Corporation. All rights reserved.
"""
from snowflake.snowpark.functions import (
    col, sql_expr, row_number, when_matched, when_not_matched
)
from snowflake.snowpark.window import Window

ORDER_COLUMNS = ["ORDER_ID", "EVENT_ID", "VERSION", "EVENT_TIME", "STATUS", "AMOUNT", "REGION"]
EVENT_COLUMNS = ["EVENT_ID", "ORDER_ID", "VERSION", "EVENT_TIME", "STATUS", "AMOUNT",
                 "REGION", "REGION_LABEL", "ACTIVE_AMOUNT_USD", "REJECT_REASON",
                 "SOURCE_FILE", "SOURCE_ROW"]

def type_events(df):
    # select_expr builds a DataFrame plan; it does not collect the event rows.
    # Classroom versions use NUMBER(3,1) to keep the displayed values compact.

    return df.select_expr(
        "NULLIF(TRIM(payload:event_id::STRING), '') AS EVENT_ID",
        "NULLIF(TRIM(payload:order_id::STRING), '') AS ORDER_ID",
        "TRY_TO_DECIMAL(payload:version::STRING, 3, 1) AS VERSION",
        "TRY_TO_TIMESTAMP_TZ(payload:event_time::STRING) AS EVENT_TIME",
        "payload:status::STRING AS STATUS",
        "TRY_TO_DECIMAL(payload:amount::STRING, 12, 2) AS AMOUNT",
        "payload:region::STRING AS REGION",
        "SOURCE_FILE", "SOURCE_ROW")

def classify_events(df):
    # Reuse the business rules developed in SQL; express a new DataFrame column.

    return df.with_column("REJECT_REASON",
                          sql_expr("""CASE
                                        WHEN event_id IS NULL OR order_id IS NULL THEN 'MISSING_KEY'
                                        WHEN version IS NULL OR version < 1 OR version <> TRUNC(version) THEN 'BAD_VERSION'
                                        WHEN event_time IS NULL THEN 'BAD_TIME'
                                        WHEN status IS NULL OR status NOT IN ('PLACED', 'PAID', 'CANCELLED') THEN 'BAD_STATUS'
                                        WHEN amount IS NULL OR amount < 0 THEN 'BAD_AMOUNT'
                                        WHEN region IS NULL OR region NOT IN ('US', 'EU') THEN 'BAD_REGION'
                                        ELSE NULL
                                    END"""))

def enrich_events(df_classified, df_reference):
    enriched = df_classified.join(
        df_reference, df_classified["REGION"] == df_reference["REGION_CODE"], "left")
    return enriched.with_column("ACTIVE_AMOUNT_USD", sql_expr("IFF(status = 'CANCELLED', 0, amount)"))

def select_candidates(df):
    good = df.filter(col("REJECT_REASON").is_null())

    # Pick a representative delivery of each immutable event; keep inbox history.
    # File/row identifies delivery lineage, not business-state precedence.

    unique = good.with_column("RN", row_number().over(Window.partition_by("EVENT_ID").order_by(col("SOURCE_FILE"), col("SOURCE_ROW"))))
    unique = unique.filter(col("RN") == 1).drop("RN")

    # Now choose the latest business state using the same SQL precedence rule.

    latest = unique.with_column("RN", row_number().over(Window.partition_by("ORDER_ID").order_by(col("VERSION").desc(), col("EVENT_TIME").desc(), col("EVENT_ID").desc())))
    return latest.filter(col("RN") == 1).select(*ORDER_COLUMNS)

def merge_current(sf_session, df_candidates):
    target = sf_session.table("SILVER.ORDERS_CURRENT")
    newer = (
        (df_candidates["VERSION"] > target["VERSION"])
        | ((df_candidates["VERSION"] == target["VERSION"])
           & (df_candidates["EVENT_TIME"] > target["EVENT_TIME"]))
        | ((df_candidates["VERSION"] == target["VERSION"])
           & (df_candidates["EVENT_TIME"] == target["EVENT_TIME"])
           & (df_candidates["EVENT_ID"] > target["EVENT_ID"]))
    )
    updates = {name: df_candidates[name] for name in ORDER_COLUMNS if name != "ORDER_ID"}
    inserts = {name: df_candidates[name] for name in ORDER_COLUMNS}

    # Table.merge is an action: Snowflake executes MERGE and returns row counts.

    return target.merge(df_candidates, target["ORDER_ID"] == df_candidates["ORDER_ID"], [
        when_matched(newer).update(updates),
        when_not_matched().insert(inserts)
    ])

def main(sf_session):
    # Only SQL transaction control and stream capture remain sf_session.sql calls.
    # All destinations already exist; no DDL belongs inside this transaction.

    sf_session.sql("BEGIN TRANSACTION").collect()
    try:
        # Consume this stream into the inbox ONCE per transaction.

        sf_session.sql("""INSERT INTO SILVER.EVENT_INBOX
            SELECT payload, source_file, source_row, scan_time
            FROM BRONZE.EVENTS_STREAM""").collect()
        if sf_session.table("SILVER.EVENT_CONFLICTS").count():
            raise ValueError("Conflicting payloads for immutable event_id")

        typed = type_events(sf_session.table("SILVER.EVENT_INBOX"))
        classified = classify_events(typed)
        enriched = enrich_events(classified, sf_session.table("SILVER.REGION_REFERENCE"))
        candidates = select_candidates(enriched)

        sf_session.table("SILVER.SNOWPARK_EVENTS").delete()
        enriched.select(*EVENT_COLUMNS).write.insert_into("SILVER.SNOWPARK_EVENTS")
        sf_session.table("SILVER.SNOWPARK_CANDIDATES").delete()
        candidates.write.insert_into("SILVER.SNOWPARK_CANDIDATES")
        result = merge_current(sf_session, sf_session.table("SILVER.SNOWPARK_CANDIDATES"))
        sf_session.sql("COMMIT").collect()
        return f"Applied with Snowpark: {result}"
    except Exception:
        sf_session.sql("ROLLBACK").collect()
        raise
$$;

DESC PROCEDURE SILVER.APPLY_EVENTS_SNOWPARK();

-- Inspect the resolved package dependency in the procedure description.

CALL SILVER.APPLY_EVENTS_SNOWPARK();

SELECT *
FROM SILVER.SNOWPARK_CANDIDATES
ORDER BY ORDER_ID;

SELECT EVENT_ID, REJECT_REASON
FROM SILVER.SNOWPARK_EVENTS
WHERE REJECT_REASON IS NOT NULL;

-- CHECKPOINT: candidates o1/e4/120, o2/e5/50 cancelled, o3/e8/45; one BAD_AMOUNT.

SELECT ORDER_ID, EVENT_ID, VERSION, EVENT_TIME, STATUS, AMOUNT, REGION
FROM SILVER.SNOWPARK_CANDIDATES
MINUS
SELECT ORDER_ID, EVENT_ID, VERSION, EVENT_TIME, STATUS, AMOUNT, REGION
FROM SILVER.ORDERS_CURRENT;

SELECT ORDER_ID, EVENT_ID, VERSION, EVENT_TIME, STATUS, AMOUNT, REGION
FROM SILVER.ORDERS_CURRENT
MINUS
SELECT ORDER_ID, EVENT_ID, VERSION, EVENT_TIME, STATUS, AMOUNT, REGION
FROM SILVER.SNOWPARK_CANDIDATES;

-- CHECKPOINT: both differences are empty. Equality does not prove all contracts;
-- inspect rejected rows and the source replay next.

-- ──────────────────────────────────────────────────────────────────────────────
-- PART C: ORCHESTRATE - switch the existing task graph to the procedure
-- ──────────────────────────────────────────────────────────────────────────────

-- If 99-Stop was run earlier, this also restarts the child. Root stays suspended.

ALTER TASK OPS.APPLY_TASK SUSPEND;

ALTER TASK OPS.AUDIT_TASK SUSPEND;

-- Wait for any in-flight run to finish before modifying the graph.

SHOW TASKS IN SCHEMA OPS;

ALTER TASK OPS.APPLY_TASK MODIFY AS CALL ADV_ORDERS.SILVER.APPLY_EVENTS_SNOWPARK();

ALTER TASK OPS.AUDIT_TASK RESUME;

-- Add one duplicate delivery in a distinct synthetic file lineage.
-- The NOT EXISTS guard makes this replay step rerunnable without adding more rows.
-- It intentionally bypasses file ingestion to isolate business replay behavior.

INSERT INTO BRONZE.EVENTS_RAW
SELECT PAYLOAD, 'snowpark/replay-e8', 1, CURRENT_TIMESTAMP()
FROM BRONZE.EVENTS_RAW
WHERE PAYLOAD:event_id::STRING = 'e8'
  AND NOT EXISTS
      (SELECT 1 FROM BRONZE.EVENTS_RAW WHERE SOURCE_FILE = 'snowpark/replay-e8')
QUALIFY ROW_NUMBER() OVER (ORDER BY SOURCE_FILE, SOURCE_ROW) = 1;

EXECUTE TASK OPS.APPLY_TASK;

SELECT NAME, STATE, QUERY_ID, ERROR_MESSAGE, COMPLETED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(RESULT_LIMIT => 100))
WHERE DATABASE_NAME = 'ADV_ORDERS'
  AND SCHEMA_NAME = 'OPS'
ORDER BY SCHEDULED_TIME DESC;

-- Do not proceed until both latest task executions are SUCCEEDED.

SELECT COUNT(*) AS PENDING_ROWS
FROM BRONZE.EVENTS_STREAM;

SELECT *
FROM GOLD.ORDER_METRICS;

-- CHECKPOINT after success: stream 0; raw/inbox 11; current 3; active value 165.
-- Preserve cancellation and late-version protections. Repeating the handler
-- changes neither current order state nor event counts when stream is empty.

CALL SILVER.APPLY_EVENTS_SNOWPARK();


-- STEP: retrieve this student's tagged statements from the past hour.
-- Includes worksheet and notebook sessions for the current user; no account-wide
-- history privilege is needed for the student's own statements.
-- The 1000-result limit applies BEFORE the tag filter: narrow the time range
-- if recent activity would push the desired statements outside that window.
-- QUERY_TAG is session-scoped. A task uses its own session and does not inherit
-- this worksheet tag; use TASK_HISTORY above to verify the task graph.
-- Source: https://docs.snowflake.com/en/sql-reference/functions/query_history

SELECT query_id, start_time, execution_status, total_elapsed_time, query_text
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(
    END_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 1000,
    INCLUDE_CLIENT_GENERATED_STATEMENT => TRUE))
WHERE query_tag = 'ADV_2026_DAY1'
ORDER BY start_time DESC;

-- Optional alternative: see Optional-NotebookPipeline.sql. Do not run two consumers
-- of the same stream concurrently. Hosted notebook execution requires prevalidation.
