/*----------------Snowflake Advanced 3-day class Lab:-------------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
C3/C4 completion — Snowpark with the existing order contract
1) Build a lazy transformation plan with tolerant casts, validation and a reference join
2) Compare Snowpark output with the established SQL baseline
3) Deploy the same source and invoke it through the existing task graph
----------------------------------------------------------------------------------*/

USE ROLE ADV_LAB;
USE WAREHOUSE ADV_WH;
USE DATABASE ADV_ORDERS;
USE SCHEMA OPS;

ALTER SESSION SET TIMEZONE = 'UTC';
ALTER SESSION SET QUERY_TAG = 'ADV_2026_DAY1';

-- ──────────────────────────────────────────────────────────────────────────────
-- STEP 1 │ Prepare stable transformation tables before any transaction
-- ──────────────────────────────────────────────────────────────────────────────

-- The first five files establish 3 current orders and active value USD 165.
-- SQL is a useful baseline; Snowpark implements the same contract.
-- Python runs inside Snowflake, with the same setup for every student.
CREATE TABLE SILVER.REGION_REFERENCE
(
    REGION_CODE  STRING,
    REGION_LABEL STRING
);

INSERT INTO SILVER.REGION_REFERENCE VALUES
    ('US', 'United States'),
    ('EU', 'Europe');

CREATE TABLE SILVER.SNOWPARK_EVENTS
(
    EVENT_ID          STRING,
    ORDER_ID          STRING,
    VERSION           NUMBER(38, 9),
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

CREATE TABLE SILVER.SNOWPARK_CANDIDATES LIKE SILVER.ORDERS_CURRENT;

-- Confirm the pinned package is available in every participant account.
-- Python3.12 / Snowpark1.55.1 was exercised on 2026-10-12; recheck before class.
SELECT PACKAGE_NAME, VERSION
FROM SNOWFLAKE.INFORMATION_SCHEMA.PACKAGES
WHERE LANGUAGE = 'python'
  AND PACKAGE_NAME = 'snowflake-snowpark-python'
  AND VERSION = '1.55.1';

-- ──────────────────────────────────────────────────────────────────────────────
-- STEP 2 │ Deploy the readable handler using SQL
-- ──────────────────────────────────────────────────────────────────────────────

-- Snowpark-OrderTransform.py is the identical handler, suitable for a Python
-- worksheet with main as handler and String as return type. No local Python needed.
CREATE PROCEDURE SILVER.APPLY_EVENTS_SNOWPARK()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python==1.55.1')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
"""Snowpark current-order transformation; transaction and business state stay in Snowflake.
Copyright © 2026 Innovation In Software Corporation. All rights reserved.
"""
from snowflake.snowpark.functions import col, lit, sql_expr, row_number
from snowflake.snowpark.window import Window

def main(session):
    # STEP 1: consume the stream and couple all output DML in one transaction.
    session.sql("BEGIN TRANSACTION").collect()
    try:
        session.sql("""INSERT INTO SILVER.EVENT_INBOX
            SELECT payload, source_file, source_row, scan_time
            FROM BRONZE.EVENTS_STREAM""").collect()

        # STEP 2: tolerant casts preserve bad business data for diagnosis.
        raw = session.table("SILVER.EVENT_INBOX")
        typed = raw.select_expr(
            "NULLIF(TRIM(payload:event_id::STRING), '') AS EVENT_ID",
            "NULLIF(TRIM(payload:order_id::STRING), '') AS ORDER_ID",
            "TRY_TO_DECIMAL(payload:version::STRING, 38, 9) AS VERSION",
            "TRY_TO_TIMESTAMP_TZ(payload:event_time::STRING) AS EVENT_TIME",
            "payload:status::STRING AS STATUS",
            "TRY_TO_DECIMAL(payload:amount::STRING, 12, 2) AS AMOUNT",
            "payload:region::STRING AS REGION",
            "SOURCE_FILE", "SOURCE_ROW")
        typed = typed.with_column("REJECT_REASON", sql_expr("""CASE
            WHEN event_id IS NULL OR order_id IS NULL THEN 'MISSING_KEY'
            WHEN version IS NULL OR version < 1 OR version <> TRUNC(version) THEN 'BAD_VERSION'
            WHEN event_time IS NULL THEN 'BAD_TIME'
            WHEN status IS NULL OR status NOT IN ('PLACED', 'PAID', 'CANCELLED') THEN 'BAD_STATUS'
            WHEN amount IS NULL OR amount < 0 THEN 'BAD_AMOUNT'
            WHEN region IS NULL OR region NOT IN ('US', 'EU') THEN 'BAD_REGION'
            ELSE NULL END"""))

        # STEP 3: a reference join and derived active amount enrich the same events.
        ref = session.table("SILVER.REGION_REFERENCE")
        enriched = typed.join(ref, typed["REGION"] == ref["REGION_CODE"], "left")
        enriched = enriched.with_column("ACTIVE_AMOUNT_USD",
            sql_expr("IFF(status = 'CANCELLED', 0, amount)"))
        session.sql("DELETE FROM SILVER.SNOWPARK_EVENTS").collect()
        enriched.select("EVENT_ID", "ORDER_ID", "VERSION", "EVENT_TIME", "STATUS",
            "AMOUNT", "REGION", "REGION_LABEL", "ACTIVE_AMOUNT_USD", "REJECT_REASON",
            "SOURCE_FILE", "SOURCE_ROW").write.insert_into("SILVER.SNOWPARK_EVENTS")

        # STEP 4: check immutable-ID conflicts, then rank unique valid events.
        # Only scalar checks return to Python; event rows are not collected.
        if session.table("SILVER.EVENT_CONFLICTS").count():
            raise ValueError("Conflicting payloads for immutable event_id")
        good = enriched.filter(col("REJECT_REASON").is_null())
        unique = good.with_column("RN", row_number().over(
            Window.partition_by("EVENT_ID").order_by(col("SOURCE_FILE"), col("SOURCE_ROW"))))
        unique = unique.filter(col("RN") == 1).drop("RN")
        latest = unique.with_column("RN", row_number().over(
            Window.partition_by("ORDER_ID").order_by(
                col("VERSION").desc(), col("EVENT_TIME").desc(), col("EVENT_ID").desc())))
        latest = latest.filter(col("RN") == 1).drop("RN")
        session.sql("DELETE FROM SILVER.SNOWPARK_CANDIDATES").collect()
        latest.select("ORDER_ID", "EVENT_ID", "VERSION", "EVENT_TIME", "STATUS", "AMOUNT",
            "REGION").write.insert_into("SILVER.SNOWPARK_CANDIDATES")

        # STEP 5: merge the same state using the same version/time/ID precedence.
        session.sql("""MERGE INTO SILVER.ORDERS_CURRENT t
            USING SILVER.SNOWPARK_CANDIDATES s ON t.order_id = s.order_id
            WHEN MATCHED AND (
                s.version > t.version
                OR (s.version = t.version AND s.event_time > t.event_time)
                OR (s.version = t.version AND s.event_time = t.event_time AND s.event_id > t.event_id))
            THEN UPDATE SET event_id = s.event_id, version = s.version,
                event_time = s.event_time, status = s.status, amount = s.amount, region = s.region
            WHEN NOT MATCHED THEN INSERT
                (order_id, event_id, version, event_time, status, amount, region)
            VALUES (s.order_id, s.event_id, s.version, s.event_time, s.status, s.amount, s.region)""").collect()
        session.sql("COMMIT").collect()
        return "Applied with Snowpark"
    except Exception:
        session.sql("ROLLBACK").collect()
        raise
$$;

CALL SILVER.APPLY_EVENTS_SNOWPARK();

SELECT * FROM SILVER.SNOWPARK_CANDIDATES ORDER BY ORDER_ID;
SELECT EVENT_ID, REJECT_REASON FROM SILVER.SNOWPARK_EVENTS
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
-- STEP 3 │ Deploy the transformation entry point without changing the task graph
-- ──────────────────────────────────────────────────────────────────────────────

-- If 99-Stop was run earlier, this also restarts the child. Root stays suspended.
ALTER TASK OPS.APPLY_TASK SUSPEND;
ALTER TASK OPS.AUDIT_TASK SUSPEND;
-- Wait for any in-flight run to finish before modifying the graph.
SHOW TASKS IN SCHEMA OPS;

ALTER TASK OPS.APPLY_TASK MODIFY AS CALL ADV_ORDERS.SILVER.APPLY_EVENTS_SNOWPARK();
ALTER TASK OPS.AUDIT_TASK RESUME;

-- Add one duplicate delivery in a distinct synthetic file lineage.
-- It intentionally bypasses file ingestion to isolate business replay behavior.
INSERT INTO BRONZE.EVENTS_RAW
SELECT PAYLOAD, 'snowpark/replay-e8', 1, CURRENT_TIMESTAMP()
FROM BRONZE.EVENTS_RAW
WHERE PAYLOAD:event_id::STRING = 'e8'
QUALIFY ROW_NUMBER() OVER (ORDER BY SOURCE_FILE, SOURCE_ROW) = 1;

EXECUTE TASK OPS.APPLY_TASK;

SELECT NAME, STATE, QUERY_ID, ERROR_MESSAGE, COMPLETED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(RESULT_LIMIT => 100))
WHERE DATABASE_NAME = 'ADV_ORDERS'
  AND SCHEMA_NAME = 'OPS'
ORDER BY SCHEDULED_TIME DESC;

-- Do not proceed until both latest task executions are SUCCEEDED.
SELECT COUNT(*) AS PENDING_ROWS FROM BRONZE.EVENTS_STREAM;
SELECT * FROM GOLD.ORDER_METRICS;
-- CHECKPOINT after success: stream 0; raw/inbox 11; current 3; active value 165.
-- Preserve cancellation and late-version protections. Repeating the handler
-- changes neither current order state nor event counts when stream is empty.
CALL SILVER.APPLY_EVENTS_SNOWPARK();
