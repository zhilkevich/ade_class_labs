/*----------------Snowflake Advanced 3-day class Lab:-------------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Classify valid and rejected events
2) Maintain the latest order state with deterministic precedence
3) Consume a stream transactionally and verify repeat execution
----------------------------------------------------------------------------------*/

-- 03 | Typed Silver, rejects, deterministic current state
USE ROLE ADV_LAB;
USE WAREHOUSE ADV_WH;
USE DATABASE ADV_ORDERS;
USE SCHEMA SILVER;

CREATE TABLE EVENT_INBOX LIKE BRONZE.EVENTS_RAW;

CREATE VIEW TYPED_EVENTS AS
SELECT
    payload,
    source_file,
    source_row,
    scan_time,
    
    NULLIF(TRIM(payload:event_id::STRING),'') event_id,
    NULLIF(TRIM(payload:order_id::STRING),'') order_id,
    TRY_TO_DECIMAL(payload:version::STRING, 38, 9) version,
    TRY_TO_TIMESTAMP_TZ(payload:event_time::STRING) event_time,
    payload:status::STRING status,
    TRY_TO_DECIMAL(payload:amount::STRING, 12, 2) amount,
    payload:region::STRING region
FROM EVENT_INBOX;

CREATE VIEW CLASSIFIED_EVENTS AS
SELECT
    *,
    CASE
        WHEN event_id IS NULL OR order_id IS NULL THEN 'MISSING_KEY'
        WHEN version IS NULL OR version < 1 OR version <> TRUNC(version) THEN 'BAD_VERSION'
        WHEN event_time IS NULL THEN 'BAD_TIME'
        WHEN status IS NULL OR status NOT IN ('PLACED','PAID','CANCELLED') THEN 'BAD_STATUS'
        WHEN amount IS NULL OR amount < 0 THEN 'BAD_AMOUNT'
        WHEN region IS NULL OR region NOT IN ('US','EU') THEN 'BAD_REGION'
        ELSE NULL
    END AS reject_reason
FROM TYPED_EVENTS;

CREATE VIEW REJECTED_EVENTS AS
SELECT *
FROM CLASSIFIED_EVENTS
WHERE reject_reason IS NOT NULL;
-- A conflicting payload for the same event ID must block this toy pipeline.
-- The lab never resolves conflicting immutable events by choosing a random row.
CREATE VIEW EVENT_CONFLICTS AS
SELECT event_id
FROM CLASSIFIED_EVENTS
WHERE reject_reason IS NULL
GROUP BY event_id HAVING COUNT(DISTINCT TO_JSON(payload)) > 1;

CREATE TABLE ORDERS_CURRENT
(
    order_id STRING,
    event_id STRING,
    version NUMBER,
    event_time TIMESTAMP_TZ,
    status STRING,
    amount NUMBER(12, 2),
    region STRING
);

-- ──────────────────────────────────────────────────────────────────────────────
-- STEP 1: A transaction couples stream consumption with the Silver update.
-- ──────────────────────────────────────────────────────────────────────────────
-- Stream rows enter a durable inbox; rejected records remain explainable.
-- The small teaching implementation ranks inbox history. Affected-order processing is a production extension.
CREATE PROCEDURE APPLY_EVENTS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    conflicts NUMBER;
    bad_event EXCEPTION (-20001, 'Conflicting payloads for immutable event_id');
BEGIN
    BEGIN TRANSACTION;

    -- Capture deliveries and consume the stream only if the transaction commits.
    INSERT INTO SILVER.EVENT_INBOX
    SELECT payload, source_file, source_row, scan_time
    FROM BRONZE.EVENTS_STREAM;

    SELECT COUNT(*) INTO :conflicts
    FROM SILVER.EVENT_CONFLICTS;

    IF (conflicts > 0) THEN
        RAISE bad_event;
    END IF;

    -- Rebuild candidates from the preserved inbox for this small teaching fixture.
    MERGE INTO SILVER.ORDERS_CURRENT t
    USING
    (
        SELECT order_id, event_id, version, event_time, status, amount, region
        FROM SILVER.CLASSIFIED_EVENTS
        WHERE reject_reason IS NULL
        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY order_id
            ORDER BY version DESC, event_time DESC, event_id DESC,
                     source_file, source_row
        ) = 1
    ) s
    ON t.order_id = s.order_id

        WHEN MATCHED AND
    (
        s.version > t.version
        OR (s.version = t.version AND s.event_time > t.event_time)
        OR (s.version = t.version AND s.event_time = t.event_time
            AND s.event_id > t.event_id)
    )
    THEN UPDATE SET
        event_id = s.event_id,
        version = s.version,
        event_time = s.event_time,
        status = s.status,
        amount = s.amount,
        region = s.region

        WHEN NOT MATCHED THEN
        INSERT (order_id, event_id, version, event_time, status, amount, region)
        VALUES (s.order_id, s.event_id, s.version, s.event_time,
                s.status, s.amount, s.region);

    COMMIT;
    RETURN 'Applied';

EXCEPTION
        WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
$$;

CALL APPLY_EVENTS();

SELECT *
FROM ORDERS_CURRENT
ORDER BY order_id;

-- ──────────────────────────────────────────────────────────────────────────────
-- CHECKPOINT: o1/e4/v2/PAID/120; o2/e5/v2/CANCELLED/50; o3/e6/v2/PLACED/30.
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    event_id,
    reject_reason
FROM REJECTED_EVENTS; -- e3 / BAD_AMOUNT
SELECT COUNT(*)
FROM BRONZE.EVENTS_STREAM; -- 0
-- ──────────────────────────────────────────────────────────────────────────────
-- STEP 2: Call again; inbox stays 8 and current orders stays 3.
-- ──────────────────────────────────────────────────────────────────────────────
CALL APPLY_EVENTS();

SELECT COUNT(*)
FROM EVENT_INBOX;
-- A lower-version late arrival cannot resurrect a cancelled order.
-- Equal versions use event_time then event_id as a documented classroom rule.
-- Production contracts should reject ambiguous versions instead of lexical ties.
