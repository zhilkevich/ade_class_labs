/*----------------Snowflake Advanced 3-day class Lab:-------------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Define the order-event contract
2) Create deterministic JSON fixtures using SQL
3) Unload files to an internal stage and verify their contents
----------------------------------------------------------------------------------*/

-- 01 | Project contract and SQL-only fixture creation
-- Instructor runs each STEP; everyone repeats and checks before continuing.
USE ROLE ADV_LAB;
USE WAREHOUSE ADV_WH;
USE DATABASE ADV_ORDERS;
USE SCHEMA BRONZE;

ALTER SESSION SET TIMEZONE='UTC';
-- One JSON row is a full order-state event, not a monetary delta.
-- event_id identifies an immutable event; order_id identifies an order.
-- version is positive and increases per order. Cancellation remains a tombstone.
-- USD only; amount is the full order amount. Event times are UTC.
CREATE TABLE FIXTURE
(
    batch NUMBER,
    payload VARIANT
);
INSERT INTO FIXTURE
SELECT
    column1,
    PARSE_JSON(column2)
FROM VALUES
(1,'{"event_id":"e1","order_id":"o1","version":1,"event_time":"2026-11-02T09:00:00Z","status":"PLACED","amount":"100.00","region":"US"}'),
(1,'{"event_id":"e2","order_id":"o2","version":1,"event_time":"2026-11-02T09:01:00Z","status":"PLACED","amount":"50.00","region":"EU"}'),
(1,'{"event_id":"e2","order_id":"o2","version":1,"event_time":"2026-11-02T09:01:00Z","status":"PLACED","amount":"50.00","region":"EU"}'),
(1,'{"event_id":"e3","order_id":"o3","version":1,"event_time":"2026-11-02T09:02:00Z","status":"PLACED","amount":"oops","region":"US"}'),
(2,'{"event_id":"e4","order_id":"o1","version":2,"event_time":"2026-11-02T10:00:00Z","status":"PAID","amount":"120.00","region":"US"}'),
(2,'{"event_id":"e5","order_id":"o2","version":2,"event_time":"2026-11-02T10:01:00Z","status":"CANCELLED","amount":"50.00","region":"EU"}'),
(2,'{"event_id":"e6","order_id":"o3","version":2,"event_time":"2026-11-02T10:02:00Z","status":"PLACED","amount":"30.00","region":"US"}'),
(2,'{"event_id":"e7","order_id":"o1","version":1,"event_time":"2026-11-02T08:59:00Z","status":"PLACED","amount":"90.00","region":"US"}');

CREATE FILE FORMAT EVENTS_JSON
    TYPE = JSON
    COMPRESSION = GZIP;

CREATE STAGE EVENTS_STAGE
    FILE_FORMAT = EVENTS_JSON;

CREATE TABLE EVENTS_RAW
(
    payload VARIANT,
    source_file STRING,
    source_row NUMBER,
    scan_time TIMESTAMP_LTZ
);
-- Create BEFORE ingest. A SELECT does not consume this append-only stream.
CREATE STREAM EVENTS_STREAM
    ON TABLE EVENTS_RAW
    APPEND_ONLY = TRUE;
-- SQL unloading generates each participant's own files. No Python or shared bucket.
COPY INTO @EVENTS_STAGE/batch1/events.json.gz
FROM
(
    SELECT payload
    FROM FIXTURE
    WHERE batch = 1
)
FILE_FORMAT = (TYPE = JSON COMPRESSION = GZIP)
SINGLE = TRUE
OVERWRITE = FALSE;

COPY INTO @EVENTS_STAGE/batch2/events.json.gz
FROM
(
    SELECT payload
    FROM FIXTURE
    WHERE batch = 2
)
FILE_FORMAT = (TYPE = JSON COMPRESSION = GZIP)
SINGLE = TRUE
OVERWRITE = FALSE;
LIST @EVENTS_STAGE;

SELECT
    batch,
    COUNT(*)
FROM FIXTURE
GROUP BY batch
ORDER BY batch;

-- ──────────────────────────────────────────────────────────────────────────────
-- CHECKPOINT: 2 staged files; 4 fixture rows in each batch.
-- ──────────────────────────────────────────────────────────────────────────────
-- Do not rerun the unloads. Duplicate file paths and load metadata are separate
-- from duplicate business event IDs. We test both explicitly in lab 02/04.
