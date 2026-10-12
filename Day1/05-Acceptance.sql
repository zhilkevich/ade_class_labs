/*----------------Snowflake Advanced 3-day class Lab:-------------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Verify exact pipeline state and control totals
2) Demonstrate rollback without stream consumption
3) Explain replay, cancellation and operational correctness
----------------------------------------------------------------------------------*/

-- 05 | Day 1 acceptance and handoff
USE ROLE ADV_LAB;
USE WAREHOUSE ADV_WH;
USE DATABASE ADV_ORDERS;
USE SCHEMA OPS;
-- Each row must say PASS after labs 01-04. Save results and query IDs.
SELECT
    'Bronze rows' check_name,
    IFF(COUNT(*)=10,'PASS','FAIL') result
FROM BRONZE.EVENTS_RAW
UNION ALL SELECT 'Inbox rows', IFF(COUNT(*)=10,'PASS','FAIL')
FROM SILVER.EVENT_INBOX
UNION ALL SELECT 'Current orders', IFF(COUNT(*)=3,'PASS','FAIL')
FROM SILVER.ORDERS_CURRENT
UNION ALL SELECT 'Rejects', IFF(COUNT(*)=1,'PASS','FAIL')
FROM SILVER.REJECTED_EVENTS
UNION ALL SELECT 'Stream drained', IFF(COUNT(*)=0,'PASS','FAIL')
FROM BRONZE.EVENTS_STREAM
UNION ALL SELECT 'No conflicts', IFF(COUNT(*)=0,'PASS','FAIL')
FROM SILVER.EVENT_CONFLICTS
UNION ALL SELECT 'Gold value', IFF(SUM(active_amount_usd)=165,'PASS','FAIL')
FROM GOLD.ORDER_METRICS;
-- Exact state check, beyond aggregate counts. Expect zero rows BOTH ways.
WITH expected AS (
SELECT
    column1 order_id,
    column2 event_id,
    column3 version,
    column4 status,
    column5 amount
FROM VALUES ('o1','e4', 2,'PAID', 120), ('o2','e5', 2,'CANCELLED', 50), ('o3','e8', 3,'PAID', 45))
SELECT
    order_id,
    event_id,
    version,
    status,
    amount
FROM SILVER.ORDERS_CURRENT
MINUS SELECT *
FROM expected;
WITH expected AS (
SELECT
    column1 order_id,
    column2 event_id,
    column3 version,
    column4 status,
    column5 amount
FROM VALUES ('o1','e4', 2,'PAID', 120), ('o2','e5', 2,'CANCELLED', 50), ('o3','e8', 3,'PAID', 45))
SELECT *
FROM expected MINUS
SELECT
    order_id,
    event_id,
    version,
    status,
    amount
FROM SILVER.ORDERS_CURRENT;
-- Committed DML consumes a stream; rolled-back DML must not. Demonstrate on
-- separate temporary objects so the course pipeline's state remains intact.
CREATE TEMP TABLE STREAM_DEMO

(
    id NUMBER
);

CREATE STREAM STREAM_DEMO_CHANGES ON TABLE STREAM_DEMO APPEND_ONLY=TRUE;

CREATE TEMP TABLE STREAM_DEMO_SINK

(
    id NUMBER
);
INSERT INTO STREAM_DEMO VALUES(1);
BEGIN TRANSACTION;
INSERT INTO STREAM_DEMO_SINK SELECT id
FROM STREAM_DEMO_CHANGES;
ROLLBACK;

SELECT COUNT(*)
FROM STREAM_DEMO_CHANGES; -- 1
SELECT COUNT(*)
FROM STREAM_DEMO_SINK; -- 0
DROP STREAM STREAM_DEMO_CHANGES;
-- Written exit check: why does file replay differ from business duplication?
-- Why retain CANCELLED rows? Why does scheduling success not prove correctness?
