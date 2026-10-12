/*----------------Snowflake Advanced 3-day class Lab:-------------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
Day 2 shutdown — preserve data and stop scheduled compute
1) Suspend Dynamic Table refresh and course tasks
2) Stop the lab data metric schedule and ingestion
3) Preserve governed Gold for Day 3
----------------------------------------------------------------------------------*/

USE ROLE ADV_LAB;
USE WAREHOUSE ADV_WH;
USE DATABASE ADV_ORDERS;
USE SCHEMA OPS;

ALTER SESSION SET TIMEZONE = 'UTC';
ALTER SESSION SET QUERY_TAG = 'ADV_2026_DAY2';

-- ──────────────────────────────────────────────────────────────────────────────
-- STEP 1 │ Stop the explicit background objects created by the course
-- ──────────────────────────────────────────────────────────────────────────────

USE ROLE ADV_LAB;
USE SECONDARY ROLES NONE;

ALTER TASK OPS.APPLY_TASK SUSPEND;
ALTER TASK OPS.AUDIT_TASK SUSPEND;
ALTER PIPE BRONZE.ORDERS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;

ALTER DYNAMIC TABLE GOLD.ORDER_METRICS_DT SUSPEND;
ALTER DYNAMIC TABLE GOLD.ORDER_DETAIL_DT SUSPEND;

-- Run these only if the DMF branch was associated and not detached in lab 10.
-- ALTER TABLE OPS.DQ_DEMO DROP DATA METRIC FUNCTION
--     GOVERNANCE.NEGATIVE_AMOUNT_COUNT ON (AMOUNT);
-- ALTER TABLE OPS.DQ_DEMO UNSET DATA_METRIC_SCHEDULE;

SHOW DYNAMIC TABLES IN SCHEMA GOLD;
SHOW TASKS IN SCHEMA OPS;
SELECT SYSTEM$PIPE_STATUS('ADV_ORDERS.BRONZE.ORDERS_PIPE');
-- Wait for already-running executions before any future reset.

-- USE WAREHOUSE does not resume compute by itself. Suspend only if running.
EXECUTE IMMEDIATE $$
DECLARE
    warehouse_state STRING;
BEGIN
    SHOW WAREHOUSES LIKE 'ADV_WH';

    SELECT "state" INTO :warehouse_state
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "name" = 'ADV_WH';

    IF (warehouse_state = 'STARTED') THEN
        ALTER WAREHOUSE ADV_WH SUSPEND;
    END IF;
END;
$$;
-- No DROP DATABASE or DROP ROLE: Day 3 reuses the same governance and Gold.
