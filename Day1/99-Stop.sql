/*----------------Snowflake Advanced 3-day class Lab:-------------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
1) Suspend tasks and pause ingestion
2) Check for in-flight work
3) Preserve the database for the next course day
----------------------------------------------------------------------------------*/

-- 99 | End-of-day stop: preserve data for Day 2
USE ROLE ADV_LAB;
USE DATABASE ADV_ORDERS;

ALTER TASK OPS.APPLY_TASK SUSPEND;

ALTER TASK OPS.AUDIT_TASK SUSPEND;

ALTER PIPE BRONZE.ORDERS_PIPE SET PIPE_EXECUTION_PAUSED=TRUE;
-- Suspension does not cancel an already executing task. Inspect task history;
-- wait for in-flight work to finish before any reset.
SHOW TASKS IN SCHEMA OPS;

SELECT SYSTEM$PIPE_STATUS('ADV_ORDERS.BRONZE.ORDERS_PIPE');

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
-- If warehouse is already suspended, that statement can report that state.
-- No DROP DATABASE here. Day 2 uses the same Bronze/Silver/Gold objects.
