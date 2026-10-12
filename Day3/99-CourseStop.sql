/*----------------Snowflake Advanced 3-day class Lab:-------------------------------
-- Copyright © 2026 Innovation In Software Corporation. All rights reserved.
Final shutdown — stop all explicitly created services
1) Stop all course refresh and task schedules
2) Remove optional search/QAS maintenance if enabled
3) Preserve or deliberately retire the training resources after review
----------------------------------------------------------------------------------*/

USE ROLE ADV_LAB;
USE WAREHOUSE ADV_WH;
USE DATABASE ADV_ORDERS;
USE SCHEMA OPS;

ALTER SESSION SET TIMEZONE = 'UTC';
ALTER SESSION SET QUERY_TAG = 'ADV_2026_DAY3';

-- ──────────────────────────────────────────────────────────────────────────────
-- STEP 1 │ Stop services while retaining reviewable evidence
-- ──────────────────────────────────────────────────────────────────────────────

USE ROLE ADV_LAB;
USE SECONDARY ROLES NONE;

ALTER TASK OPS.APPLY_TASK SUSPEND;
ALTER TASK OPS.AUDIT_TASK SUSPEND;
ALTER PIPE BRONZE.ORDERS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER DYNAMIC TABLE GOLD.ORDER_METRICS_DT SUSPEND;
ALTER DYNAMIC TABLE GOLD.ORDER_DETAIL_DT SUSPEND;
ALTER MATERIALIZED VIEW PERF.DAILY_ORDER_MV SUSPEND;

-- Optional branches only if their objects were created:
-- ALTER TABLE PERF.ORDERS_SEARCH DROP SEARCH OPTIMIZATION;
-- ALTER TABLE PERF.ORDERS_SORTED SUSPEND RECLUSTER;
-- ALTER WAREHOUSE ADV_QAS_WH SET ENABLE_QUERY_ACCELERATION = FALSE;
-- ALTER WAREHOUSE ADV_QAS_WH SUSPEND;
-- ALTER WAREHOUSE ADV_TUNE_WH SUSPEND;
-- DMF lab 10 detaches its metric; verify no association remains on OPS.DQ_DEMO.

SHOW DYNAMIC TABLES IN SCHEMA GOLD;
SHOW TASKS IN SCHEMA OPS;
SHOW MATERIALIZED VIEWS IN SCHEMA PERF;

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
-- Account retention/deletion is a deliberate later administrative action.
-- Do not DROP DATABASE as part of classroom shutdown; preserve query IDs/evidence.
