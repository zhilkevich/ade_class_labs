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
