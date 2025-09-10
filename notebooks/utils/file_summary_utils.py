"""
File summary utilities for analyzing Iceberg table metadata and data files.
"""

import re
import datetime
import pandas as pd
from pyspark.sql import functions as F


def _format_exception_message(e: Exception) -> str:
    """Format exception messages for better readability."""
    msg = str(e)
    m = re.search(r"(NotFoundException|TABLE_OR_VIEW_NOT_FOUND|AnalysisException|ServiceFailureException)[^\n]*", msg)
    return m.group(0) if m else (msg.splitlines()[0] if msg else "Unknown error")


def summarize_files(spark, input_param: str,
                    operation_name: str,
                    save_path: str = None,
                    run_id: str = None):
    """
    Build minute-bucketed file summaries for Iceberg metadata + data.

    Returns: (meta_pd, data_pd, all_pd)  # Pandas
    """
    print(f"--- File Summary ({operation_name}) ---")

    table_name = input_param
    # If a location/path slipped in, map to the known table name you want to analyze.
    # (Best: pass a proper catalog.db.table so we can query metadata tables.)
    if input_param.startswith("s3a://warehouse/default/pii_data"):
        table_name = "demo.default.pii_data"
        print("Using table name for better reliability...")

    if run_id is None:
        import datetime as _dt
        run_id = _dt.datetime.utcnow().isoformat(timespec="seconds").replace(":", "-") + "Z"

    meta_df = None
    data_df = None

    # --- Metadata files ---
    try:
        metadata_query = f"""
        WITH manifest_lists AS (
          -- Each snapshot has exactly one manifest list file (*.avro)
          SELECT
            manifest_list                       AS file_path,
            committed_at                        AS created_at,
            'avro'                              AS file_format,
            'manifest_lists'                    AS file_type
          FROM {table_name}.snapshots
          WHERE manifest_list IS NOT NULL
        ),
        manifests_raw AS (
          -- A manifest can be referenced by added_snapshot_id (typical) or by snapshot_id
          SELECT
            m.path                              AS file_path,
            s1.committed_at                     AS committed_at_added,
            s2.committed_at                     AS committed_at_snap,
            'avro'                              AS file_format,
            'manifests'                         AS file_type
          FROM {table_name}.all_manifests m
          LEFT JOIN {table_name}.snapshots s1
            ON m.added_snapshot_id = s1.snapshot_id
          LEFT JOIN {table_name}.snapshots s2
            ON s1.snapshot_id = s2.snapshot_id
          WHERE m.path IS NOT NULL
        ),
        manifests AS (
          SELECT
            file_path,
            LEAST(committed_at_added, committed_at_snap) AS created_at,
            file_format,
            file_type
          FROM manifests_raw
          -- if both are NULL (shouldn't happen in practice), drop the row so we don't get NULL buckets
          WHERE LEAST(committed_at_added, committed_at_snap) IS NOT NULL
        ),
        metadata_json AS (
          -- Log entries are JSON files in metadata
          SELECT
            file                               AS file_path,
            timestamp                          AS created_at,
            'json'                             AS file_format,
            'metadata_log_entries'             AS file_type
          FROM {table_name}.metadata_log_entries
          WHERE file IS NOT NULL
        )
        SELECT
          'metadata'                                   AS prefix,
          file_type,
          file_format,
          date_trunc('minute', created_at)            AS created_minute,
          COUNT(*)                                     AS files_in_catalog
        FROM (
          SELECT * FROM manifest_lists
          UNION ALL
          SELECT * FROM manifests
          UNION ALL
          SELECT * FROM metadata_json
        )
        GROUP BY prefix, file_type, file_format, date_trunc('minute', created_at)
        """
        meta_df = (spark.sql(metadata_query)
                        .withColumn("run_id", F.lit(run_id))
                        .withColumn("operation", F.lit(operation_name)))
        print("\nMetadata file summary:")
        meta_df.orderBy("created_minute","file_type","file_format").show(truncate=False)
    except Exception as e:
        print(f"Metadata file summary unavailable: {_format_exception_message(e)}")

    # --- Data files ---
    try:
        debug_query = f"""
        SELECT
          e.data_file.file_path                                     AS file_path,
          CASE e.status WHEN 1 THEN 'ADDED' WHEN 2 THEN 'DELETED' WHEN 3 THEN 'EXISTING'
              ELSE CONCAT('UNKNOWN(', e.status, ')') END           AS status,
          s.committed_at                                            AS committed_at
        FROM {table_name}.all_entries e
        JOIN {table_name}.snapshots s USING (snapshot_id)
        ORDER BY s.committed_at DESC
        LIMIT 10
        """
        print("\nDebug - Recent entries since last snapshot:")
        spark.sql(debug_query).show(truncate=False)

        data_query = f"""
        WITH present AS (
          SELECT
            e.data_file.file_path       AS file_path,
            MIN(s.committed_at)         AS created_at,         -- first time this file is present
            MIN(e.data_file.content)    AS content             -- 0=data, 1=position_deletes, 2=equality_deletes
          FROM {table_name}.all_entries e
          LEFT JOIN {table_name}.snapshots s USING (snapshot_id)
          WHERE e.status != 2
          GROUP BY e.data_file.file_path
        )
        SELECT
          'data' AS prefix,
          CASE content
            WHEN 0 THEN 'data'
            WHEN 1 THEN 'position_deletes'
            WHEN 2 THEN 'equality_deletes'
            ELSE 'unknown'
          END AS file_type,
          'parquet' AS file_format,
          date_trunc('minute', created_at) AS created_minute,
          COUNT(*) AS files_in_catalog
        FROM present
        GROUP BY prefix, content, date_trunc('minute', created_at)
        """
        data_df = (spark.sql(data_query)
                        .withColumn("run_id", F.lit(run_id))
                        .withColumn("operation", F.lit(operation_name)))
        print("\nData file summary:")
        data_df.orderBy("created_minute","file_type").show(truncate=False)
    except Exception as e:
        print(f"Data file summary unavailable: {_format_exception_message(e)}")

    # --- Union with aligned schema ---
    cols = ["prefix","file_type","file_format","created_minute","files_in_catalog","run_id","operation"]
    def _empty_df():
        return spark.createDataFrame([], "prefix string, file_type string, file_format string, created_minute timestamp, files_in_catalog long, run_id string, operation string")
    if meta_df is None: meta_df = _empty_df()
    if data_df is None: data_df = _empty_df()
    all_df = meta_df.select(cols).unionByName(data_df.select(cols))

    if save_path:
        (all_df
          .repartition("run_id")
          .write.mode("append")
          .partitionBy("run_id")
          .parquet(f"{save_path.rstrip('/')}/summary"))
        print(f"\nSaved summary to {save_path.rstrip('/')}/summary (partitioned by run_id={run_id})")

    return meta_df.toPandas(), data_df.toPandas(), all_df.toPandas()
