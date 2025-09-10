"""
Cleanup utilities for managing orphaned files and table maintenance.
"""

import datetime


def cleanup_orphan_files(spark, table_ident: str, location: str = None, 
                        cutoff: str = None, method: str = "action"):
    """
    Clean up orphan files in an Iceberg table.

    Args:
        spark: Spark session
        table_ident (str): Fully qualified table name, e.g. "demo.default.pii_data"
        location (str, optional): Path to scan. Defaults to table root.
        cutoff (str, optional):
            None       → use Iceberg's default safety window
            "immediate" → delete everything (sets cutoff to far-future)
            timestamp  → ISO-8601 timestamp string (e.g. "2025-09-01T00:00:00Z")
        method (str): "action" (DeleteOrphanFiles API) or "sql" (Spark SQL procedure).
    """
    jvm = spark._jvm
    js  = spark._jsparkSession

    if method == "sql":
        print("Running SQL procedure remove_orphan_files …")
        if cutoff is None:
            # Let Iceberg decide the cutoff (default safety window)
            result = spark.sql(f"""
                CALL demo.system.remove_orphan_files(table => '{table_ident}')
            """)
        else:
            cutoff_str = cutoff
            if cutoff == "immediate":
                cutoff_str = "2100-01-01 00:00:00"  # far-future
            else:
                cutoff_str = cutoff.replace("T", " ").replace("Z", "")
            result = spark.sql(f"""
                CALL demo.system.remove_orphan_files(
                    table => '{table_ident}',
                    older_than => TIMESTAMP '{cutoff_str}'
                )
            """)
        result.show(truncate=False)
        print("✓ Orphaned files cleanup (SQL) completed")
        return result

    elif method == "action":
        print("Running Iceberg DeleteOrphanFiles Action …")
        tbl = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(js, table_ident)

        action = jvm.org.apache.iceberg.spark.actions.SparkActions.get(js).deleteOrphanFiles(tbl)

        if cutoff:
            if cutoff == "immediate":
                cutoff_ms = jvm.java.time.Instant.parse("2100-01-01T00:00:00Z").toEpochMilli()
            else:
                cutoff_ms = jvm.java.time.Instant.parse(cutoff).toEpochMilli()
            action = action.olderThan(cutoff_ms)

        if location:
            action = action.location(location)

        result = action.execute()
        print(f"✓ Orphaned files cleanup (Action) completed for {table_ident}")
        return result

    else:
        raise ValueError("method must be 'action' or 'sql'")


def create_orphaned_files(spark):
    """
    Create some orphaned files to demonstrate cleanup.
    These files exist in S3 but are not referenced by Iceberg metadata.
    """
    try:
        # Create a fake Parquet file with some data
        orphan_data = [
            {"case_id": "orphan-1", "first_name": "Orphan", "email_address": "orphan@example.com", 
             "key_nm": "orphan_key", "secure_txt": "orphan secret", "secure_key": "orphan_secure", 
             "update_date": "2023-01-03"}
        ]
        
        # Convert to DataFrame and write as Parquet
        orphan_df = spark.createDataFrame(orphan_data)
        
        # Write directly to S3 (bypassing Iceberg metadata)
        orphan_df.write \
            .mode("overwrite") \
            .option("path", "s3a://warehouse/default/pii_data/data/orphaned_file_1.parquet") \
            .parquet("s3a://warehouse/default/pii_data/data/orphaned_file_1.parquet")
        
        # Create another orphaned file
        orphan_data2 = [
            {"case_id": "orphan-2", "first_name": "Another", "email_address": "another@example.com", 
             "key_nm": "another_key", "secure_txt": "another secret", "secure_key": "another_secure", 
             "update_date": "2023-01-04"}
        ]
        
        orphan_df2 = spark.createDataFrame(orphan_data2)
        orphan_df2.write \
            .mode("overwrite") \
            .option("path", "s3a://warehouse/default/pii_data/data/orphaned_file_2.parquet") \
            .parquet("s3a://warehouse/default/pii_data/data/orphaned_file_2.parquet")
        
        print("✓ Created 2 orphaned Parquet files in the data directory")
        print("  - These files exist in S3 but are NOT referenced by Iceberg metadata")
        
    except Exception as e:
        print(f"Error creating orphaned files: {e}")
