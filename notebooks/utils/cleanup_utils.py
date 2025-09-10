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
            .parquet("s3a://warehouse/default/pii_data/data/orphaned_file_1.parquet")
                
        print("✓ Created orphaned Parquet files in the data directory")
        print("  - These files exist in S3 but are NOT referenced by Iceberg metadata")
        
    except Exception as e:
        print(f"Error creating orphaned files: {e}")


def upload_parquet_file(spark, data, file_path, description="Custom parquet file"):
    """
    Manually upload a parquet file to S3.
    This can be used to demonstrate how external files can be added to the data directory.
    
    Args:
        spark: Spark session
        data: List of dictionaries containing the data to upload
        file_path: S3 path where to upload the file
        description: Description of what this file contains
    """
    try:
        # Convert data to DataFrame
        df = spark.createDataFrame(data)
        
        # Write to S3 as Parquet
        df.write \
            .mode("overwrite") \
            .parquet(file_path)
        
        print(f"✅ Successfully uploaded {description}")
        print(f"   File path: {file_path}")
        print(f"   Records: {len(data)}")
        
        # Show the data that was uploaded
        print(f"\nData uploaded to {file_path}:")
        df.show(truncate=False)
        
    except Exception as e:
        print(f"Error uploading parquet file: {e}")


def examine_delete_files(spark, table_name="demo.default.pii_data"):
    """
    Examine delete files to show what data was supposedly 'deleted' but still exists.
    This is crucial for understanding the security implications of incomplete PII deletion.
    
    Args:
        spark: Spark session
        table_name: Fully qualified table name (default: "demo.default.pii_data")
    """
    print("=== Delete File Reader Utility ===")
    
    # Get all files from the table
    files_meta = spark.table(f"{table_name}.files")
    delete_files = files_meta.filter("content IN (1,2)").select("file_path", "content", "record_count")
    
    if delete_files.count() == 0:
        print("✅ No delete files found - all deletes have been properly applied!")
        return
    
    print(f"Found {delete_files.count()} delete file(s):")
    delete_files.show(truncate=False)
    
    # Read each delete file
    for row in delete_files.collect():
        file_path = row["file_path"]
        content_type = row["content"]
        record_count = row["record_count"]
        
        print(f"\n--- Examining {file_path} ---")
        print(f"Content type: {content_type} (1=position delete, 2=equality delete)")
        print(f"Record count: {record_count}")
        
        try:
            # Read the delete file
            delete_df = spark.read.parquet(file_path)
            print("Contents of delete file:")
            delete_df.show(truncate=False)
            
            # Show schema
            print("Schema of delete file:")
            delete_df.printSchema()
            
            # For position deletes, try to read the original data file to show what was "deleted"
            if content_type == 1:  # Position delete
                print("\n--- Position Delete Analysis ---")
                print("Position deletes reference specific rows in data files.")
                print("Let's examine what data was actually 'deleted':")
                
                # Get the data file path from the delete file
                # Position deletes have a 'file_path' column that points to the original data file
                if 'file_path' in delete_df.columns:
                    data_file_paths = delete_df.select('file_path').distinct().collect()
                    for data_file_row in data_file_paths:
                        data_file_path = data_file_row['file_path']
                        print(f"\nOriginal data file: {data_file_path}")
                        
                        try:
                            # Read the original data file
                            data_df = spark.read.parquet(data_file_path)
                            print("Contents of original data file (shows what was 'deleted'):")
                            data_df.show(truncate=False)
                            print("🚨 DANGER: This shows the exact PII data that was supposedly 'deleted'!")
                        except Exception as e:
                            print(f"Could not read original data file: {e}")
            
            print("\n🚨 DANGER: This shows the exact data that was supposedly 'deleted'!")
            print("   The delete file contains references to PII data that still exists in storage!")
            
        except Exception as e:
            print(f"Could not read delete file: {e}")
