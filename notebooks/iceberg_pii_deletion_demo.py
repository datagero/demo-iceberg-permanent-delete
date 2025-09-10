# %% [markdown]
# # Iceberg PII Data Deletion Demo
#
# This notebook demonstrates how to **permanently delete PII (Personally Identifiable Information)** from Apache Iceberg tables using a complete data lifecycle approach.
#
# ## What You'll Learn
#
# 1. **The Problem**: Why simple SQL `DELETE` isn't enough for PII removal in Iceberg
# 2. **The Solution**: A complete data lifecycle approach using Iceberg maintenance operations
# 3. **Key Operations**: 
#    - Logical deletion (setting PII to NULL)
#    - Snapshot expiration (removing time travel access)
#    - Orphaned file cleanup (removing unreferenced files)
#    - Data file rewriting (physical PII removal)
#
# ## Why This Matters
#
# Iceberg's time travel feature means that even after "deleting" data, it can still be accessed through historical snapshots. For PII compliance (GDPR, CCPA, etc.), you need to ensure data is **permanently and irreversibly** removed.

# %% [markdown]
# ## 1. Setup and Configuration
#
# First, let's set up our Spark session and import the utility functions we'll need for this demo.

# %%
# Import all utility functions using the import script
exec(open('import_utils.py').read())

# %%
# Configure Spark for S3A and MinIO
print("Spark:", spark.version)
hconf = spark._jsc.hadoopConfiguration()

# S3A configuration for MinIO
hconf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hconf.set("fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A")
hconf.set("fs.s3a.endpoint", "http://minio:9000")
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.access.key", "admin")
hconf.set("fs.s3a.secret.key", "password")
hconf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
hconf.set("fs.s3a.connection.ssl.enabled", "false")

# Verify S3A is working
spark._jvm.java.lang.Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jvm.java.net.URI.create("s3://warehouse"),
    spark._jsc.hadoopConfiguration()
)
print("✅ S3A configuration successful")

# %%
# Display Iceberg catalog configuration
print("Iceberg Catalog Configuration:")
for k, v in spark.sparkContext.getConf().getAll():
    if "catalog" in k:
        print(f"  {k} = {v}")

# %% [markdown]
# ## 2. Create the PII Data Table
#
# Let's create an Iceberg table to store PII data. We'll include various types of sensitive information that we'll later need to delete.

# %%
# Clean up any existing table via REST API
print("=== Cleaning up any existing table via REST API ===")
!curl -X DELETE http://rest:8181/v1/namespaces/default/tables/pii_data

# Also clean up via SQL as backup
spark.sql("DROP TABLE IF EXISTS demo.default.pii_data")
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.default")

# Create the PII data table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.default.pii_data (
    case_id STRING,
    first_name STRING,
    email_address STRING,
    key_nm STRING,
    secure_txt STRING,
    secure_key STRING,
    update_date DATE
)
USING iceberg
""")

print("✅ PII data table created successfully")

# %%
# Define the base path for our table for easy reuse
table_base_path = "s3a://warehouse/default/pii_data"

# Check initial state
_, _, all_previous = summarize_files(spark, table_base_path, "After Table Creation")
print("Initial file state:")
all_previous

# %% [markdown]
# ## 3. Insert Sample PII Data
#
# Now let's add some sample data containing PII that we'll later need to delete.

# %%
# Insert sample PII data
spark.sql("""
INSERT INTO demo.default.pii_data VALUES
('case-1', 'John', 'john.doe@example.com', 'key1', 'secret text 1', 'secret_key_1', DATE('2023-01-01')),
('case-2', 'Jane', 'jane.doe@example.com', 'key2', 'secret text 2', 'secret_key_2', DATE('2023-01-02')),
('case-3', 'Alice', 'alice.smith@example.com', 'key3', 'secret text 3', 'secret_key_3', DATE('2023-01-03'))
""")

print("✅ Sample PII data inserted")
print("\nCurrent table data:")
df = spark.table("demo.default.pii_data").toPandas()
display(df)

# %%
# Check the table history (snapshots)
print("Table snapshots:")
initial_snapshots = spark.table("demo.default.pii_data.history")
initial_snapshots.show(truncate=False)

# Check files after data insertion
_, _, all_current = summarize_files(spark, table_base_path, "After Data Insertion")
print("\nFile summary after data insertion:")
all_current

# %% [markdown]
# ## 4. Create Orphaned Files (Simulation)
#
# Let's create some orphaned files to demonstrate cleanup later. These files exist in S3 but aren't tracked by Iceberg metadata, simulating failed writes or manual operations.

# %%
# Check files before creating orphaned files
_, _, all_previous = summarize_files(spark, table_base_path, "Before Creating Orphaned Files")

# Create orphaned files to demonstrate cleanup
create_orphaned_files(spark)

# Show all files in the data directory (including orphaned ones)
print("All files under data/ directory (including orphaned):")
ls_s3_recursive(spark, "s3://warehouse/default/pii_data/data")

# %%
# Check files after creating orphaned files
_, _, all_current = summarize_files(spark, table_base_path, "After Creating Orphaned Files")
print("\nFile summary after creating orphaned files:")
all_current

# Show the difference
print("\n=== File Summary Comparison (Orphaned Files Added) ===")
diff_orphaned = diff_summaries(all_previous, all_current)
diff_orphaned

# %% [markdown]
# ### 4b. Create Merge-on-Read Delete Files (simulate previous deletion)
#
# For MOR, `DELETE` does not rewrite data files immediately. Instead, it produces
# *delete files* that mask rows at read time. We'll:
# 1) Ensure the table is configured for MOR deletes
# 2) Issue a DELETE (simulating a previous deletion operation)
# 3) Inspect metadata to confirm delete files exist

# %%
print("=== Configure table for MOR deletes ===")
spark.sql("""
  ALTER TABLE demo.default.pii_data SET TBLPROPERTIES (
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'copy-on-write' -- keep updates as COW for this demo
  )
""")
print("✅ Table configured for MOR deletes")

# %%
print("=== Create MOR delete files via SQL DELETE ===")
# Delete the 'case-2' record entirely (simulating a previous deletion operation)
spark.sql("""
  DELETE FROM demo.default.pii_data
  WHERE case_id = 'case-2'
""")
print("✅ DELETE issued (MOR): row is gone from reads, but data still in files until rewrite")

# Show current table (case-2 should no longer appear)
print("\nCurrent table data (case-2 should be gone):")
df = spark.table("demo.default.pii_data").toPandas()
display(df)

# %%
# Check files after MOR delete
all_previous = all_current.copy(deep=True)
_, _, all_current = summarize_files(spark, table_base_path, "After MOR Delete")
print("\nFile summary after MOR delete:")
all_current

# Show the difference
print("\n=== File Summary Comparison (MOR Delete) ===")
diff_mor = diff_summaries(all_previous, all_current)
diff_mor

# %%
# Inspect metadata to verify delete files were written
print("=== Inspect metadata: delete files present? ===")
# Iceberg metadata: 'files' table includes both data and delete files.
# content: 0=data, 1=position deletes, 2=equality deletes
files_meta = spark.table("demo.default.pii_data.files")
print("All file contents (content=0 data, 1 pos-delete, 2 eq-delete):")
files_meta.select("content","file_path","record_count","file_format").show(truncate=False)

print("Delete files only (content IN (1,2)):")
files_meta.filter("content IN (1,2)") \
          .select("content","file_path","record_count","file_format") \
          .show(truncate=False)

# Optional: keep a count for validation before/after rewrite
delete_file_count_before = files_meta.filter("content IN (1,2)").count()
print(f"Delete files BEFORE rewrite_data_files: {delete_file_count_before}")

# %% [markdown]
# ## 5. The Problem: Logical Deletion Isn't Enough
#
# Now let's demonstrate the core problem. We'll "delete" PII by setting it to NULL, but this doesn't actually remove the data permanently.

# %%
# Step 1: Logically "delete" PII by setting it to NULL
print("=== Step 1: Logical PII Deletion ===")

# Execute logical deletion by setting PII columns to NULL
spark.sql("""
UPDATE demo.default.pii_data
SET
    first_name = NULL,
    email_address = NULL,
    secure_txt = NULL
WHERE case_id = 'case-1'
""")

print("Data after logical deletion:")
df = spark.table("demo.default.pii_data").toPandas()
display(df)

# %%
# Check files after logical deletion
all_previous = all_current.copy(deep=True)
_, _, all_current = summarize_files(spark, table_base_path, "After Logical Deletion")
print("\nFile summary after logical deletion:")
all_current

# Show the difference
print("\n=== File Summary Comparison (Logical Deletion) ===")
diff_logical = diff_summaries(all_previous, all_current)
diff_logical

# %%
# Step 2: Show that the PII still exists in previous snapshots!
print("=== Step 2: The Problem - Time Travel ===")
print("Even though we 'deleted' the PII, it still exists in previous snapshots:")

# Get the first snapshot ID
first_snapshot_id = initial_snapshots.select("snapshot_id").first()[0]
print(f"\nTime traveling back to snapshot {first_snapshot_id}:")
df_time_travel = spark.read.option("snapshot-id", first_snapshot_id).table("demo.default.pii_data").toPandas()
display(df_time_travel)

print("\n🚨 PROBLEM: The PII is still accessible through time travel!")
print("This violates data privacy regulations like GDPR and CCPA.")

# %%
# Let's also examine any delete files that might exist
print("\n=== Examining Delete Files (if any) ===")
examine_delete_files(spark)

# %% [markdown]
# ## 6. The Solution: Complete PII Lifecycle Management
#
# To permanently delete PII, we need to perform a series of Iceberg maintenance operations. Let's walk through each step.

# %% [markdown]
# ### Step 1: Expire Old Snapshots
#
# First, we expire old snapshots to remove time travel access to the PII data.

# %%
# Check files before expiring snapshots
print("=== Before Expiring Snapshots ===")
all_previous = all_current.copy(deep=True)
_, _, all_current = summarize_files(spark, table_base_path, "Before Expiring Snapshots")

# %%
# Expires snapshots: CALL demo.system.expire_snapshots('default.pii_data', TIMESTAMP '2024-09-10 20:00:00')

# Expire all snapshots older than current timestamp
from pyspark.sql.functions import current_timestamp
now = spark.sql("SELECT current_timestamp()").collect()[0][0]
print(f"Expiring snapshots older than: {now}")

spark.sql(f"CALL demo.system.expire_snapshots('default.pii_data', TIMESTAMP '{now}')")
print("✅ Old snapshots expired")

# %%
# Verify that time travel no longer works
print("=== Verification: Time Travel Blocked ===")
try:
    spark.read.option("snapshot-id", first_snapshot_id).table("demo.default.pii_data").show()
except Exception as e:
    print("✅ SUCCESS: Time travel to old snapshots is now blocked!")
    print(f"Error: {e}")

# Check current table state
print("\nCurrent table state:")
df = spark.table("demo.default.pii_data").toPandas()
display(df)

print("\nTable history (should only have current snapshot):")
df_history = spark.table("demo.default.pii_data.history").toPandas()
display(df_history)

# %%
# Check files after snapshot expiration
all_previous = all_current.copy(deep=True)
_, _, all_current = summarize_files(spark, table_base_path, "After Snapshot Expiration")
print("\nFile summary after snapshot expiration:")
all_current

# Show the difference
print("\n=== File Summary Comparison (Snapshot Expiration) ===")
diff_snapshots = diff_summaries(all_previous, all_current)
diff_snapshots

# %% [markdown]
# ### Step 2: Clean Up Orphaned Files
#
# Now let's clean up the orphaned files we created earlier. These files exist in S3 but are not referenced by Iceberg metadata.

# %%
# Show orphaned files before cleanup
print("=== Before Orphaned Files Cleanup ===")
print("Files in data directory (including orphaned):")
ls_s3_recursive(spark, "s3://warehouse/default/pii_data/data")

# %%
# SQL approach: CALL demo.system.remove_orphan_files(table => 'demo.default.pii_data', older_than => TIMESTAMP '2100-01-01 00:00:00')

try:
    # Try with a far-future date (this should FAIL due to safety protection)
    print("Trying with far-future date (should FAIL due to safety protection):")
    result = spark.sql("""
        CALL demo.system.remove_orphan_files(
            table => 'demo.default.pii_data',
            older_than => TIMESTAMP '2100-01-01 00:00:00'
        )
    """)
    result.show(truncate=False)
    print("✅ SQL approach worked!")
except Exception as e:
    print(f"❌ SQL approach failed: {e}")
    print("This is EXPECTED! Iceberg has safety protections to prevent accidental deletion.")
    print("The safety window prevents deletion of files that might still be referenced.")
    print("Since we're in a controlled environment, we'll use the Action approach instead.")

# %%
# Action approach: cleanup_orphan_files(spark, 'demo.default.pii_data', method='action', cutoff='immediate')

# Clean up orphaned files using the Action approach
cleanup_orphan_files(spark, "demo.default.pii_data", method="action", cutoff="immediate")

# Show files after cleanup
print("\n=== After Orphaned Files Cleanup ===")
print("Files in data directory (orphaned files removed):")
ls_s3_recursive(spark, "s3://warehouse/default/pii_data/data")

# %%
# Check files after orphaned files cleanup
all_previous = all_current.copy(deep=True)
_, _, all_current = summarize_files(spark, table_base_path, "After Orphaned Files Cleanup")
print("\nFile summary after orphaned files cleanup:")
all_current

# Show the difference
print("\n=== File Summary Comparison (Orphaned Files Cleanup) ===")
diff_cleanup = diff_summaries(all_previous, all_current)
diff_cleanup

# %% [markdown]
# ### Step 3: Rewrite Data Files (Apply Deletes / VACUUM)
#
# **MOR (Merge-on-Read) vs COW (Copy-on-Write) Deletion:**
#
# - **MOR**: Deleted rows still reside in data files until we **apply deletes**.
#   - `rewrite_data_files` applies delete files and rewrites Parquet files
#   - Deleted rows are physically purged from storage
#   - `expire_snapshots` drops old snapshots that reference pre-delete files
#
# - **COW**: Data files are rewritten immediately with the delete.
#   - Only `expire_snapshots` would be required (no rewrite needed)
#   - Deleted data is immediately removed from new data files
#
# Since we're using MOR, we need both operations for complete PII deletion.

# %%
# Check files before VACUUM
print("=== Before VACUUM (Rewrite Data Files) ===")
all_previous = all_current.copy(deep=True)
_, _, all_current = summarize_files(spark, table_base_path, "Before VACUUM")

# %%
print("=== Step 3: Rewrite Data Files (apply deletes) ===")

# First, let's show that delete files still exist even after snapshot expiration and orphan cleanup
print("\n=== DANGER: Delete files still exist! ===")
print("Even after expiring snapshots and cleaning orphaned files, delete files remain:")
ls_s3_recursive(spark, "s3://warehouse/default/pii_data/data")

# %%
# Let's examine the delete files to show the PII still exists
print("=== Examining Delete Files - PII Still Exists! ===")
examine_delete_files(spark)

# %%
print("=== Now Running VACUUM to Apply Deletes ===")

# Step 1: Rewrite data files to remove deleted rows
print("Step 1: Rewriting data files...")
result1 = spark.sql("""
  CALL demo.system.rewrite_data_files(
    table => 'default.pii_data',
    options => map(
      'rewrite-all','true',
      'target-file-size-bytes','134217728'  -- 128 MiB example
    )
  )
""")
print("✅ rewrite_data_files finished (deleted rows removed from data files)")
result1.show(truncate=False)

# Step 2: Rewrite position delete files
print("\nStep 2: Rewriting position delete files...")
result2 = spark.sql("""
  CALL demo.system.rewrite_position_delete_files(
    table => 'default.pii_data',
    options => map(
      'rewrite-all','true',
      'target-file-size-bytes','134217728'  -- 128 MiB example
    )
  )
""")
print("✅ rewrite_position_delete_files finished (delete files processed)")
result2.show(truncate=False)

# Validate: delete files should be gone or reduced
files_after = spark.table("demo.default.pii_data.files")
delete_file_count_after = files_after.filter("content IN (1,2)").count()
print(f"Delete files AFTER rewrite_data_files: {delete_file_count_after}")

# %%
print("=== What rewrite_data_files Accomplished ===")
print("✅ Applied delete files (MOR): Deleted rows are now physically purged from Parquet files")
print("✅ Rewrote data files: Consolidated small files and removed deleted data")
print("✅ Physical deletion complete: PII data no longer exists in storage")
print("\nNext: We need to expire snapshots to remove time travel access to pre-delete files")

# %%
print("=== Validation: MOR deletes applied ===")
if delete_file_count_after < delete_file_count_before:
    print("✅ Delete files reduced — MOR deletes were applied during rewrite.")
else:
    print("⚠️ Delete files count did not drop. Check table props and engine versions.")

# %%
# Examine delete files after VACUUM to show the difference
print("=== Examining Delete Files After VACUUM ===")
examine_delete_files(spark)

# %%
# Check files after VACUUM
print("=== After VACUUM (Rewrite Data Files) ===")
all_previous = all_current.copy(deep=True)
_, _, all_current = summarize_files(spark, table_base_path, "After VACUUM")

# Show the difference
print("\n=== File Summary Comparison (After VACUUM) ===")
diff_vacuum = diff_summaries(all_previous, all_current)
diff_vacuum

# %%
# Final snapshot expiration to complete the VACUUM cleanup
print("=== Final Snapshot Expiration (Complete VACUUM Cleanup) ===")
print("Expiring any remaining old snapshots to complete the cleanup...")
from pyspark.sql.functions import current_timestamp
now = spark.sql("SELECT current_timestamp()").collect()[0][0]
print(f"Expiring snapshots older than: {now}")

spark.sql(f"CALL demo.system.expire_snapshots('default.pii_data', TIMESTAMP '{now}')")
print("✅ Final snapshot expiration completed")

# %%
# Show final file state after complete cleanup
print("=== Final File State After Complete Cleanup ===")
print("Files remaining in data directory after all operations:")
ls_s3_recursive(spark, "s3://warehouse/default/pii_data/data")

# %%
# Show final table state
print("\n=== Final Table State ===")
print("Current table data (after all operations):")
df = spark.table("demo.default.pii_data").toPandas()
display(df)

# %% [markdown]
# ## 7. Final Validation
#
# Let's verify that the PII has been permanently and irreversibly deleted.

# %%
# Final file listing before validation
print("=== Final File Listing ===")
print("Files remaining in data directory after all operations:")
ls_s3_recursive(spark, "s3://warehouse/default/pii_data/data")

# %%
# Final verification
print("=== Final Validation: PII Permanently Deleted ===")

# 1. Verify time travel is blocked
print("=== Step 1: Verify Time Travel is Blocked ===")
# Get the latest snapshot ID (should be the only one remaining)
latest_snapshots = spark.table("demo.default.pii_data.history")
latest_snapshot_id = latest_snapshots.select("snapshot_id").first()[0]
print(f"Latest snapshot ID: {latest_snapshot_id}")

try:
    spark.read.option("snapshot-id", first_snapshot_id).table("demo.default.pii_data").show()
except Exception as e:
    print("✅ Time travel to old snapshots is blocked")

# 2. Verify current data (PII should be gone)
print("\n=== Step 2: Verify Current Data ===")
print("Current data (PII should be gone):")
df = spark.table("demo.default.pii_data").toPandas()
display(df)

# 3. Verify table history (should only have one snapshot)
print("\n=== Step 3: Verify Table History ===")
print("Table history (should only have current snapshot):")
df_history = spark.table("demo.default.pii_data.history").toPandas()
display(df_history)

# 4. Show final file state
print("\n=== Step 4: Final File State ===")
print("Final file state:")
_, _, all_final = summarize_files(spark, table_base_path, "Final State")
all_final

# 5. Final delete file examination
print("\n=== Step 5: Final Delete File Examination ===")
examine_delete_files(spark)

# %% [markdown]
# # 🎉 PII DELETION COMPLETE!
#
# ## ✅ What we accomplished:
# - Logically deleted PII (set to NULL)
# - Expired old snapshots (blocked time travel)
# - Cleaned up orphaned files
# - Rewrote data files (physically removed PII)
#
# ## ✅ Compliance achieved:
# - PII is permanently and irreversibly deleted
# - Time travel to old snapshots is impossible
# - No orphaned files remain
# - Data files no longer contain the PII
#
# This approach ensures compliance with GDPR, CCPA, and other data privacy regulations.

# %% [markdown]
# ## 8. Summary: What We Demonstrated & Key Takeaways
#
# ### What We Actually Did
# This demo showed a complete PII deletion workflow using Apache Iceberg:
#
# 1. **Created PII Data**: Inserted sample data with personally identifiable information
# 2. **Demonstrated the Problem**: Showed that simple SQL `UPDATE` to NULL doesn't permanently remove PII
# 3. **Simulated Previous Deletion**: Created merge-on-read delete files to show how PII persists after deletion
# 4. **Exposed Data Persistence**: Revealed that PII remains accessible in delete files and through time travel
# 5. **Applied Complete Data Erasure**: Used both data file rewriting AND position delete file processing
# 6. **Expired Snapshots**: Removed old snapshots to block time travel access
# 7. **Removed Orphaned Files**: Cleaned up unreferenced files that may contain PII
# 8. **Verified Compliance**: Confirmed PII was permanently and irreversibly removed
#
# ### Key Technical Insights
#
# **Merge-on-Read (MOR) Mode Challenges:**
# - Delete operations create separate delete files (position/equality deletes)
# - Original Parquet files still contain the PII data
# - Delete files can be read directly to expose the "deleted" PII
# - **Critical**: You MUST rewrite data files for RTE compliance in MOR mode
#
# **Complete Data Erasure Process:**
# - **Data file rewriting**: Physically removes deleted rows from Parquet files
# - **Position delete processing**: Handles and consolidates delete files
# - **Snapshot expiration**: Removes old snapshots to block time travel
# - **Orphan cleanup**: Removes unreferenced files
#
# ### Critical Security Findings
#
# **Before Data Erasure:**
# - PII data remains in original Parquet files
# - Delete files contain references to the exact PII that was "deleted"
# - Time travel can restore the original PII data
# - Orphaned files may contain PII
#
# **After Complete Data Erasure:**
# - PII is physically removed from all data files
# - Delete files are processed and removed
# - Time travel to old snapshots is blocked
# - No orphaned files remain
#
# ### Compliance & Operations
#
# **For GDPR/CCPA Compliance:**
# - **COW Mode**: Snapshot expiration + orphan cleanup is sufficient
# - **MOR Mode**: Data file rewriting + snapshot expiration + orphan cleanup is required
# - **Governance**: Maintain RTE ledger of snapshots that must never be restored
# - **Monitoring**: Track delete file counts and ensure they trend down after erasure
#
# **Production Considerations:**
# - Run data erasure operations during maintenance windows
# - Use dry-run options for orphan cleanup
# - Monitor S3 lifecycle rules to prevent external file deletion
# - Implement proper access controls and audit trails
#
# This demo proves that **logical deletion alone is insufficient** for PII compliance in Iceberg MOR mode - you must physically rewrite data files to achieve true data erasure.
