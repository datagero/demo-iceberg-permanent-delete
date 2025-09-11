# Iceberg PII Data Deletion Demo

This repository demonstrates a process for permanently deleting Personally Identifiable Information (PII) from an Apache Iceberg table stored in a MinIO S3-compatible object store.

## Overview

The demonstration covers the following steps:
1.  **Setup**: An environment is spun up using Docker Compose, including a Spark service with Iceberg configured and a MinIO service for storage.
2.  **Table Creation**: A sample Iceberg table is created with a schema containing PII columns.
3.  **Data Seeding**: The table is populated with dummy data.
4.  **PII Deletion**: A function is executed to "erase" PII for a specific record. This is done by two methods: deleting a row and/or updating the PII columns to `NULL`.
5.  **Table Maintenance**: Iceberg maintenance operations (`expire_snapshots` and `rewrite_data_files`) are run to make the old data and snapshots unavailable and to remove the underlying data files containing the PII.
6.  **Validation**: We verify that the PII has been permanently removed by attempting to time-travel to a state before the deletion.

## Prerequisites

*   Docker
*   Docker Compose

## Running the Demo

### Option 1: Gitpod (Recommended)

[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/datagero/demo-iceberg-permanent-delete)

1.  **Click "Open in Gitpod"** above - services start automatically
2.  **Open MinIO Console** at http://localhost:9001 (admin/password) to explore storage
3.  **Open Jupyter** at http://localhost:8888 and run `iceberg_pii_deletion_demo.ipynb`
4.  **Watch storage changes** in MinIO as you execute each notebook cell

### Option 2: Local Setup

1.  **Start the services**:
    ```bash
    docker-compose up -d
    ```
    This will start a MinIO container and a Spark container with a Jupyter notebook environment.

2.  **Access MinIO (Optional)**:
    You can access the MinIO console at [http://localhost:9001](http://localhost:9001).
    *   **Username**: `admin`
    *   **Password**: `password`

3.  **Access Jupyter Notebook**:
    The Jupyter notebook server will be running at [http://localhost:8888](http://localhost:8888). Open this URL in your browser. You should see the `iceberg_pii_deletion_demo.ipynb` notebook in the file list.

4.  **Run the Notebook**:
    Open the `iceberg_pii_deletion_demo.ipynb` notebook and execute the cells in order to see the full process. The notebook is self-contained and includes explanations for each step.

5.  **Shutdown**:
    When you are finished, you can stop and remove the containers:
    ```bash
    docker-compose down
    ```

## Storage Layer Exploration

**For the best learning experience**, keep the MinIO console open while running the notebook to observe how Iceberg manages data:

- **Before starting**: Check that the `warehouse` bucket is empty
- **During table creation**: Watch metadata files appear
- **During data seeding**: Observe Parquet files being created  
- **During PII deletion**: See new snapshots and metadata updates
- **During maintenance**: Watch old files being removed and new ones created

This real-time exploration shows why simple SQL `DELETE` or `UPDATE` isn't enough for PII compliance in Iceberg.

## Key Takeaways: Why Simple DELETE Isn't Enough

Iceberg's time travel feature means deleted data can still be accessed through historical snapshots. For PII compliance, you need **permanent deletion** using Iceberg maintenance operations.

## Permanent Deletion Commands

### Step 1: Logical Deletion (All Tables)
```sql
UPDATE your_table SET pii_column = NULL WHERE condition;
```

### Step 2: Physical Deletion (MOR Tables Only)
**Check if your table is MOR or COW:**
```sql
-- Check table properties
DESCRIBE EXTENDED your_table;
-- Look for 'write.delete.mode' = 'merge-on-read' (MOR) or 'copy-on-write' (COW)
-- if write.delete.mode isn’t set, Spark defaults to COW (Copy-on-Write)
```

**MOR (Merge-on-Read)**: Deleted rows remain in data files until rewritten
**COW (Copy-on-Write)**: Data files are immediately rewritten with deletes

```sql
-- For MOR tables only - rewrite data files to physically purge deleted data
CALL your_catalog.system.rewrite_data_files(
    table => 'your_schema.your_table',
    options => map('rewrite-all', 'true')
);
CALL your_catalog.system.rewrite_position_delete_files(
    table => 'your_schema.your_table',
    options => map('rewrite-all', 'true')
);
```

### Step 3: Expire Snapshots (All Tables)
```sql
-- ⚠️ WARNING: Use timestamp older than your longest query runtime
-- This prevents breaking active queries that might reference old snapshots
CALL your_catalog.system.expire_snapshots('your_schema.your_table', TIMESTAMP '2024-01-01 00:00:00');
```

### Step 4: Clean Orphaned Files (All Tables)
```sql
-- PySpark has built-in protection for files <3 days old
-- Still safe to run immediately after GDPR request, or wait a few days for maintenance
CALL your_catalog.system.remove_orphan_files(
    table => 'your_schema.your_table',
    older_than => TIMESTAMP '2024-01-01 00:00:00'
);
```

### ⚠️ Critical Warnings
- **Timing**: Expire snapshots older than your longest query runtime to avoid breaking active queries
- **MOR vs COW**: Only MOR tables need Step 2 (rewrite data files)
- **Safety**: PySpark protects files <3 days old from orphaned file cleanup

# Extra JAR Dependencies for Spark + Iceberg + MinIO

Spark-Iceberg images do **not** include:
- **Hadoop S3A** → required to use `s3://` with MinIO/S3.

## Required JARs

### Hadoop S3A (match Hadoop 3.3.4 in container)
```bash
mkdir -p jars

curl -L -o jars/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

curl -L -o jars/aws-java-sdk-bundle-1.12.262.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
````

## Mount in `docker-compose.yml`

```yaml
volumes:
  - ./jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/hadoop-aws-3.3.4.jar:ro
  - ./jars/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar:ro
```
