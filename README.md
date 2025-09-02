# Iceberg PII Data Deletion Demo

This repository demonstrates a process for permanently deleting Personally Identifiable Information (PII) from an Apache Iceberg table stored in a MinIO S3-compatible object store.

## Overview

The demonstration covers the following steps:
1.  **Setup**: An environment is spun up using Docker Compose, including a Spark service with Iceberg configured and a MinIO service for storage.
2.  **Table Creation**: A sample Iceberg table is created with a schema containing PII columns.
3.  **Data Seeding**: The table is populated with dummy data.
4.  **PII Deletion**: A function is executed to "delete" PII for a specific record. This is done by updating the PII columns to `NULL`.
5.  **Table Maintenance**: Iceberg maintenance operations (`expire_snapshots` and `rewrite_data_files`) are run to make the old data and snapshots unavailable and to remove the underlying data files containing the PII.
6.  **Validation**: We verify that the PII has been permanently removed by attempting to time-travel to a state before the deletion.

## Prerequisites

*   Docker
*   Docker Compose

## Running the Demo

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
