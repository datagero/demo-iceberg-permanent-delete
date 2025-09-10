#!/bin/bash

echo "Waiting for Jupyter to be ready..."
sleep 30

echo "Opening Iceberg PII Deletion Demo notebook..."
echo "Notebook URL: https://${GITPOD_WORKSPACE_URL#https://}notebooks/iceberg_pii_deletion_demo.ipynb"

# Open the notebook in the browser
gp preview "https://${GITPOD_WORKSPACE_URL#https://}notebooks/iceberg_pii_deletion_demo.ipynb" &

echo "Notebook should now be opening in your browser!"
