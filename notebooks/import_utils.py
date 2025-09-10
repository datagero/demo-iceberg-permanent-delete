"""
Import script for utils modules.
This script imports all utility functions from the utils directory.
"""

import sys
import os

# Add the utils directory to the Python path
# In Jupyter notebooks, we use the current working directory
current_dir = os.getcwd()
utils_dir = os.path.join(current_dir, 'utils')

print(f"Current directory: {current_dir}")
print(f"Utils directory: {utils_dir}")
print(f"Utils exists: {os.path.exists(utils_dir)}")

if not os.path.exists(utils_dir):
    print(f"ERROR: Utils directory not found at {utils_dir}")
    print(f"Available in current directory: {os.listdir(current_dir)}")
    raise ImportError("Utils directory not found")

if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

# Import all utility functions
from s3_utils import ls_s3_with_date, ls_s3_recursive
from file_summary_utils import summarize_files, _format_exception_message
from diff_utils import diff_summaries
from cleanup_utils import cleanup_orphan_files, create_orphaned_files, upload_parquet_file, examine_delete_files

print(f"Successfully imported utilities from: {utils_dir}")
