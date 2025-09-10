#!/bin/bash
# Simple script to convert Python file to Jupyter notebook

if [ $# -ne 2 ]; then
    echo "Usage: $0 <input_py_file> <output_ipynb_file>"
    echo "Example: $0 notebooks/iceberg_pii_deletion_demo.py notebooks/iceberg_pii_deletion_demo.ipynb"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="$2"

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file '$INPUT_FILE' does not exist."
    exit 1
fi

# Run the conversion
echo "Converting $INPUT_FILE to $OUTPUT_FILE..."
python convert_python_to_notebook.py "$INPUT_FILE" "$OUTPUT_FILE"

if [ $? -eq 0 ]; then
    echo "✅ Conversion completed successfully!"
    echo "You can now open $OUTPUT_FILE in Jupyter Lab/Notebook"
else
    echo "❌ Conversion failed!"
    exit 1
fi
