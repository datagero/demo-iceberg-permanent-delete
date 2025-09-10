#!/usr/bin/env python3
"""
Fixed script to convert iceberg_pii_deletion_demo.py to iceberg_pii_deletion_demo.ipynb

This script properly handles markdown formatting and preserves line breaks.
"""

import json
import re
import sys
from pathlib import Path


def parse_python_file(file_path):
    """Parse the Python file and extract cells with proper formatting."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    cells = []
    lines = content.split('\n')
    current_cell = []
    current_cell_type = None
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Check for markdown cell markers
        if line.strip().startswith('# %% [markdown]'):
            # Save previous cell if exists
            if current_cell and current_cell_type:
                cells.append({
                    'type': current_cell_type,
                    'content': '\n'.join(current_cell)
                })
            
            # Start new markdown cell
            current_cell = []
            current_cell_type = 'markdown'
            i += 1
            continue
            
        # Check for code cell markers
        elif line.strip().startswith('# %%'):
            # Save previous cell if exists
            if current_cell and current_cell_type:
                cells.append({
                    'type': current_cell_type,
                    'content': '\n'.join(current_cell)
                })
            
            # Start new code cell
            current_cell = []
            current_cell_type = 'code'
            i += 1
            continue
            
        # Add line to current cell
        if current_cell_type:
            if current_cell_type == 'markdown':
                # Remove markdown comment prefix
                if line.startswith('# '):
                    current_cell.append(line[2:])
                elif line.startswith('#'):
                    current_cell.append(line[1:])
                else:
                    current_cell.append(line)
            else:  # code
                current_cell.append(line)
        
        i += 1
    
    # Add final cell
    if current_cell and current_cell_type:
        cells.append({
            'type': current_cell_type,
            'content': '\n'.join(current_cell)
        })
    
    return cells


def create_notebook_cells(parsed_cells):
    """Convert parsed cells to Jupyter notebook format."""
    notebook_cells = []
    
    for cell in parsed_cells:
        if cell['type'] == 'markdown':
            # For markdown, split by newlines to create proper array
            markdown_content = cell['content']
            # Split into lines, preserving empty lines
            lines = markdown_content.split('\n')
            
            notebook_cell = {
                "cell_type": "markdown",
                "metadata": {},
                "source": lines
            }
        else:  # code
            # For code, also split by newlines
            code_content = cell['content']
            lines = code_content.split('\n') if code_content else [""]
            
            notebook_cell = {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": lines
            }
        
        notebook_cells.append(notebook_cell)
    
    return notebook_cells


def create_notebook(notebook_cells):
    """Create the complete Jupyter notebook structure."""
    notebook = {
        "cells": notebook_cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.8.5"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    return notebook


def main():
    """Main function to convert Python file to Jupyter notebook."""
    if len(sys.argv) != 3:
        print("Usage: python convert_python_to_notebook_fixed.py <input_py_file> <output_ipynb_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    # Check if input file exists
    if not Path(input_file).exists():
        print(f"Error: Input file '{input_file}' does not exist.")
        sys.exit(1)
    
    try:
        # Parse the Python file
        print(f"Parsing {input_file}...")
        parsed_cells = parse_python_file(input_file)
        print(f"Found {len(parsed_cells)} cells")
        
        # Create notebook cells
        print("Creating notebook cells...")
        notebook_cells = create_notebook_cells(parsed_cells)
        
        # Create the notebook
        print("Creating notebook structure...")
        notebook = create_notebook(notebook_cells)
        
        # Write the notebook
        print(f"Writing notebook to {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(notebook, f, indent=2, ensure_ascii=False)
        
        print("✅ Conversion completed successfully!")
        
    except Exception as e:
        print(f"Error during conversion: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
