#!/bin/bash
set -e

# This script validates a path (file or directory) by checking the md5 checksums of all files within it against their expected hashes.
# Usage: ./dt_path_validate.sh <path>
# Example: ./dt_path_validate.sh data/

# Read input parameter
input_path="$1"
if [ -z "$input_path" ]; then
    echo "Usage: $0 <path>"
    exit 1
fi

# Check if the input path exists within the DVC project
if [ dvc list . "$input_path" &> /dev/null ]; then
    echo "Validating path: $input_path"
else
    echo "Error: Path '$input_path' does not exist in the DVC project."
    exit 1
fi

# Use dt_hash_validate to validate each file in the specified path
# Find all files in the specified path and validate each one
dvc list -R --show-hash . "$input_path" \
    | while read -r hash file; do
        if [ -n "$hash" ] && [ -n "$file" ]; then
            echo "Validating file: $file with hash: $hash"
            ./dt_hash_validate.sh "$hash"
        fi
    done