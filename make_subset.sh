#!/bin/bash

# Check if the input filename is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <input_filename>"
    exit 1
fi

# Input and output file names
input_file=$1
output_file="subsetAF.jsonl"

# Extract the first 5000 lines and save to subsetAF.jsonl
head -n 5000 "$input_file" > "$output_file"

echo "First 5000 lines of $input_file have been written to $output_file"
