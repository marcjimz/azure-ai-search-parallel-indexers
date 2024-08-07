#!/bin/bash

# Define the source file name
source_file="OP_CVRD_RCPNT_PRFL_SPLMTL_P06282024_06122024.csv"

# Extract the header
head -n 1 "$source_file" > header.csv

# Split the file without the header
tail -n +2 "$source_file" | split -b 10M - "${source_file%.csv}_part_"

# Prepend the header to each split file, truncate the last line using sed, and remove trailing empty lines
count=1
for file in "${source_file%.csv}_part_"*
do
    new_filename="${source_file%.csv}_part${count}.csv"
    # Prepend the header, truncate the last line using sed, and remove trailing empty lines
    (cat header.csv; sed '$d' "$file" | sed '/^$/d') > "$new_filename"
    rm "$file"
    count=$((count + 1))
done

# Clean up the header file
rm header.csv

# Iterate through the generated files and remove the trailing line and empty lines
for file in "${source_file%.csv}_part"*.csv
do
    # Remove the last line and any trailing empty lines
    awk 'NR>1 {print last} {last=$0} END {ORS=""; print last}' "$file" > temp.csv
    mv -f temp.csv "$file"
done