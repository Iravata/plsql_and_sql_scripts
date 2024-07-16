#!/bin/bash

# Define the S3 bucket path
S3_BUCKET="your-s3-bucket-name"

# List all feat_final.csv files and store them in a temporary file
aws s3 ls s3://$S3_BUCKET/trusted/na/na/ltr_data/features/ --recursive | grep 'feat_final.csv' > files_list.txt

# Loop through each line in the file
while IFS= read -r line
do
    # Extract the full path of the file
    FILE_PATH=$(echo $line | awk '{print $4}')
    
    # Extract the date from the path
    DATE=$(echo $FILE_PATH | awk -F'/' '{print $(NF-1)}')
    
    # Define the local file name
    LOCAL_FILE_NAME="feat_final_${DATE}.csv"
    
    # Download the file from S3 and rename it
    aws s3 cp s3://$S3_BUCKET/$FILE_PATH ./$LOCAL_FILE_NAME
done < files_list.txt

# Clean up the temporary file
rm files_list.txt

echo "Download and renaming completed."
