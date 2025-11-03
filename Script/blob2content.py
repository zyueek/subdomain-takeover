import os
import csv
import subprocess
import re
import argparse

# Set up argument parsing
parser = argparse.ArgumentParser(description="Filter blob content based on patterns and save outputs.")
parser.add_argument('--start', type=int, required=True, help="Starting batch index.")
parser.add_argument('--end', type=int, required=True, help="Ending batch index (inclusive).")
args = parser.parse_args()

# Define input and output directories
inputdir = 'sha2blob'
outputdir = 'sha_content'
os.makedirs(outputdir, exist_ok=True)  # Ensure output directory exists

# Define the pattern to filter content
pattern = re.compile(r'^(?!.*\.(?:js|css|xml|png|jpg|src|svg|io)$)(?=.*[^\d.])(?:[^.\n]+\.){1,2}[^.\n]+$')

# Iterate over the specified range of batch files
for batch_num in range(args.start, args.end + 1):
    print(batch_num)
    input_csv = os.path.join(inputdir, f"{batch_num}.csv")
    output_csv = os.path.join(outputdir, f"{batch_num}.csv")

    # Check if the input CSV exists
    if not os.path.isfile(input_csv):
        print(f"Input file {input_csv} does not exist. Skipping.")
        continue

    # List to store rows with the new 'matched pattern' column
    output_rows = []

    # Read the blob IDs from the input CSV
    with open(input_csv, mode='r', newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            blob_id = row.get('blob id')
            
            if blob_id and blob_id != 'N/A':
                # Execute the bash command for each blob ID
                command = f"echo {blob_id} | ~/lookup/showCnt blob"
                try:
                    # Run the command and capture the output
                    result = subprocess.run(command, shell=True, capture_output=True, text=True)
                    output = result.stdout.strip()
                    
                    # Find matching patterns in the command output
                    matched_patterns = pattern.findall(output)
                    print(matched_patterns)
                    # Save matched patterns in the row (join if multiple patterns found)
                    row['matched pattern'] = ' | '.join(matched_patterns) if matched_patterns else 'No Match'
                    
                except:
#                    print(f"Error processing blob id {blob_id}: {e}")
                    row['matched pattern'] = 'Error'
            else:
                row['matched pattern'] = 'N/A'
                
            # Append the processed row to the output list
            output_rows.append(row)

    # Write the updated rows with the new 'matched pattern' column to the output CSV
    with open(output_csv, mode='w', newline='') as outfile:
        # Determine fieldnames from the first row
        fieldnames = output_rows[0].keys() if output_rows else []
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        
        # Write the header and rows
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"Processed data with matched patterns saved to {output_csv}")

print("Processing complete.")

