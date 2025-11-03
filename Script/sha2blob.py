import os
import csv
import subprocess
import argparse

# Set up argument parsing
parser = argparse.ArgumentParser(description="Process SHA to blob IDs and save outputs.")
parser.add_argument('--start', type=int, required=True, help="Starting batch index.")
parser.add_argument('--end', type=int, required=True, help="Ending batch index (inclusive).")
args = parser.parse_args()

# Define directories for input and output
inputdir = 'valid_io'
outputdir = 'sha2blob'
os.makedirs(outputdir, exist_ok=True)  # Ensure output directory exists

# Iterate over the specified range of batch files
for batch_num in range(args.start, args.end + 1):
    input_csv = os.path.join(inputdir, f"{batch_num}.csv")
    output_csv = os.path.join(outputdir, f"{batch_num}.csv")

    # Check if the input CSV exists
    if not os.path.isfile(input_csv):
        print(f"Input file {input_csv} does not exist. Skipping.")
        continue

    # List to store rows with the new 'blob id' column
    rows = []

    # Read the SHA values from the input CSV
    with open(input_csv, mode='r', newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            sha = row['sha']
            
            # Execute the bash command for each SHA
            command = f"echo {sha} | ~/lookup/getValues c2b"
            try:
                # Run the command and capture the output
                result = subprocess.run(command, shell=True, capture_output=True, text=True)
                
                # Process the command output to extract the blob id
                output_lines = result.stdout.strip().splitlines()
                
                if len(output_lines) >= 2:
                    # Extract the second line of the output as the 'blob id'
                    blob_id_line = output_lines[1]
                    blob_id = blob_id_line.split(';')[-1]  # Get the blob id from the second output line
                    
                    # Add the 'blob id' to the row
                    row['blob id'] = blob_id
                else:
                    # If the command output is incomplete, add a placeholder or leave empty
                    row['blob id'] = 'N/A'
                    
            except subprocess.CalledProcessError as e:
                print(f"An error occurred while executing the command for SHA {sha}: {e}")
                row['blob id'] = 'Error'

            # Append the row with the 'blob id' added to the list
            rows.append(row)

    # Write the updated rows with the new 'blob id' column to the output CSV
    with open(output_csv, mode='w', newline='') as outfile:
        # Determine fieldnames from the first row
        fieldnames = rows[0].keys() if rows else []
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        
        # Write the header and rows
        writer.writeheader()
        writer.writerows(rows)

    print(f"Processed data with blob IDs saved to {output_csv}")

print("Processing complete.")

