import csv
import subprocess
import re
import os
import argparse
import sys
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Increase CSV field size limit to avoid field limit errors
csv.field_size_limit(sys.maxsize)

# Set up argument parsing
parser = argparse.ArgumentParser(description="Process domains in 404 CSV files, check for CNAME, and filter based on repo.")
parser.add_argument('--start', type=int, required=True, help="Starting batch index.")
parser.add_argument('--end', type=int, required=True, help="Ending batch index (inclusive).")
parser.add_argument('--threads', type=int, default=10, help="Number of concurrent threads.")
args = parser.parse_args()

# Define directories
inputdir = 'sha_content'
outputdir_cname = 'sha_content_cname'
outputdir_final = 'valid_domain'
outputdir_other = 'valid_domain_other'

# Create necessary directories if they don't exist
os.makedirs(outputdir_cname, exist_ok=True)
os.makedirs(outputdir_final, exist_ok=True)
os.makedirs(outputdir_other, exist_ok=True)

cname_pattern = re.compile(r'\sCNAME\s+([^\s]+)')

# Function to run the dig command and process the CNAME
def process_row(row):
    website = row.get('matched pattern')
    cname_record = 'N/A'

    if len(website.split('.')) == 3:
        try:
            # Execute the dig command to retrieve CNAME
            result = subprocess.run(f"dig {website}", shell=True, capture_output=True, text=True)
            dig_output = result.stdout

            # Find CNAME in the output
            cname_match = cname_pattern.search(dig_output)
            if cname_match:
                cname_record = cname_match.group(1)

            # Add CNAME record to the row
            row['CNAME'] = cname_record

            # Check if the CNAME matches the 'repo' value
            if cname_record == row.get('repo') + '.':
                return row, "valid"

            # Check if CNAME has four segments when split by '_'
            if len(cname_record.split('.')) == 4 and cname_record.split('.')[-2] != 'io':
                return row, "other"

        except subprocess.CalledProcessError as e:
            row['CNAME'] = 'Error'
    else:
        row['CNAME'] = 'N/A'

    return row, "cname"

# Iterate over the specified range of batch files
for batch_num in range(args.start, args.end + 1):
    input_csv = os.path.join(inputdir, f"{batch_num}.csv")
    output_csv_cname = os.path.join(outputdir_cname, f"{batch_num}.csv")
    output_csv_final = os.path.join(outputdir_final, f"{batch_num}.csv")
    output_csv_other = os.path.join(outputdir_other, f"{batch_num}.csv")

    # Check if the input CSV exists
    if not os.path.isfile(input_csv):
        print(f"Input file {input_csv} does not exist. Skipping.")
        continue

    # Lists to store rows for different outputs
    cname_rows = []
    valid_rows = []
    other_rows = []

    # Open and read the input CSV file
    with open(input_csv, mode='r', newline='') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['CNAME']  # Add 'CNAME' to the list of field names

        rows = list(reader)

    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = {executor.submit(process_row, row): row for row in rows}

        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Processing batch {batch_num}"):
            try:
                processed_row, row_type = future.result()
                cname_rows.append(processed_row)

                if row_type == "valid":
                    valid_rows.append(processed_row)
                elif row_type == "other":
                    other_rows.append(processed_row)

            except Exception as e:
                print(f"Error processing row: {e}")

    # Save the rows with CNAME to the output file in 'sha_content_cname'
    if cname_rows:
        with open(output_csv_cname, mode='w', newline='') as outfile_cname:
            writer = csv.DictWriter(outfile_cname, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cname_rows)
        print(f"CNAME records saved to {output_csv_cname}")

    # Save the rows with matching CNAME and repo to the 'valid_domain' directory
    if valid_rows:
        with open(output_csv_final, mode='w', newline='') as outfile_final:
            writer = csv.DictWriter(outfile_final, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(valid_rows)
        print(f"Matching CNAME and repo rows saved to {output_csv_final}")

    # Save rows with CNAME having four segments when split by '_' to the 'valid_domain_other' directory
    if other_rows:
        with open(output_csv_other, mode='w', newline='') as outfile_other:
            writer = csv.DictWriter(outfile_other, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(other_rows)
        print(f"CNAME with four segments saved to {output_csv_other}")

print("Processing complete.")

