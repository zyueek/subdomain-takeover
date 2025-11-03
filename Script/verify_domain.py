import csv
import requests
from tqdm import tqdm
import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up argument parsing
parser = argparse.ArgumentParser(description="Check subdomains and save 404 status to batch files.")
parser.add_argument('--start', type=int, required=True, help="Starting batch index.")
parser.add_argument('--end', type=int, required=True, help="Ending batch index (inclusive).")
parser.add_argument('--threads', type=int, default=10, help="Number of concurrent threads.")
args = parser.parse_args()

# Define directories
inputdir = 'valid_domain'             # Directory where input files are located
#updated_outputdir = 'sha_content_code'  # Directory to save updated files with all response codes
outputdir = 'result_domain'             # Directory to save only 404 status files
#os.makedirs(updated_outputdir, exist_ok=True)  # Create the updated output directory if it doesn't exist
os.makedirs(outputdir, exist_ok=True)          # Create the 404 directory if it doesn't exist

# Function to handle a single request and return the result
def check_website(row):
    website = row.get('matched pattern')
    if website and len(website.split('.')) == 3:
        try:
            response = requests.get(f"http://{website}", timeout=10)
            row['code'] = response.status_code
        except requests.ConnectionError:
            row['code'] = 'ConnectionError'
        except requests.Timeout:
            row['code'] = 'Timeout'
        except requests.RequestException as e:
            row['code'] = f"RequestException: {e}"
    else:
        row['code'] = 'N/A'
    return row

# Iterate over the specified range of batch files
for batch_num in range(args.start, args.end + 1):
    input_csv = os.path.join(inputdir, f"{batch_num}.csv")
#    updated_csv = os.path.join(updated_outputdir, f"{batch_num}.csv")
    output_csv = os.path.join(outputdir, f"{batch_num}.csv")

    # Check if the input CSV exists
    if not os.path.isfile(input_csv):
        print(f"Input file {input_csv} does not exist. Skipping.")
        continue

    # List to store rows with 404 status
    updated_rows = []
    not_found_rows = []

    # Read the input CSV file
    with open(input_csv, mode='r', newline='') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['code']  # Add 'code' to the list of field names
        rows = list(reader)

    # Use ThreadPoolExecutor for multi-threaded requests
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        future_to_row = {executor.submit(check_website, row): row for row in rows}
        for future in tqdm(as_completed(future_to_row), total=len(rows), desc=f"Processing {input_csv}"):
            row = future.result()
            updated_rows.append(row)
            if row['code'] == 404:
                not_found_rows.append(row)


    # Save only 404 rows to the specific batch file in '404_domain'
    if not_found_rows:
        with open(output_csv, mode='w', newline='') as not_found_file:
            writer = csv.DictWriter(not_found_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(not_found_rows)
        print(f"Subdomains with 404 status from {input_csv} saved to {output_csv}")
    else:
        print(f"No 404 subdomains found in {input_csv}, skipping {output_csv}")

print("Processing complete.")

