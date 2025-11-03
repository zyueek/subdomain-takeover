import os
import csv
import pandas as pd
import argparse
import warnings
warnings.filterwarnings("ignore")

# Set up argument parsing
parser = argparse.ArgumentParser(description="Filter GitHub.io rows from CSV files.")
parser.add_argument('--start', type=int, required=True, help="Starting batch index.")
parser.add_argument('--end', type=int, required=True, help="Ending batch index (inclusive).")
args = parser.parse_args()

# Define input and output directories
in_dir = 'cname_sha_repo'
out_dir = 'io_output'
os.makedirs(out_dir, exist_ok=True)

# Define columns for the output DataFrame
columns = ['sha', 'username', 'repo']

# Iterate over each CSV file in the specified range
for batch_num in range(args.start, args.end + 1):
    input_csv = os.path.join(in_dir, f"{batch_num}.csv")
    output_csv = os.path.join(out_dir, f"{batch_num}.csv")
    
    # Initialize an empty DataFrame to store filtered results
    df = pd.DataFrame(columns=columns)

    # Process the input file if it exists
    if os.path.isfile(input_csv):
        with open(input_csv, mode='r', newline='') as file:
            reader = csv.reader(file)
            for row in reader:
                # Ensure there are at least two columns to process
                if len(row) >= 2:
                    try:
                        # Keep only the first line of the second column
                        second_column = row[1].splitlines()[1]
                    except IndexError:
                        continue  # Skip if there is no second line

                    # Remove everything before the first ";" in the second column
                    formatted_value = second_column.split(';', 1)[-1]

                    # Filter rows that contain "github.io" and satisfy the length conditions
                    if "github.io" in formatted_value and len(formatted_value) < 100 and len(formatted_value.split(';')) <= 1:
                        try:
                            # Split the value by '_' to get username and repo
                            username, repo = formatted_value.split('_')
                            df = df.append({'sha': row[0], 'username': username, 'repo': repo}, ignore_index=True)
                        except ValueError:
                            print(f"Skipping row due to formatting issues: {formatted_value}")

        # Save the filtered DataFrame to the output CSV
        df.to_csv(output_csv, index=False)
        print(f"Filtered data saved to {output_csv}")

    else:
        print(f"Input file {input_csv} does not exist. Skipping.")

print("Processing complete.")

