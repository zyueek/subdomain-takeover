import os
import subprocess
import csv
import argparse

# Set up argument parsing
parser = argparse.ArgumentParser(description="Process SHA batches and save outputs to CSV files.")
parser.add_argument('--start', type=int, required=True, help="Starting batch index.")
parser.add_argument('--end', type=int, required=True, help="Ending batch index (inclusive).")
args = parser.parse_args()

# Directory to save the output files
output_dir = "cname_sha_repo"
os.makedirs(output_dir, exist_ok=True)

# Set the batch size for saving to CSV
batch_size = 5000
current_batch = args.start

# Open and read the input file just once
try:
    with open("cname_sha/output_part_1.txt", "r") as file:
        sha_list = file.read().splitlines()

        # Start processing from the appropriate line number
        start_index = batch_size * args.start
        end_index = min(batch_size * (args.end + 1), len(sha_list))

        # Ensure start index is within the bounds of the list
        if start_index >= len(sha_list):
            raise ValueError("Start index exceeds the length of the SHA list.")

        # Iterate through the SHAs for the specified batch range
        sha_outputs = []
        for idx in range(start_index, end_index):
            sha = sha_list[idx]
            if current_batch > args.end:
                break  # Stop if we have reached beyond the end batch

            try:
                # Execute the command
                command = f"echo {sha} | ~/lookup/getValues c2p"
                result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

                # Append the SHA and the result to the list
                sha_outputs.append([sha, result.stdout.strip()])

                # Check if batch size is reached
                if len(sha_outputs) == batch_size:
                    # Save to a CSV file named based on the current batch number
                    csv_file = os.path.join(output_dir, f"{current_batch}.csv")
                    with open(csv_file, mode='w', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(["SHA", "Command Output"])
                        writer.writerows(sha_outputs)

                    print(f"Batch {current_batch} saved to {csv_file}")

                    # Clear the list for the next batch and increment the batch counter
                    sha_outputs.clear()
                    current_batch += 1

            except Exception as e:
                print(f"Error processing SHA {sha}: {e}")

    # Save any remaining outputs if they don't complete a full batch
    if sha_outputs and current_batch <= args.end:
        csv_file = os.path.join(output_dir, f"{current_batch}.csv")
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["SHA", "Command Output"])
            writer.writerows(sha_outputs)

        print(f"Final batch {current_batch} saved to {csv_file}")

except FileNotFoundError:
    print("File cname_sha/output_part_1.txt not found. Make sure the input file is available.")
except ValueError as e:
    print(e)

print("Processing complete.")

