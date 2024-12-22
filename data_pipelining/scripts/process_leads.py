import os
import pandas as pd
from utils import load_email_formats, load_bad_emails

# Define folders relative to the root directory
root_dir = os.path.dirname(os.path.abspath(__file__))
input_folder = os.path.join(root_dir, "../input/")
output_folder = os.path.join(root_dir, "../output/")
logs_folder = os.path.join(root_dir, "../logs/")
data_folder = os.path.join(root_dir, "../data/")
processed_log = os.path.join(logs_folder, "processed_files.log")
merged_output_file = os.path.join(output_folder, "merged_processed_leads.xlsx")

# Placeholder: Stub for "bad emails" logic
def check_against_bad_emails(email):
    bad_emails = load_bad_emails()
    return email not in bad_emails

# Function: Clean a single dataset
def clean_data(df):
    """Clean and normalize data."""
    print("Cleaning data...")
    initial_rows = len(df)

    # Ensure column names are uppercase and trimmed
    df.columns = df.columns.str.strip().str.upper()

    # Remove duplicates
    df.drop_duplicates(subset=["FIRST NAME", "LAST NAME", "COMPANY"], inplace=True)
    print(f"Dropped {initial_rows - len(df)} duplicate rows.")

    # Normalize capitalization for names and company
    df["FIRST NAME"] = df["FIRST NAME"].str.title()
    df["LAST NAME"] = df["LAST NAME"].str.title()
    df["COMPANY"] = df["COMPANY"].str.title()

    # Drop rows with missing mandatory fields
    mandatory_fields = ["FIRST NAME", "LAST NAME", "COMPANY"]
    missing_fields = df[mandatory_fields].isnull().any(axis=1).sum()
    df.dropna(subset=mandatory_fields, inplace=True)
    print(f"Dropped {missing_fields} rows with missing mandatory fields.")

    # Fill remaining empty cells with blank strings
    df.fillna("", inplace=True)

    print("Data cleaning complete.")
    return df

# Function: Guess email addresses
def guess_email(row, email_formats):
    """Generate email address based on known or generic patterns."""
    first = row["FIRST NAME"].lower()
    last = row["LAST NAME"].lower()
    company = row["COMPANY"].lower().replace(" ", "")  # Remove spaces from company name
    domain = f"{company}.com"

    # Use known pattern if available
    if domain in email_formats:
        try:
            known_pattern = email_formats[domain]
            return known_pattern.format(
                first=first,
                last=last,
                first_0=first[0] if first else ""
            )
        except Exception as e:
            print(f"Error applying known pattern for {domain}: {e}")

    # Fall back to generic patterns
    return f"{first}.{last}@{domain}"  # Use the most common pattern

# Function: Process all input files
def process_all_leads(input_folder, output_folder, merged_output_file=None):
    """Process all input files, clean data, guess emails, and save results."""
    # Ensure output and logs folders exist
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(logs_folder, exist_ok=True)

    # Track all cleaned leads for optional merging
    all_leads = []

    # Load processed files log
    processed_files = set()
    if os.path.exists(processed_log):
        with open(processed_log, "r") as log_file:
            processed_files = set(line.strip() for line in log_file)

    # Load email formats
    email_formats_path = os.path.join(data_folder, "email_formats.json")
    email_formats = load_email_formats(email_formats_path)

    # Process each Excel file in the input folder
    for file_name in os.listdir(input_folder):
        # Skip temporary and invalid files
        if file_name.startswith("~$"):
            print(f"Skipping temporary file: {file_name}")
            continue

        if file_name.endswith(".xlsx") and file_name not in processed_files:
            input_file = os.path.join(input_folder, file_name)
            print(f"Processing {input_file}...")
            try:
                # Load the dataset
                leads = pd.read_excel(input_file)

                # Clean data
                leads = clean_data(leads)

                # Guess emails
                leads["EMAIL"] = leads.apply(guess_email, axis=1, email_formats=email_formats)
                leads = leads[leads["EMAIL"].apply(check_against_bad_emails)]
                print(f"Processed {len(leads)} valid leads from {file_name}.")

                # Save processed leads to the output folder
                output_file = os.path.join(output_folder, f"processed_{file_name}")
                leads.to_excel(output_file, index=False)
                print(f"Processed leads saved to {output_file}")

                # Append to all_leads for optional merging
                all_leads.append(leads)

                # Update processed files log
                with open(processed_log, "a") as log_file:
                    log_file.write(f"{file_name}\n")
            except Exception as e:
                print(f"Error processing file {file_name}: {e}")

    # Merge all leads into a single file (if specified)
    if merged_output_file and all_leads:
        print("Merging all processed leads into a single file...")
        merged_leads = pd.concat(all_leads, ignore_index=True)
        initial_merged_rows = len(merged_leads)

        # Remove duplicates across all files
        merged_leads.drop_duplicates(subset=["FIRST NAME", "LAST NAME", "COMPANY", "EMAIL"], inplace=True)
        print(f"Dropped {initial_merged_rows - len(merged_leads)} duplicate rows during merge.")

        merged_leads.to_excel(merged_output_file, index=False)
        print(f"All leads merged and saved to {merged_output_file}")

# Example usage
if __name__ == "__main__":
    process_all_leads(input_folder, output_folder, merged_output_file)
