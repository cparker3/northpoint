import os
import pandas as pd

# -----------------------------------------------------
# External Utils (imported from elsewhere)
# -----------------------------------------------------
from utils import load_email_formats, load_bad_emails

# -----------------------------------------------------
# File / Folder Config
# -----------------------------------------------------
root_dir = os.path.dirname(os.path.abspath(__file__))
input_folder = os.path.join(root_dir, "../input/")
output_folder = os.path.join(root_dir, "../output/")
logs_folder = os.path.join(root_dir, "../logs/")
data_folder = os.path.join(root_dir, "../data/")
processed_log = os.path.join(logs_folder, "processed_files.log")
merged_output_file = os.path.join(output_folder, "master_leads.xlsx")

# -----------------------------------------------------
# Check if email is in bad_emails
# -----------------------------------------------------
def check_against_bad_emails(email, debug=False):
    """
    Returns True if 'email' is NOT in the bad email set, False otherwise.
    """
    bad_emails = load_bad_emails()
    if debug:
        print(f"[DEBUG] Checking '{email}' against bad emails...")
    return email not in bad_emails

# -----------------------------------------------------
# Clean a single dataset
# -----------------------------------------------------
def clean_data(df, debug=False):
    """
    Clean and normalize data. Modifies 'df' in-place and returns it.
    - Ensures uppercase column names
    - Removes duplicates
    - Normalizes name/company casing
    - Drops rows missing mandatory fields
    - Fills remaining empty cells with blank strings
    """
    if debug:
        print("[DEBUG] Beginning data cleaning...")

    initial_rows = len(df)

    # Ensure column names are uppercase and trimmed
    df.columns = df.columns.str.strip().str.upper()

    # Remove duplicates
    df.drop_duplicates(subset=["FIRST NAME", "LAST NAME", "COMPANY"], inplace=True)
    if debug:
        print(f"[DEBUG] Dropped {initial_rows - len(df)} duplicate rows.")

    # Normalize capitalization for names and company
    df["FIRST NAME"] = df["FIRST NAME"].str.title()
    df["LAST NAME"]  = df["LAST NAME"].str.title()
    df["COMPANY"]    = df["COMPANY"].str.title()

    # Drop rows with missing mandatory fields
    mandatory_fields = ["FIRST NAME", "LAST NAME", "COMPANY"]
    missing_fields = df[mandatory_fields].isnull().any(axis=1).sum()
    df.dropna(subset=mandatory_fields, inplace=True)
    if debug:
        print(f"[DEBUG] Dropped {missing_fields} rows with missing mandatory fields.")

    # Fill remaining empty cells with blank strings
    df.fillna("", inplace=True)

    if debug:
        print("[DEBUG] Data cleaning complete.")
    return df

# -----------------------------------------------------
# Guess email address
# -----------------------------------------------------
def guess_email(row, email_formats, debug=False):
    """
    Generate email address based on:
    - A known pattern from 'email_formats' if available
    - Otherwise, a common fallback pattern: first.last@domain.com
    """
    first = row["FIRST NAME"].lower()
    last  = row["LAST NAME"].lower()
    company = row["COMPANY"].lower().replace(" ", "")  # Remove spaces from company name
    domain  = f"{company}.com"

    # Use known pattern if available
    if domain in email_formats:
        try:
            known_pattern = email_formats[domain]
            if debug:
                print(f"[DEBUG] Found known pattern '{known_pattern}' for '{domain}'")
            return known_pattern.format(
                first=first,
                last=last,
                first_0=first[0] if first else ""
            )
        except Exception as e:
            if debug:
                print(f"[DEBUG] Error applying known pattern for {domain}: {e}")

    # Fall back to generic pattern
    if debug:
        print(f"[DEBUG] No known pattern for '{domain}'. Using fallback 'first.last@{domain}'")
    return f"{first}.{last}@{domain}"

# -----------------------------------------------------
# Process all input files
# -----------------------------------------------------
def process_all_leads(
    input_folder, 
    output_folder, 
    merged_output_file=None, 
    debug=False
):
    """
    Process all Excel files in 'input_folder':
      - Skips already-processed files logged in 'processed_log'
      - Cleans data
      - Guesses emails
      - Excludes known bad emails
      - Saves processed leads individually
      - Optionally merges all leads into 'merged_output_file'
    """
    if debug:
        print("[DEBUG] Ensuring output and log folders exist...")

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
    if debug:
        if email_formats:
            print(f"[DEBUG] Loaded email formats for {len(email_formats)} domains.")
        else:
            print("[DEBUG] No email formats loaded or file not found.")

    # Process each file in the input folder
    for file_name in os.listdir(input_folder):
        # Skip temporary and invalid files
        if file_name.startswith("~$"):
            if debug:
                print(f"[DEBUG] Skipping temporary file: {file_name}")
            continue

        if file_name.endswith(".xlsx") and file_name not in processed_files:
            input_file = os.path.join(input_folder, file_name)
            if debug:
                print(f"[DEBUG] Processing '{input_file}'...")
            try:
                # Load dataset
                leads = pd.read_excel(input_file)

                # Clean data
                leads = clean_data(leads, debug=debug)

                # Guess emails
                leads["EMAIL"] = leads.apply(
                    lambda row: guess_email(row, email_formats, debug=debug),
                    axis=1
                )

                # Check against bad emails
                valid_mask = leads["EMAIL"].apply(
                    lambda x: check_against_bad_emails(x, debug=debug)
                )
                leads = leads[valid_mask]
                if debug:
                    print(f"[DEBUG] {sum(~valid_mask)} leads removed due to bad emails.")
                    print(f"[DEBUG] {len(leads)} valid leads remain in '{file_name}'.")

                # Save processed leads
                output_file = os.path.join(output_folder, f"processed_{file_name}")
                leads.to_excel(output_file, index=False)
                if debug:
                    print(f"[DEBUG] Processed leads saved to '{output_file}'")

                # Append to 'all_leads' for optional merging
                all_leads.append(leads)

                # Update processed files log
                with open(processed_log, "a") as log_file:
                    log_file.write(f"{file_name}\n")

            except Exception as e:
                print(f"Error processing file {file_name}: {e}")

    # Merge all leads into a single file (if specified)
    if merged_output_file and all_leads:
        if debug:
            print("[DEBUG] Merging all processed leads into a single file...")
        merged_leads = pd.concat(all_leads, ignore_index=True)
        initial_merged_rows = len(merged_leads)

        # Remove duplicates across all files
        merged_leads.drop_duplicates(
            subset=["FIRST NAME", "LAST NAME", "COMPANY", "EMAIL"],
            inplace=True
        )
        if debug:
            print(f"[DEBUG] Dropped {initial_merged_rows - len(merged_leads)} duplicates during merge.")

        merged_leads.to_excel(merged_output_file, index=False)
        if debug:
            print(f"[DEBUG] All leads merged and saved to '{merged_output_file}'")

# -----------------------------------------------------
# Main Function
# -----------------------------------------------------
if __name__ == "__main__":
    process_all_leads(
        input_folder,
        output_folder,
        merged_output_file,
        debug=False
    )