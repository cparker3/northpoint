import os
import pandas as pd

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct paths relative to the script directory
input_folder = os.path.join(script_dir, "../input/")
output_folder = os.path.join(script_dir, "../output/")
logs_folder = os.path.join(script_dir, "../logs/")
processed_log = os.path.join(logs_folder, "processed_files.log")
merged_output_file = os.path.join(output_folder, "merged_processed_leads.xlsx")

# Placeholder: Stub for "bad emails" logic
def check_against_bad_emails(email):
    """
    Stub function to eventually check emails against the 'bad emails' list.
    For now, it always returns True (email is not bad).
    """
    # TODO: Implement this when SES feedback integration is ready.
    print(f"Checking {email} against bad emails list... (Not implemented)")
    return True  # Assume all emails are valid for now.

# Function: Clean a single dataset
def clean_data(df):
    # Ensure column names are uppercase and trimmed
    df.columns = df.columns.str.strip().str.upper()

    # Remove duplicates
    df.drop_duplicates(subset=["FIRST NAME", "LAST NAME", "COMPANY"], inplace=True)

    # Normalize capitalization for names and company
    df["FIRST NAME"] = df["FIRST NAME"].str.title()
    df["LAST NAME"] = df["LAST NAME"].str.title()
    df["COMPANY"] = df["COMPANY"].str.title()

    # Drop rows with missing mandatory fields
    mandatory_fields = ["FIRST NAME", "LAST NAME", "COMPANY"]
    df.dropna(subset=mandatory_fields, inplace=True)

    # Fill remaining empty cells with blank strings
    df.fillna("", inplace=True)

    return df

# Function: Guess email addresses
def guess_email(row):
    first = row["FIRST NAME"].lower()
    last = row["LAST NAME"].lower()
    company = row["COMPANY"].lower().replace(" ", "")  # Remove spaces from company name
    domain = f"{company}.com"
    email_patterns = [
        f"{first}.{last}@{domain}",
        f"{first}@{domain}",
        f"{first}{last}@{domain}",
    ]
    return email_patterns[0]  # Use the first pattern

# Function: Process all input files
def process_all_leads(input_folder, output_folder, merged_output_file=None):
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

    # Process each Excel file in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".xlsx") and file_name not in processed_files:
            input_file = os.path.join(input_folder, file_name)
            print(f"Processing {input_file}...")

            # Load the dataset
            leads = pd.read_excel(input_file)

            # Clean data
            leads = clean_data(leads)

            # Guess emails and check against "bad emails" (stubbed out)
            leads["EMAIL"] = leads.apply(guess_email, axis=1)
            leads = leads[leads["EMAIL"].apply(check_against_bad_emails)]  # Stub call

            # Save processed leads to the output folder
            output_file = os.path.join(output_folder, f"processed_{file_name}")
            leads.to_excel(output_file, index=False)
            print(f"Processed leads saved to {output_file}")

            # Append to all_leads for optional merging
            all_leads.append(leads)

            # Update processed files log
            with open(processed_log, "a") as log_file:
                log_file.write(f"{file_name}\n")

    # Merge all leads into a single file (if specified)
    if merged_output_file and all_leads:
        merged_leads = pd.concat(all_leads, ignore_index=True)
        merged_leads.drop_duplicates(subset=["FIRST NAME", "LAST NAME", "COMPANY", "EMAIL"], inplace=True)
        merged_leads.to_excel(merged_output_file, index=False)
        print(f"All leads merged and saved to {merged_output_file}")

# Example usage
if __name__ == "__main__":
    # Run the process_all_leads function with predefined paths
    process_all_leads(input_folder, output_folder, merged_output_file)
