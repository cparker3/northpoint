import os
import json
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
from utils import load_email_formats

# Define paths relative to the root directory
script_dir = os.path.dirname(os.path.abspath(__file__))
data_folder = os.path.join(script_dir, "../data/")
dynamic_email_format_db_path = os.path.join(data_folder, "dynamic_email_format_db.json")

# Load email format database
def load_dynamic_email_format_db(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return json.load(f)
    return {}

# Save email format database
def save_dynamic_email_format_db(dynamic_email_format_db, filepath):
    with open(filepath, "w") as f:
        json.dump(dynamic_email_format_db, f, indent=4)

# Validate an email using MillionVerifier
def verify_email_millionverifier(email, api_key="Zi1Q7CYUTEznhotJ0ImUpICyG", debug=False, retries=3):
    """
    Returns a dict, e.g. { "resultcode": 1, "subresult": "..." } if successful,
    or None if the request fails after all retries.
    """
    for attempt in range(retries):
        try:
            if debug:
                print(f"Attempt {attempt + 1}: Verifying email: {email}")
            response = requests.get(
                "https://api.millionverifier.com/api/v3/",
                params={"api": api_key, "email": email},
                timeout=10
            )
            response.raise_for_status()
            result = response.json()  # This is a dictionary

            if debug:
                print(f"Response: {result}")

            # Return the entire dictionary so we can check .get("resultcode") etc. later
            return result

        except Exception as e:
            print(f"Error verifying {email}: {e}")
            if attempt < retries - 1:
                print(f"Retrying ({attempt + 1}/{retries})...")
            else:
                print(f"All attempts failed for email: {email}")
                return None

# Update email format database
def update_dynamic_email_format_db(dynamic_email_format_db, company_domain, email_pattern):
    if company_domain not in dynamic_email_format_db:
        dynamic_email_format_db[company_domain] = {}
    dynamic_email_format_db[company_domain][email_pattern] = dynamic_email_format_db[company_domain].get(email_pattern, 0) + 1

# Get the dominant format for a company
def get_dominant_email_format(dynamic_email_format_db, company_domain):
    if company_domain in dynamic_email_format_db:
        return max(dynamic_email_format_db[company_domain], key=dynamic_email_format_db[company_domain].get)
    return None

# Validation function for parallel execution
def validate_email_parallel(row, dynamic_email_format_db, email_formats, debug=False):
    """Validates a single row and updates its status."""
    first_name = row["FIRST NAME"]
    last_name = row["LAST NAME"]
    company = row["COMPANY"]
    email = row["EMAIL"]
    company_domain = company.lower().replace(" ", "") + ".com"

    # Track emails already checked
    already_checked = {email}

    # Validate the existing email
    result = verify_email_millionverifier(email, debug=debug)
    if result:
        if result.get("resultcode") == 1:  # Definitively valid
            row["EMAIL STATUS"] = "Valid"
            row["VALIDATED EMAIL"] = email
            update_dynamic_email_format_db(dynamic_email_format_db, company_domain, email.split("@")[0])
            return row

        elif result.get("resultcode") == 2:  # Catch-all
            row["EMAIL STATUS"] = "Catch-All"
            row["VALIDATED EMAIL"] = email
            return row

    # Generate fallback patterns if validation fails or no valid emails are found
    fallback_patterns = [
        f"{first_name.lower()}@{company_domain}",
        f"{first_name.lower()}.{last_name.lower()}@{company_domain}",
        f"{first_name.lower()}_{last_name.lower()}@{company_domain}",
        f"{first_name[0].lower()}.{last_name.lower()}@{company_domain}",
    ]

    for pattern in fallback_patterns:
        if pattern in already_checked:
            continue
        already_checked.add(pattern)
        result = verify_email_millionverifier(pattern, debug=debug)
        if result and result.get("resultcode") == 1:  # Found valid
            row["EMAIL STATUS"] = "Valid"
            row["VALIDATED EMAIL"] = pattern
            update_dynamic_email_format_db(dynamic_email_format_db, company_domain, pattern.split("@")[0])
            return row

        if result and result.get("resultcode") == 2:  # Found catch-all
            row["EMAIL STATUS"] = "Catch-All"
            row["VALIDATED EMAIL"] = pattern

    # Fallback: Use the most common format from email_formats if no valid emails found
    if company_domain in email_formats:
        most_common_format = email_formats[company_domain]
        try:
            # Updated keys to match JSON placeholders
            guessed_email = most_common_format.format(
                first=first_name,
                last=last_name,
                domain=company_domain
            )
            row["EMAIL STATUS"] = "Unverified"
            row["VALIDATED EMAIL"] = guessed_email
            update_dynamic_email_format_db(dynamic_email_format_db, company_domain, guessed_email.split("@")[0])
        except KeyError as e:
            if debug:
                print(f"Error generating email for {company_domain} using format: {most_common_format}. Error: {e}")
    return row

# Main validation function
def validate_leads(input_file, output_file, debug=True):
    """
    Main validation function for leads.
    """
    print(f"Starting email validation from {input_file}...")

    # Load processed leads
    leads = pd.read_excel(input_file)
    leads["EMAIL STATUS"] = "Invalid"
    leads["VALIDATED EMAIL"] = ""

    # Ensure data folder exists
    os.makedirs(data_folder, exist_ok=True)

    # Load dynamic email format database
    dynamic_email_format_db = load_dynamic_email_format_db(dynamic_email_format_db_path)
    print("Dynamic email format database loaded.")

    # Load static email formats
    email_formats = load_email_formats(os.path.join(data_folder, "email_formats.json"))

    # Parallel processing
    rows = leads.to_dict("records")  # Convert DataFrame to list of dictionaries
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(
            validate_email_parallel, rows, [dynamic_email_format_db] * len(rows), [email_formats] * len(rows), [debug] * len(rows)
        ))

    # Save results back to DataFrame
    validated_leads = pd.DataFrame(results)

    # Save validated leads to output file
    validated_leads.to_excel(output_file, index=False)
    print(f"\nValidated leads saved to {output_file}")

    # Save the updated dynamic email format database
    save_dynamic_email_format_db(dynamic_email_format_db, dynamic_email_format_db_path)
    print("Dynamic email format database updated.")

# Example usage
if __name__ == "__main__":
    validate_leads("../output/processed_test_leads.xlsx", "../output/validated_test_leads.xlsx")
