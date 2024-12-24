import os
import json
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Set the OpenAI API key
mv_api_key = os.getenv("MILLION_VERIFIER_API_KEY")

# -----------------------------------------------------
# File paths
# -----------------------------------------------------
script_dir = os.path.dirname(os.path.abspath(__file__))
data_folder = os.path.join(script_dir, "../data/")
bad_emails_path = os.path.join(data_folder, "bad_emails.json")
email_formats_path = os.path.join(data_folder, "email_formats.json")
dynamic_db_path = os.path.join(data_folder, "dynamic_email_format_db.json")

# -----------------------------------------------------
# Load / Save Helpers
# -----------------------------------------------------
def load_bad_emails(debug=False):
    if os.path.exists(bad_emails_path):
        with open(bad_emails_path, "r") as f:
            bads = set(json.load(f))
            if debug:
                print(f"[DEBUG] Loaded {len(bads)} bad emails from {bad_emails_path}")
            return bads
    if debug:
        print(f"[DEBUG] No {bad_emails_path} found, returning empty set of bad emails.")
    return set()

def load_email_formats(debug=False):
    if os.path.exists(email_formats_path):
        with open(email_formats_path, "r") as f:
            data = json.load(f)
            if debug:
                print(f"[DEBUG] Loaded email formats for {len(data)} domains from {email_formats_path}")
            return data
    if debug:
        print(f"[DEBUG] No {email_formats_path} found, returning empty dict.")
    return {}

def load_dynamic_db(debug=False):
    if os.path.exists(dynamic_db_path):
        with open(dynamic_db_path, "r") as f:
            content = f.read().strip()
            if not content:
                # File is empty
                if debug:
                    print(f"[DEBUG] {dynamic_db_path} is empty, returning empty dynamic DB.")
                return {}
            try:
                db = json.loads(content)
                if debug:
                    print(f"[DEBUG] Loaded dynamic DB with {len(db.keys())} domains from {dynamic_db_path}")
                return db
            except json.JSONDecodeError:
                if debug:
                    print(f"[DEBUG] {dynamic_db_path} contains invalid JSON, returning empty dynamic DB.")
                return {}
    if debug:
        print(f"[DEBUG] No {dynamic_db_path} found, returning empty dynamic DB.")
    return {}

def save_dynamic_db(db, debug=False):
    os.makedirs(data_folder, exist_ok=True)
    with open(dynamic_db_path, "w") as f:
        json.dump(db, f, indent=4)
    if debug:
        print(f"[DEBUG] Saved dynamic DB with {len(db.keys())} domains to {dynamic_db_path}")

# -----------------------------------------------------
# MillionVerifier
# -----------------------------------------------------
def verify_email_millionverifier(email, api_key=mv_api_key, retries=3, debug=False):
    for attempt in range(retries):
        try:
            if debug:
                print(f"[DEBUG] Attempt {attempt+1} to verify '{email}' via MillionVerifier...")

            response = requests.get(
                "https://api.millionverifier.com/api/v3/",
                params={"api": api_key, "email": email},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            if debug:
                print(f"[DEBUG] MillionVerifier response for '{email}': {data}")

            return data
        except Exception as e:
            if debug:
                print(f"[DEBUG] Error verifying '{email}': {e}")
            if attempt == retries - 1:
                if debug:
                    print(f"[DEBUG] All {retries} attempts failed for '{email}'")
                return None

# -----------------------------------------------------
# Known fallback patterns
# -----------------------------------------------------
FALLBACK_PATTERNS = {
    "first":            "{first}",
    "first.last":       "{first}.{last}",
    "first_{last}":     "{first}_{last}",
    "firstInitial.last":"{firstInitial}.{last}"
}

def apply_pattern(pattern_key, first, last, debug=False):
    """
    Convert a pattern key into the actual email prefix.
    - If it's one of our known fallback patterns, apply it directly.
    - If it's "customPattern:xxxx", treat that as a custom .format(...) template.
    """
    firstInitial = first[:1] if first else ""
    if pattern_key in FALLBACK_PATTERNS:
        template = FALLBACK_PATTERNS[pattern_key]
        prefix = template.format(first=first, last=last, firstInitial=firstInitial)
        if debug:
            print(f"[DEBUG] Applying fallback pattern '{pattern_key}' -> '{prefix}'")
        return prefix

    if pattern_key.startswith("customPattern:"):
        custom_template = pattern_key[len("customPattern:"):]
        prefix = (custom_template
                  .replace("{first[0]}", firstInitial)
                  .replace("{first_0}", firstInitial)
                  .replace("{first}", first)
                  .replace("{last}", last))
        if debug:
            print(f"[DEBUG] Applying custom pattern '{pattern_key}' -> '{prefix}'")
        return prefix

    return ""

def record_email_usage(db, domain, pattern_key, debug=False):
    """
    Increments usage for a pattern key ONLY for Valid addresses.
    """
    if domain not in db:
        db[domain] = {}
    old_val = db[domain].get(pattern_key, 0)
    db[domain][pattern_key] = old_val + 1
    if debug:
        print(f"[DEBUG] Updated usage for domain '{domain}', pattern '{pattern_key}' from {old_val} -> {old_val+1}")

def sorted_patterns_by_usage(db, domain, debug=False):
    """
    Return pattern keys for a domain, sorted by usage desc
    """
    if domain not in db:
        if debug:
            print(f"[DEBUG] No dynamic patterns for domain '{domain}' yet.")
        return []
    usage_map = db[domain]
    patterns = sorted(usage_map.keys(), key=lambda p: usage_map[p], reverse=True)
    # We exclude '_catchall_domains' if it exists in the same top-level dict
    return [p for p in patterns if not p.startswith("_")]

    # If you stored patterns in sub-dicts, you'd adjust accordingly.
    # This is fine for now.

# -----------------------------------------------------
# Parsing the static pattern from email_formats.json
# -----------------------------------------------------
def parse_static_pattern(static_pattern_str, debug=False):
    """
    Example: given "{first[0]}{last}@airbnb.com",
    return "customPattern:{first[0]}{last}" (omitting '@airbnb.com'),
    unless it matches a known fallback exactly.
    """
    parts = static_pattern_str.split("@")
    if len(parts) < 2:
        if debug:
            print(f"[DEBUG] parse_static_pattern: '{static_pattern_str}' is not a valid pattern!")
        return None
    prefix_part = parts[0]

    # Check if it matches one of our fallback patterns exactly
    for key, tpl in FALLBACK_PATTERNS.items():
        if prefix_part == tpl:
            if debug:
                print(f"[DEBUG] parse_static_pattern: '{prefix_part}' matched known fallback '{key}'")
            return key

    # Otherwise, it's a custom pattern
    if debug:
        print(f"[DEBUG] parse_static_pattern: '{prefix_part}' -> 'customPattern:{prefix_part}'")
    return f"customPattern:{prefix_part}"

# -----------------------------------------------------
# Check if domain is known catch-all
# -----------------------------------------------------
def is_domain_catchall(db, domain_key):
    """
    We store catch-all info in e.g. db["_catchall_domains"] = {domain_key: True, ...}
    """
    catchall_info = db.get("_catchall_domains", {})
    return catchall_info.get(domain_key, False)

def mark_domain_catchall(db, domain_key, debug=False):
    """
    Mark domain as catch-all so we can skip future MV calls.
    """
    if "_catchall_domains" not in db:
        db["_catchall_domains"] = {}
    db["_catchall_domains"][domain_key] = True
    if debug:
        print(f"[DEBUG] Marked domain '{domain_key}' as catch-all in dynamic DB.")

# -----------------------------------------------------
# Validation for a single lead
# -----------------------------------------------------
def validate_one_lead(row, bad_emails, email_formats, dynamic_db, debug=False):
    """
    Validate a single lead row with domain-level catch-all optimization:
      - If domain is known catch-all, skip MV calls, just label as Catch-All.
      - Otherwise, proceed with pattern tries.
      - If we find a new catch-all response from MV, mark domain as catch-all.
      - Only record usage for code==1 (Valid).
    """
    row["EMAIL STATUS"] = "Invalid"
    row["VALIDATED EMAIL"] = ""

    first = row["FIRST NAME"].strip().lower()
    last = row["LAST NAME"].strip().lower()
    company_key = row["COMPANY"].strip().lower().replace(" ", "")
    domain_key = f"{company_key}.com"

    if debug:
        print(f"\n[DEBUG] Validating lead for: '{row['FIRST NAME']} {row['LAST NAME']}' (Domain: '{domain_key}')")

    # 1) If domain is already known catch-all, skip verifying
    if is_domain_catchall(dynamic_db, domain_key):
        if debug:
            print(f"[DEBUG] Domain '{domain_key}' is known catch-all, skipping further MV calls.")
        # Optionally generate some email if you want a placeholder, e.g. from email_formats or fallback
        # Let's do the static pattern if available, else fallback
        static_pattern_str = email_formats.get(domain_key, "")
        prefix = ""
        if static_pattern_str:
            parsed_key = parse_static_pattern(static_pattern_str, debug=debug)
            if parsed_key:
                prefix = apply_pattern(parsed_key, first, last, debug=debug)
        else:
            # fallback to 'first.last'
            prefix = f"{first}.{last}"

        # label row as catch-all
        row["EMAIL STATUS"] = "Catch-All"
        row["VALIDATED EMAIL"] = prefix + "@" + domain_key if prefix else ""
        return row

    # 2) Build a list of pattern keys to try:
    pattern_keys_to_try = []

    # a) The pattern from email_formats.json (if present)
    static_pattern_str = email_formats.get(domain_key, "")
    if static_pattern_str:
        parsed_key = parse_static_pattern(static_pattern_str, debug=debug)
        if parsed_key:
            pattern_keys_to_try.append(parsed_key)

    # b) Patterns from the dynamic DB, sorted by usage desc
    dynamic_patterns = sorted_patterns_by_usage(dynamic_db, domain_key, debug=debug)
    pattern_keys_to_try.extend(dynamic_patterns)

    # c) Fallback patterns in a fixed order
    fallback_order = ["first", "first.last", "first_{last}", "firstInitial.last"]
    for fb in fallback_order:
        if fb not in pattern_keys_to_try:
            pattern_keys_to_try.append(fb)

    # Remove duplicates while preserving order
    seen = {}
    pattern_keys_cleaned = []
    for pk in pattern_keys_to_try:
        if pk not in seen:
            pattern_keys_cleaned.append(pk)
            seen[pk] = True
    pattern_keys_to_try = pattern_keys_cleaned

    if debug:
        print(f"[DEBUG] Final pattern list for '{domain_key}': {pattern_keys_to_try}")

    # 3) Try each pattern key in order
    for pk in pattern_keys_to_try:
        prefix = apply_pattern(pk, first, last, debug=debug)
        if not prefix:
            continue

        email_address = prefix + "@" + domain_key
        if debug:
            print(f"[DEBUG] Testing email '{email_address}'")

        # Skip if known-bad
        if email_address in bad_emails:
            if debug:
                print(f"[DEBUG] '{email_address}' is in bad_emails. Skipping...")
            continue

        # Verify with MillionVerifier
        mv_result = verify_email_millionverifier(email_address, debug=debug)
        if not mv_result:
            if debug:
                print(f"[DEBUG] No MV result or final attempt failed for '{email_address}'.")
            continue

        code = mv_result.get("resultcode")
        if code == 1:
            # Confirmed Valid
            row["EMAIL STATUS"] = "Valid"
            row["VALIDATED EMAIL"] = email_address
            # record usage only for valid
            record_email_usage(dynamic_db, domain_key, pk, debug=debug)
            if debug:
                print(f"[DEBUG] '{email_address}' is Valid. Stopping search.")
            return row

        elif code == 2:
            # Found a Catch-All
            row["EMAIL STATUS"] = "Catch-All"
            row["VALIDATED EMAIL"] = email_address

            # Mark domain so future leads skip verifying
            mark_domain_catchall(dynamic_db, domain_key, debug=debug)

            if debug:
                print(f"[DEBUG] '{email_address}' is Catch-All. Stopping search.")
            return row

        else:
            if debug:
                print(f"[DEBUG] '{email_address}' returned code '{code}', continuing...")

    # 4) If all fail, remain invalid
    if debug:
        print(f"[DEBUG] No valid patterns found for '{first} {last}' at '{domain_key}'. Marking invalid.")
    return row

# -----------------------------------------------------
# Master Function
# -----------------------------------------------------
def validate_leads(input_file, output_file_all, output_file_valid, debug=False):
    """
    Validate leads from 'input_file', save:
      1) 'output_file_all': includes ALL leads (valid, catch-all, invalid),
         with the 'EMAIL' column removed if it existed.
      2) 'output_file_valid': includes ONLY valid or catch-all rows,
         sorted by 'COMPANY'.

    :param input_file: path to the input Excel of leads
    :param output_file_all: path to save the full validated output
    :param output_file_valid: path to save only the valid/catch-all rows
    :param debug: True or False for debug printing
    """
    if debug:
        print(f"[DEBUG] Beginning validation process.")
        print(f"[DEBUG] Reading leads from '{input_file}'")

    leads = pd.read_excel(input_file)

    # Load data
    bad_emails = load_bad_emails(debug=debug)
    email_formats_data = load_email_formats(debug=debug)
    dynamic_db = load_dynamic_db(debug=debug)

    # Initialize columns
    leads["EMAIL STATUS"] = "Invalid"
    leads["VALIDATED EMAIL"] = ""

    rows = leads.to_dict("records")

    if debug:
        print(f"[DEBUG] Beginning parallel validation with up to 10 workers...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(
            executor.map(
                lambda r: validate_one_lead(r, bad_emails, email_formats_data, dynamic_db, debug=debug),
                rows
            )
        )

    updated_leads = pd.DataFrame(results)

    # --- 1) Drop the original "EMAIL" column if it exists ---
    if "EMAIL" in updated_leads.columns:
        updated_leads.drop(columns=["EMAIL"], inplace=True)

    # --- 2) Save the FULL file (all leads: valid, catch-all, invalid) ---
    updated_leads.to_excel(output_file_all, index=False)
    if debug:
        print(f"[DEBUG] Wrote full results (including invalid) to '{output_file_all}'")

    # --- 3) Create a second DataFrame with only Valid or Catch-All ---
    valid_df = updated_leads[updated_leads["EMAIL STATUS"].isin(["Valid", "Catch-All"])]

    # Sort by 'COMPANY' in ascending order
    valid_df = valid_df.sort_values(by="COMPANY", ascending=True)

    # Save this second file
    valid_df.to_excel(output_file_valid, index=False)
    if debug:
        print(f"[DEBUG] Wrote valid/catch-all results to '{output_file_valid}'")

    # Save updated dynamic DB
    save_dynamic_db(dynamic_db, debug=debug)
    if debug:
        print("[DEBUG] Validation process complete.")

# -----------------------------------------------------
# Main function
# -----------------------------------------------------
if __name__ == "__main__":
    validate_leads(
        input_file="../output/processed_test_leads.xlsx",
        output_file_all="../output/pro_processed_test_leads.xlsx",
        output_file_valid="../output/validated_test_leads.xlsx",
        debug=False
    )
