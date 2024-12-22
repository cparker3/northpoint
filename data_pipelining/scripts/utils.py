import json
import os

# Define the paths for the data folder
data_folder = os.path.join(os.path.dirname(__file__), "../data/")
bad_emails_file = os.path.join(data_folder, "bad_emails.json")
email_formats_file = os.path.join(data_folder, "email_formats.json")

def load_email_formats(filepath=email_formats_file):
    """
    Load static email formats from a JSON file.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Email formats file not found: {filepath}")
    with open(filepath, "r") as f:
        return json.load(f)

def load_bad_emails(filepath=bad_emails_file):
    """
    Load the list of bad emails from a JSON file.
    """
    if not os.path.exists(filepath):
        print(f"Warning: Bad emails file not found. Returning an empty dictionary.")
        return {}
    with open(filepath, "r") as f:
        return json.load(f)

def save_bad_emails(bad_emails, filepath=bad_emails_file):
    """
    Save the list of bad emails to a JSON file.
    """
    with open(filepath, "w") as f:
        json.dump(bad_emails, f, indent=4)
