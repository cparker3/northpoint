import subprocess

def main():
    print("Step 1: Processing Leads...")
    subprocess.run(["python3", "process_leads.py"])

    print("Step 2: Validating Emails...")
    subprocess.run(["python3", "validate_emails.py"])

    # print("Step 3: Generating GPT Blurbs...")
    # subprocess.run(["python3", "personalize_blurbs.py"])

    print("Pipeline completed successfully!")

if __name__ == "__main__":
    main()
