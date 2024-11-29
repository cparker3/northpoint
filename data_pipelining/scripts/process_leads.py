import pandas as pd

def process_leads(input_file, output_file):
    # Load input leads
    leads = pd.read_excel(input_file)

    # Ensure column names are uppercase and trimmed
    leads.columns = leads.columns.str.strip().str.upper()

    # Remove duplicates and clean data
    leads.drop_duplicates(subset=["FIRST NAME", "LAST NAME", "COMPANY"], inplace=True)
    leads.fillna("", inplace=True)  # Fill empty cells with blank strings

    # Guess email addresses based on common patterns
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

    leads["EMAIL"] = leads.apply(guess_email, axis=1)

    # Save cleaned and guessed data
    leads.to_excel(output_file, index=False)
    print(f"Processed leads saved to {output_file}")

# Example usage
if __name__ == "__main__":
    process_leads("input/test_leads.xlsx", "output/processed_leads.xlsx")
