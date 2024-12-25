import os
import logging
import openai
import pandas as pd
import time
import argparse

from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Set the OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

def classify_contact(role, company):
    """
    Classifies a single contact into one of the following consulting categories:
    - MANAGEMENT
    - MARKETING
    - TECHNOLOGY
    - CSR
    """
    prompt = f"""
    Classify the contact into one of the following consulting categories based on their role and company:
    - Management Consulting (Label as MANAGEMENT)
    - Marketing Consulting (Label MARKETING)
    - Tech / Software Consulting (Label TECHNOLOGY)
    - DEI/CSR/ESG (Label CSR)

    The contact's role is: {role}
    The contact's company is: {company}

    Return only the label (MANAGEMENT, MARKETING, TECHNOLOGY, or CSR) without any explanation.
    """

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",  # Example model name; adjust as needed
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0
        )
        label = response["choices"][0]["message"]["content"].strip()
        return label
    except Exception as e:
        logging.error(f"Error classifying contact: {e}")
        return "CLASSIFICATION_ERROR"

def generate_blurb(role, company, first_name):
    """
    Generates a personalized blurb for a single contact.
    """
    prompt = f"""
    You are crafting a personalized blurb to include in a business email. The goal is to make the email highly personalized, professional, and engaging. It should seamlessly fit into the email, both before and after the blurb, and motivate the recipient to consider engaging with Varick Consulting. Here's the context:

    - The recipient's first name is {first_name}.
    - Their role is {role}.
    - The company they work for is {company}.

    Make sure what you return is ONLY the blurb. This will then be transferred over to fit seamlessly into the rest of the email. ONLY include the Blurb and NOTHING before or after it. 

    Email Context:
    
    Before the blurb:
    "Hello {first_name},

    I hope this email finds you well. I recently came across your work at {company} and wanted to reach out to discuss a potential collaboration opportunity to support your upcoming projects and initiatives.

    My name is Vasuman Moza, and I have led and executed several consulting projects for over a dozen Fortune-500 companies over the last 4 years, including Nike, DoorDash, Uber, and more. Our projects have included topics such as go-to-market strategy, competitor analysis, and industry/consumer research."

    After the blurb:
    "If you're open to exploring the possibility of working together, feel free to reach out to vasumanmoza@varickassociates.com. For a brief overview of our services and testimonials from previous clients, please see the deck attached.

    We understand that today's economic climate can make people hesitant about investing in new services. To address this concern and demonstrate our commitment to providing value, we'd like to offer you a complimentary, no-obligation consultation. During this initial call, we can discuss your current challenges and share insights on how we can help as a formal proposal.

    Thank you for your time, and we look forward to the opportunity to learn more about your work and how we might collaborate."

    Based on the recipient's role and company, craft a 2-3 sentence blurb that seamlessly fits into the context of this email, adds value, and encourages engagement. Avoid generic language and tailor it specifically to their industry and position. Ensure the tone remains professional and friendly.
    """
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",  # Example model name; adjust as needed
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        blurb = response["choices"][0]["message"]["content"].strip()
        return blurb
    except Exception as e:
        logging.error(f"Error generating blurb: {e}")
        return "Error generating blurb."

def process_row(row):
    """
    Processes a single row, generating a blurb and classification.
    """
    role = row.get("ROLE", "")
    company = row.get("COMPANY", "")
    first_name = row.get("FIRST", "")

    try:
        blurb = generate_blurb(role, company, first_name)
        classification = classify_contact(role, company)
        return blurb, classification
    except Exception as e:
        logging.error(f"Error processing row: ROLE={role}, COMPANY={company}, FIRST={first_name}, Error: {e}")
        return "Error generating blurb", "CLASSIFICATION_ERROR"

def generate_customs(input_file, output_file, debug=False):
    """
    Loads leads from an Excel file, generates personalized blurbs,
    classifies consulting groups, and saves the results to a new Excel file.
    """
    # Set up logging
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info("Starting the personalization process...")

    # Load leads from Excel
    try:
        leads = pd.read_excel(input_file)
        total_rows = len(leads)
        logging.info(f"Loaded {total_rows} leads from {input_file}")
    except Exception as e:
        logging.error(f"Failed to read Excel file {input_file}: {e}")
        return

    # Prepare columns for results
    leads["GPT_BLURB"] = ""
    leads["CONSULTING_GROUP"] = ""

    # Start processing with timing
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(process_row, [row for _, row in leads.iterrows()]))

    # Assign results back to the DataFrame
    for idx, (blurb, classification) in enumerate(results):
        leads.at[idx, "GPT_BLURB"] = blurb
        leads.at[idx, "CONSULTING_GROUP"] = classification

    # Log runtime
    end_time = time.time()
    runtime = end_time - start_time
    logging.info(f"Processed {len(leads)} rows in {runtime:.2f} seconds")

    # Save the result to Excel
    try:
        leads.to_excel(output_file, index=False)
        logging.info(f"Blurbed and classified leads saved to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save Excel file {output_file}: {e}")


# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate personalized blurbs for leads.")
    parser.add_argument(
        "--input_file",
        type=str,
        default="../output/validated_test_leads.xlsx",
        help="Path to validated leads Excel file."
    )
    parser.add_argument(
        "--output_file",
        type=str,
        default="../output/blurbed_and_classified_leads.xlsx",
        help="Path to save blurbed and classified leads."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode."
    )
    args = parser.parse_args()

    generate_customs(
        input_file=args.input_file,
        output_file=args.output_file,
        debug=args.debug
    )

