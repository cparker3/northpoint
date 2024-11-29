import openai
import pandas as pd
import os

def generate_blurb(row):
    openai.api_key = os.getenv("OPENAI_API_KEY")  # Load API key from environment variable
    role = row["ROLE"]
    company = row["COMPANY"]
    prompt = f"""
    You are creating a personalized blurb for a business email. Here's the context:
    
    - The recipient's role is {role}.
    - The company is {company}.
    - The email should maintain a professional but friendly tone.

    Generate a 2-3 sentence blurb that smoothly fits into the middle of an email and highlights how we can collaborate with them based on their role and company.
    """
    try:
        response = openai.Completion.create(
            engine="text-davinci-003",
            prompt=prompt,
            max_tokens=100,
            temperature=0.7,
        )
        return response.choices[0].text.strip()
    except Exception as e:
        print(f"Error generating blurb for {row['EMAIL']}: {e}")
        return "Error generating blurb"

def personalize_blurbs(input_file, output_file):
    # Load validated leads
    leads = pd.read_excel(input_file)

    # Filter valid leads
    valid_leads = leads[leads["EMAIL STATUS"] == "Valid"]

    # Generate GPT blurbs
    valid_leads["GPT BLURB"] = valid_leads.apply(generate_blurb, axis=1)

    # Save results
    valid_leads.to_excel(output_file, index=False)
    print(f"Blurbed leads saved to {output_file}")

# Example usage
if __name__ == "__main__":
    personalize_blurbs("output/validated_leads.xlsx", "output/blurbed_leads.xlsx")
