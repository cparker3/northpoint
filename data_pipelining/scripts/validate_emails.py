import smtplib
import dns.resolver
import pandas as pd

def validate_email(email):
    try:
        domain = email.split("@")[1]
        mx_records = dns.resolver.resolve(domain, 'MX')
        mx_host = str(mx_records[0].exchange)
        
        with smtplib.SMTP(mx_host, 25) as server:
            server.set_debuglevel(0)  # 0 = Silent, 1 = Verbose
            server.helo("example.com")  # Replace with your domain
            server.mail("test@example.com")  # Replace with your email
            code, message = server.rcpt(email)
            
            return code == 250  # Valid email if code is 250
    except Exception as e:
        print(f"Error validating {email}: {e}")
        return False

def validate_leads(input_file, output_file):
    # Load processed leads
    leads = pd.read_excel(input_file)

    # Validate each email
    leads["EMAIL STATUS"] = leads["EMAIL"].apply(
        lambda email: "Valid" if validate_email(email) else "Invalid"
    )

    # Save results
    leads.to_excel(output_file, index=False)
    print(f"Validated leads saved to {output_file}")

# Example usage
if __name__ == "__main__":
    validate_leads("output/processed_leads.xlsx", "output/validated_leads.xlsx")