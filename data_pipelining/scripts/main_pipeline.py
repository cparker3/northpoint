import subprocess
import os
import sys

print("DEBUG: Using the updated main_pipeline.py in", __file__)

def run_pipeline(input_file, final_output_file):
    """
    Runs the 3-step pipeline on 'input_file' and produces a final XLSX at 'final_output_file'.
    """
    # 'script_dir' is the absolute path to the directory containing THIS main_pipeline.py
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Our dedicated output folder is ../output relative to this file
    output_dir = os.path.join(script_dir, "..", "output")
    os.makedirs(output_dir, exist_ok=True)

    base_name = os.path.splitext(os.path.basename(input_file))[0]

    # Step 1 output => processed_<base_name>.xlsx
    processed_file = os.path.join(output_dir, f"processed_{base_name}.xlsx")
    # Step 2 output => pro_<base_name>.xlsx (all) & validated_<base_name>.xlsx (good only)
    validated_all = os.path.join(output_dir, f"pro_{base_name}.xlsx")
    validated_good = os.path.join(output_dir, f"validated_{base_name}.xlsx")

    # -----------------------------------------------------------------
    # Step 1: Processing Leads
    # -----------------------------------------------------------------
    print("Step 1: Processing Leads...")
    process_leads_script = os.path.join(script_dir, "process_leads.py")
    subprocess.run([
        "python3",
        process_leads_script,
        "--input_file", input_file
    ], check=True)

    # -----------------------------------------------------------------
    # Step 2: Validate Emails
    # -----------------------------------------------------------------
    print("Step 2: Validating Emails...")
    validate_emails_script = os.path.join(script_dir, "validate_emails.py")
    subprocess.run([
        "python3",
        validate_emails_script,
        "--input_file", processed_file,
        "--output_file_all", validated_all,
        "--output_file_valid", validated_good
    ], check=True)

    # -----------------------------------------------------------------
    # Step 3: Generate GPT Blurbs
    # -----------------------------------------------------------------
    print("Step 3: Generating GPT Blurbs...")
    personalize_blurbs_script = os.path.join(script_dir, "personalize_blurbs.py")
    subprocess.run([
        "python3",
        personalize_blurbs_script,
        "--input_file", validated_good,
        "--output_file", final_output_file
    ], check=True)

    print("Pipeline completed successfully!")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python main_pipeline.py <path_to_input_xlsx> <path_to_final_output_xlsx>")
        sys.exit(1)

    in_file = sys.argv[1]
    out_file = sys.argv[2]
    run_pipeline(in_file, out_file)
