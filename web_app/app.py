from flask import Flask, request, jsonify, send_file, render_template
import os
import uuid
import sys

# 1) Add scripts dir to Python path
SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data_pipelining/scripts"))
sys.path.append(SCRIPT_DIR)

from main_pipeline import run_pipeline

app = Flask(__name__)

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
PROCESSED_FOLDER = os.path.join(BASE_DIR, "processed")

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER

logs = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    if not file.filename.endswith('.xlsx'):
        return jsonify({"error": "Invalid file format"}), 400

    job_id = str(uuid.uuid4())
    upload_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{job_id}.xlsx")
    file.save(upload_path)
    logs[job_id] = ["File uploaded successfully"]

    return jsonify({"job_id": job_id})

@app.route('/process', methods=['POST'])
def process_job():
    data = request.get_json()
    job_id = data['job_id']

    upload_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{job_id}.xlsx")
    final_output = os.path.join(app.config['PROCESSED_FOLDER'], f"{job_id}_processed.xlsx")

    logs[job_id].append("Starting pipeline...")
    try:
        run_pipeline(upload_path, final_output)
        logs[job_id].append("Pipeline completed!")
    except Exception as e:
        logs[job_id].append(f"Error: {str(e)}")

    return jsonify({"status": "processing started"})

@app.route('/progress', methods=['GET'])
def progress():
    job_id = request.args.get('job_id')
    return jsonify({"logs": logs.get(job_id, [])})

@app.route('/download', methods=['GET'])
def download():
    job_id = request.args.get('job_id')
    output_path = os.path.join(app.config['PROCESSED_FOLDER'], f"{job_id}_processed.xlsx")
    if not os.path.exists(output_path):
        return jsonify({"error": "File not found or still processing"}), 404
    return send_file(
        output_path,
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)
    app.run(debug=True, host='0.0.0.0', port=5001)
