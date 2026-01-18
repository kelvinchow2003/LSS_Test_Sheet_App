from flask import Flask, render_template, request, send_file
import pandas as pd
import os
import zipfile
import shutil
from form_logic import process_efa, process_bronze_med, process_bronze_cross, process_bronze_star, process_sfa, process_airway_management, process_leadership_mastersheet

app = Flask(__name__)

# Configuration
UPLOAD_FOLDER = 'outputs'
TEMPLATE_FOLDER = 'templates_pdf'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(TEMPLATE_FOLDER, exist_ok=True)

# Map drop-down values to filenames and functions
FORM_CONFIG = {
    "efa": {
        "filename": "95efa_on2014.pdf",
        "func": process_efa
    },
    "bronze_med": {
        "filename": "95tsbronzemedallion2020_fillable.pdf",
        "func": process_bronze_med
    },
    "bronze_cross": {
        "filename": "95tsbronzecross2020_fillable.pdf",
        "func": process_bronze_cross
    },
    "bronze_star": {
        "filename": "95tsbronzestar2020_fillable.pdf",
        "func": process_bronze_star
    },
    "sfa": {
        "filename": "95on_sfa_test_sheet-20231121-fillable.pdf",
        "func": process_sfa
    },
    "airway_management": {
        "filename": "95airwaymanagement2022-fillable.pdf",
        "func": process_airway_management
    },
    "leadership_mastersheet": {
        "filename": "leadershipmastersheet_on_20250219_fillable.pdf",
        "func": process_leadership_mastersheet
    }
}

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # 1. Check for file
        if 'csv_file' not in request.files:
            return "No file uploaded", 400
        
        file = request.files['csv_file']
        form_type = request.form.get('form_type')
        
        if file.filename == '' or not form_type:
            return "Missing file or selection", 400

        # 2. Save CSV temporarily
        csv_path = os.path.join(UPLOAD_FOLDER, "temp_roster.csv")
        file.save(csv_path)

        # 3. Read CSV
        try:
            df = pd.read_csv(csv_path, dtype=str).fillna("")
        except Exception as e:
            return f"Error reading CSV: {str(e)}", 500

        # 4. Get Template Path
        config = FORM_CONFIG.get(form_type)
        template_path = os.path.join(TEMPLATE_FOLDER, config['filename'])
        
        if not os.path.exists(template_path):
            return f"Template PDF not found: {config['filename']}. Please put it in the templates_pdf folder.", 500

        # 5. Run the Processor Logic
        # Create a unique subfolder for this run to avoid conflicts
        run_folder = os.path.join(UPLOAD_FOLDER, "generated_files")
        if os.path.exists(run_folder): shutil.rmtree(run_folder)
        os.makedirs(run_folder)

        try:
            generated_pdfs = config['func'](df, template_path, run_folder)
        except Exception as e:
            return f"Error processing PDF: {str(e)}", 500

        # 6. Zip the results
        zip_path = os.path.join(UPLOAD_FOLDER, "Filled_Forms.zip")
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for pdf in generated_pdfs:
                zipf.write(pdf, os.path.basename(pdf))

        # 7. Cleanup and Send
        return send_file(zip_path, as_attachment=True)

    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)