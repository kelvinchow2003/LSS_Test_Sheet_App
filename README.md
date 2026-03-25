# Test Sheet Automator

A streamlined web application built with **Flask** and **React** (styled templates) to automate the generation of Lifesaving Society test sheets. By uploading a single CSV roster, this tool automatically populates complex PDF forms—handling candidate data, host details, and date formatting—saving hours of manual data entry.

## 🚀 Features

* **Multi-Form Support:** Automates generation for:
    * Emergency First Aid (2014)
    * Standard First Aid (2023)
    * Bronze Star (2020)
    * Bronze Medallion (2020)
    * Bronze Cross (2020)
    * Airway Management (2022)
    * Leadership Mastersheet (2025) - *Includes automatic continuation sheets.*
* **Batch Processing:** Automatically splits large rosters into multiple PDF files (e.g., batches of 10 or 13 candidates).
* **Smart Mapping:** Handles complex field hierarchies (e.g., `Name1` vs `Name.0`) and "shotgun" address filling for tricky PDF structures.
* **Privacy Focused:** Runs entirely in-memory. No personal data is stored on disk or in a database after processing.
* **Instant Reset:** The interface automatically resets after download for rapid processing of multiple classes.

## 🛠️ Tech Stack

* **Backend:** Python (Flask)
* **PDF Engine:** `pypdf` (for reading/writing fillable PDFs)
* **Frontend:** HTML5, Bootstrap 5, Custom CSS (Glassmorphism UI)
* **Deployment:** Ready for local hosting or cloud deployment (e.g., Render/Heroku).

## 📂 Project Structure

```text
/project-root
│
├── app.py                 # Main Flask server entry point
├── form_logic.py          # Core logic for processing specific PDF types
├── requirements.txt       # Python dependencies
│
├── templates/
│   └── index.html         # The frontend user interface
│
├── templates_pdf/         # BLANK fillable PDFs (Must be placed here)
│   ├── 95efa_on2014.pdf
│   ├── 95on_sfa_test_sheet-20231121-fillable.pdf
│   ├── 95tsbronzestar2020_fillable.pdf
│   ├── 95tsbronzemedallion2020_fillable.pdf
│   ├── 95tsbronzecross2020_fillable.pdf
│   ├── 95airwaymanagement2022-fillable.pdf
│   └── leadershipmastersheet_on_20250219_fillable.pdf
│
└── outputs/               # Temporary folder for ZIP generation (auto-cleared) 

```
## 📝 Usage Guide

### 1. Prepare your CSV
Your roster file **must** include the following headers (order does not matter, and extra columns are ignored):

| Header | Description |
| :--- | :--- |
| `AttendeeName` | Full Name (Last, First format is auto-converted) |
| `Street` | Candidate's street address |
| `City` | City of residence |
| `PostalCode` | Postal Code (e.g., L3P 3M2) |
| `E-mail` | Contact email |
| `AttendeePhone` | Primary Phone Number |
| `DateOfBirth` | Format: DD/MM/YYYY or YYYY-MM-DD |

### 2. Generate Forms
1.  Open the web interface.
2.  Click **Upload File** and select your CSV.
3.  Select the **Course Type** from the dropdown menu.
4.  Click **Generate PDFs**.
5.  A `.zip` file containing all filled batches will download automatically.

## 🛡️ Privacy & Security

This application is designed with **Privacy by Design** principles:
* **Ephemeral Processing:** Data is processed in temporary memory and immediately discarded.
* **No Database:** No candidate names, addresses, or DOBs are ever saved to a persistent database.
* **Auto-Cleanup:** The `outputs/` folder is overwritten on every new request.

### Developed by Kelvin Chow
