import pandas as pd
from pypdf import PdfReader, PdfWriter
import math
import os

# --- UTILS ---
def clean_name(raw_name):
    if pd.isna(raw_name): return ""
    raw_name = str(raw_name)
    if "," in raw_name:
        parts = raw_name.split(",")
        if len(parts) >= 2:
            return f"{parts[1].strip()} {parts[0].strip()}"
    return raw_name

def parse_date(raw_dob, use_full_year=True):
    dd, mm, yy = "", "", ""
    if pd.notna(raw_dob):
        try:
            dt = pd.to_datetime(raw_dob, dayfirst=True)
            dd = str(dt.day).zfill(2)
            mm = str(dt.month).zfill(2)
            yy = str(dt.year) if use_full_year else str(dt.year)[-2:]
        except: pass
    return dd, mm, yy

# --- EMERGENCY FIRST AID LOGIC ---
def process_efa(df, template_path, output_folder):
    # Mapping for EFA (1-10)
    candidate_map = []
    for i in range(1, 11):
        suffix = str(i)
        entry = {
            "name": f"Name {suffix}", "addr": f"Address {suffix}", "city": f"City {suffix}",
            "zip": f"Postal {suffix}", "email": f"Email {suffix}", "phone": f"Phone {suffix}",
            "dd": f"Day {suffix}", "mm": f"Month {suffix}", "yy": f"Year {suffix}"
        }
        if i == 10: entry["name"] = "10"
        candidate_map.append(entry)

    BATCH_SIZE = 10
    total_batches = math.ceil(len(df) / BATCH_SIZE)
    generated_files = []

    for b in range(total_batches):
        batch_df = df.iloc[b * BATCH_SIZE : (b + 1) * BATCH_SIZE]
        reader = PdfReader(template_path)
        writer = PdfWriter()
        writer.append(reader)
        data_map = {}

        for i, (idx, row) in enumerate(batch_df.iterrows()):
            if i >= len(candidate_map): break
            fields = candidate_map[i]
            full_name = clean_name(row.get("AttendeeName", ""))
            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=True)

            data_map[fields["name"]] = full_name
            data_map[fields["addr"]] = str(row.get("Street", ""))
            data_map[fields["city"]] = str(row.get("City", ""))
            data_map[fields["zip"]] = str(row.get("PostalCode", ""))
            data_map[fields["email"]] = str(row.get("E-mail", ""))
            data_map[fields["phone"]] = str(row.get("AttendeePhone", ""))
            data_map[fields["dd"]] = dd; data_map[fields["mm"]] = mm; data_map[fields["yy"]] = yy

        for page in writer.pages:
            writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"EFA_Batch_{b+1}.pdf")
        with open(out_name, "wb") as f: writer.write(f)
        generated_files.append(out_name)
    
    return generated_files

# --- BRONZE MEDALLION LOGIC ---
def process_bronze_med(df, template_path, output_folder):
    candidate_map = [
        {"base": "1", "s": ".0"}, {"base": "1", "s": ".1.0"}, {"base": "1", "s": ".1.1.0"},
        {"base": "1", "s": ".1.1.1.0"}, {"base": "1", "s": ".1.1.1.1.0"}, {"base": "1", "s": ".1.1.1.1.1"},
        {"base": "", "s": ".0.0"}, {"base": "", "s": ".0.1.0"}, {"base": "", "s": ".0.1.1.0"},
        {"base": "", "s": ".0.1.1.1.0"}, {"base": "", "s": ".0.1.1.1.1.0"}, {"base": "", "s": ".0.1.1.1.1.1.0"},
        {"base": "", "s": ".0.1.1.1.1.1.1"},
    ]
    
    BATCH_SIZE = 13
    total_batches = math.ceil(len(df) / BATCH_SIZE)
    generated_files = []

    for b in range(total_batches):
        batch_df = df.iloc[b * BATCH_SIZE : (b + 1) * BATCH_SIZE]
        reader = PdfReader(template_path)
        writer = PdfWriter()
        writer.append(reader)
        data_map = {}

        for i, (idx, row) in enumerate(batch_df.iterrows()):
            if i >= len(candidate_map): break
            slot = candidate_map[i]
            base, s = slot["base"], slot["s"]
            
            full_name = clean_name(row.get("AttendeeName", ""))
            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=False) # 2 digit year

            data_map[f"Name{base}{s}"] = full_name
            data_map[f"Address{base}{s}"] = str(row.get("Street", ""))
            data_map[f"City{base}{s}"] = str(row.get("City", ""))
            data_map[f"Postal{base}{s}"] = str(row.get("PostalCode", ""))
            data_map[f"Email{base}{s}"] = str(row.get("E-mail", ""))
            data_map[f"Phone{base}{s}"] = str(row.get("AttendeePhone", ""))
            data_map[f"DOBD{base}{s}"] = dd; data_map[f"DOBM{base}{s}"] = mm; data_map[f"DOBY{base}{s}"] = yy

        for page in writer.pages: writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"BronzeMed_Batch_{b+1}.pdf")
        with open(out_name, "wb") as f: writer.write(f)
        generated_files.append(out_name)
    return generated_files

# --- BRONZE CROSS LOGIC ---
def process_bronze_cross(df, template_path, output_folder):
    candidate_map = [
        {"p": "", "s": ".0"}, {"p": "", "s": ".1.0"}, {"p": "", "s": ".1.1.0"},
        {"p": "", "s": ".1.1.1.0"}, {"p": "", "s": ".1.1.1.1.0"}, {"p": "", "s": ".1.1.1.1.1"},
        {"p": "7", "s": ".0"}, {"p": "8", "s": ".1.0"},
        {"p": "9", "s": ".1.1.0", "addr_override": ["Address1.1.1.0X", "Address1.1.1", "Address1.1.1.0", "Address1.1.0", "Address1.0", "Address1", "Address1.1", "Text2", "Text16", "Text17"]},
        {"p": "10", "s": ".1.1.1.0", "name_override": "10"},
        {"p": "11", "s": ".1.1.1.1.0"}, {"p": "12", "s": ".1.1.1.1.1"}, {"p": "13", "s": ".1.1.1.1.1"}
    ]

    BATCH_SIZE = 13
    total_batches = math.ceil(len(df) / BATCH_SIZE)
    generated_files = []

    for b in range(total_batches):
        batch_df = df.iloc[b * BATCH_SIZE : (b + 1) * BATCH_SIZE]
        reader = PdfReader(template_path)
        writer = PdfWriter()
        writer.append(reader)
        data_map = {}

        for i, (idx, row) in enumerate(batch_df.iterrows()):
            if i >= len(candidate_map): break
            slot = candidate_map[i]
            p, s = slot.get("p", ""), slot.get("s", "")
            prefix = p if p else ""

            f_name = slot.get("name_override", f"{prefix}Name1{s}")
            full_name = clean_name(row.get("AttendeeName", ""))
            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=False)

            data_map[f_name] = full_name
            data_map[f"{prefix}City1{s}"] = str(row.get("City", ""))
            data_map[f"{prefix}Postal1{s}"] = str(row.get("PostalCode", ""))
            data_map[f"{prefix}Email1{s}"] = str(row.get("E-mail", ""))
            data_map[f"{prefix}Phone1{s}"] = str(row.get("AttendeePhone", ""))
            data_map[f"{prefix}DOBD1{s}"] = dd; data_map[f"{prefix}DOBM1{s}"] = mm; data_map[f"{prefix}DOBY1{s}"] = yy

            # Handle Address Override (The Shotgun Approach)
            addr_val = str(row.get("Street", ""))
            if "addr_override" in slot:
                overrides = slot["addr_override"]
                if isinstance(overrides, list):
                    for f in overrides: data_map[f] = addr_val
                else:
                    data_map[overrides] = addr_val
            else:
                data_map[f"{prefix}Address1{s}"] = addr_val

        for page in writer.pages: writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"BronzeCross_Batch_{b+1}.pdf")
        with open(out_name, "wb") as f: writer.write(f)
        generated_files.append(out_name)
    return generated_files

# ... (existing imports and functions) ...

# --- BRONZE STAR LOGIC (Based on your provided script) ---

def process_bronze_star(df, template_path, output_folder):

    # The exact mapping from your script

    candidate_map = [

        # === PAGE 1 (Candidates 1-6) ===

        {"type": "explicit", "s": "1"}, 

        {"type": "explicit", "s": "2"}, 

        {"type": "explicit", "s": "3"}, 

        {"type": "explicit", "s": "4"}, 

        {"type": "explicit", "s": "5"}, 

        {"type": "explicit", "s": "6"}, 

        # === PAGE 2 (Candidates 7-13) ===

        {"type": "dot", "s": ".0"},           

        {"type": "dot", "s": ".1.0"},         

        {"type": "dot", "s": ".1.1.0"},       

        {"type": "dot", "s": ".1.1.1.0"},     

        {"type": "dot", "s": ".1.1.1.1.0"},   

        {"type": "dot", "s": ".1.1.1.1.1.0"}, 

        {"type": "dot", "s": ".1.1.1.1.1.1"},

    ]

    BATCH_SIZE = 13

    total_batches = math.ceil(len(df) / BATCH_SIZE)

    generated_files = []

    for b in range(total_batches):

        batch_df = df.iloc[b * BATCH_SIZE : (b + 1) * BATCH_SIZE]

        reader = PdfReader(template_path)

        writer = PdfWriter()

        writer.append(reader)

        data_map = {}

        for i, (idx, row) in enumerate(batch_df.iterrows()):

            if i >= len(candidate_map): break

            slot = candidate_map[i]

            suffix = slot["s"]

            # --- DATA PREP ---

            full_name = clean_name(row.get("AttendeeName", ""))

            # Use existing helper to get 2-digit year (YY)

            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=False)

            # --- BUILD FIELD NAMES ---

            # Note: Whether "explicit" (1) or "dot" (.0), the format Name{suffix} works for both

            # e.g., Name1 or Name.0

            data_map[f"Name{suffix}"] = full_name

            data_map[f"Address{suffix}"] = str(row.get("Street", ""))

            data_map[f"City{suffix}"] = str(row.get("City", ""))

            data_map[f"Postal{suffix}"] = str(row.get("PostalCode", ""))

            data_map[f"Email{suffix}"] = str(row.get("E-mail", ""))

            data_map[f"Phone{suffix}"] = str(row.get("AttendeePhone", ""))

            data_map[f"DOBD{suffix}"] = dd

            data_map[f"DOBM{suffix}"] = mm

            data_map[f"DOBY{suffix}"] = yy

        for page in writer.pages:

            writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"BronzeStar_Batch_{b+1}.pdf")

        with open(out_name, "wb") as f: writer.write(f)

        generated_files.append(out_name)

    return generated_files

    # --- STANDARD FIRST AID LOGIC ---

def process_sfa(df, template_path, output_folder):

    # Mapping for Standard First Aid (1-10)

    candidate_map = []

    for i in range(1, 11):

        suffix = str(i)

        entry = {

            "name": f"NAME {suffix}",

            "addr": f"Address {suffix}",

            "apt":  f"Apt# {suffix}",

            "city": f"City {suffix}",

            "zip":  f"Postal Code {suffix}", # Specific to SFA form

            "email": f"Email {suffix}",

            "phone": f"Phone {suffix}",

            "dd": f"Day {suffix}",

            "mm": f"Month {suffix}",

            "yy": f"Year {suffix}"

        }

        candidate_map.append(entry)

    BATCH_SIZE = 10

    total_batches = math.ceil(len(df) / BATCH_SIZE)

    generated_files = []

    for b in range(total_batches):

        batch_df = df.iloc[b * BATCH_SIZE : (b + 1) * BATCH_SIZE]

        reader = PdfReader(template_path)

        writer = PdfWriter()

        writer.append(reader)

        data_map = {}

        for i, (idx, row) in enumerate(batch_df.iterrows()):

            if i >= len(candidate_map): break

            fields = candidate_map[i]

            full_name = clean_name(row.get("AttendeeName", ""))

            # SFA typically uses full 4-digit year

            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=True)

            data_map[fields["name"]] = full_name

            data_map[fields["addr"]] = str(row.get("Street", ""))

            # CSV usually lacks Apt column, leaving blank as per your script

            data_map[fields["apt"]] = "" 

            data_map[fields["city"]] = str(row.get("City", ""))

            data_map[fields["zip"]] = str(row.get("PostalCode", ""))

            data_map[fields["email"]] = str(row.get("E-mail", ""))

            data_map[fields["phone"]] = str(row.get("AttendeePhone", ""))

            data_map[fields["dd"]] = dd

            data_map[fields["mm"]] = mm

            data_map[fields["yy"]] = yy

        for page in writer.pages:

            writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"SFA_Exam_Sheet_{b+1}.pdf")

        with open(out_name, "wb") as f: writer.write(f)

        generated_files.append(out_name)

    return generated_files
 