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
    # --- CONSTANT DATA (HOST & FACILITY) ---
    # The dump confirms these are SPLIT fields (Area Code + Number).
    HOST_DATA = {
        "Host Name": "City of Markham",
        "Host Address": "8600 McCowan Road",
        "Host City": "Markham",
        "Host Province": "ON",
        "Host Postal Code": "L3P 3M2",
        
        # SPLIT PHONES (Matches your dump exactly)
        "Host Area Code": "905",
        "Host Number": "470-3590 EXT 4342",
        
        "Facility Name": "Centennial C.C.",
        "Facility Area Code": "905",
        "Facility Number": "470-3590 EXT 4342",
        
        # SHOTGUN FALLBACKS (In case there are hidden fields)
        "Host Phone": "905-470-3590",
        "Facility Phone": "905-470-3590",
        "Telephone": "905-470-3590",
        "Phone": "905-470-3590"
    }

    # --- CANDIDATE MAPPING ---
    candidate_map = []
    # Generate the map for candidates 1 to 10
    for i in range(1, 11):
        suffix = str(i)
        entry = {
            "name": f"Name {suffix}",
            "addr": f"Address {suffix}",
            "apt":  f"apt {suffix}",      # Lowercase "apt" from dump
            "city": f"City {suffix}",
            "zip":  f"Postal {suffix}",
            "email": f"Email {suffix}",
            "phone": f"Phone {suffix}",
            "dd": f"Day {suffix}",
            "mm": f"Month {suffix}",
            "yy": f"Year {suffix}"
        }
        
        # SPECIAL CASE: Candidate 10's Name field is just "10"
        if i == 10:
            entry["name"] = "10"
            
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

        # --- 1. APPLY HOST & FACILITY DATA ---
        for field, value in HOST_DATA.items():
            data_map[field] = value

        # --- 2. APPLY CANDIDATE DATA ---
        for i, (idx, row) in enumerate(batch_df.iterrows()):
            if i >= len(candidate_map): break
            fields = candidate_map[i]
            
            full_name = clean_name(row.get("AttendeeName", ""))
            
            # Using shared parse_date utility (False = 2 digit year based on your snippet)
            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=False)

            data_map[fields["name"]] = full_name
            data_map[fields["addr"]] = str(row.get("Street", ""))
            data_map[fields["city"]] = str(row.get("City", ""))
            data_map[fields["zip"]] = str(row.get("PostalCode", ""))
            data_map[fields["email"]] = str(row.get("E-mail", ""))
            data_map[fields["phone"]] = str(row.get("AttendeePhone", ""))
            data_map[fields["dd"]] = dd
            data_map[fields["mm"]] = mm
            data_map[fields["yy"]] = yy

        for page in writer.pages:
            writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"EFA_Batch_{b+1}.pdf")
        with open(out_name, "wb") as f: 
            writer.write(f)
        generated_files.append(out_name)
    
    return generated_files
# --- BRONZE MEDALLION LOGIC ---
def process_bronze_med(df, template_path, output_folder):
    # --- INVOICING DATA (HOST & FACILITY) ---
    # Hardcoded based on your provided file
    HOST_DATA = {
        "host_name": "City of Markham",
        "host_area_code": "905",               
        "host_phone_num": "4703590 EXT 4342",  
        "host_addr": "8600 McCowan Road",
        "host_city": "Markham",
        "host_prov": "ON",
        "host_postal": "L3P 3M2",
        "facility_name": "Centennial C.C."
    }

    # --- FIELD MAPPING FOR INVOICE ---
    HOST_FIELD_MAP = {
        "host_name": "Text19",      
        "host_area_code": "Text20", 
        "host_phone_num": "Text21", 
        "host_addr": "Text22",      
        "host_city": "Text23",      
        "host_prov": "Text24",      
        "host_postal": "Text25",    
        "facility_name": "Text29"   
    }

    # --- CANDIDATE MAPPING ---
    candidate_map = [
        # === PAGE 1 (Candidates 1-6) ===
        {"base": "1", "s": ".0"},           # 1
        {"base": "1", "s": ".1.0"},         # 2
        {"base": "1", "s": ".1.1.0"},       # 3
        {"base": "1", "s": ".1.1.1.0"},     # 4
        {"base": "1", "s": ".1.1.1.1.0"},   # 5
        {"base": "1", "s": ".1.1.1.1.1"},   # 6

        # === PAGE 2 (Candidates 7-13) ===
        {"base": "", "s": ".0.0"},          # 7
        {"base": "", "s": ".0.1.0"},        # 8
        {"base": "", "s": ".0.1.1.0"},      # 9
        {"base": "", "s": ".0.1.1.1.0"},    # 10
        {"base": "", "s": ".0.1.1.1.1.0"},  # 11
        {"base": "", "s": ".0.1.1.1.1.1.0"},# 12
        {"base": "", "s": ".0.1.1.1.1.1.1"} # 13
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

        # --- 1. APPLY HOST & FACILITY DATA ---
        for key, pdf_field in HOST_FIELD_MAP.items():
            data_map[pdf_field] = HOST_DATA[key]

        # --- 2. APPLY CANDIDATE DATA ---
        for i, (idx, row) in enumerate(batch_df.iterrows()):
            if i >= len(candidate_map): break
            slot = candidate_map[i]
            base, s = slot["base"], slot["s"]
            
            full_name = clean_name(row.get("AttendeeName", ""))
            # Use shared parse_date utility (False = 2 digit year)
            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=False) 

            data_map[f"Name{base}{s}"] = full_name
            data_map[f"Address{base}{s}"] = str(row.get("Street", ""))
            data_map[f"City{base}{s}"] = str(row.get("City", ""))
            data_map[f"Postal{base}{s}"] = str(row.get("PostalCode", ""))
            data_map[f"Email{base}{s}"] = str(row.get("E-mail", ""))
            data_map[f"Phone{base}{s}"] = str(row.get("AttendeePhone", ""))
            data_map[f"DOBD{base}{s}"] = dd
            data_map[f"DOBM{base}{s}"] = mm
            data_map[f"DOBY{base}{s}"] = yy

        for page in writer.pages: writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"BronzeMed_Batch_{b+1}.pdf")
        with open(out_name, "wb") as f: writer.write(f)
        generated_files.append(out_name)
    
    return generated_files
# --- BRONZE CROSS LOGIC ---
def process_bronze_cross(df, template_path, output_folder):
    # --- INVOICING DATA (HOST & FACILITY) ---
    HOST_DATA = {
        "host_name": "City of Markham",
        "host_area_code": "905",
        "host_phone_num": "4703590 EXT 4342",
        "host_addr": "8600 McCowan Road",
        "host_city": "Markham",
        "host_prov": "ON",
        "host_postal": "L3P 3M2",
        "facility_name": "Centennial C.C."
    }

    # --- FIELD MAPPING FOR INVOICE ---
    HOST_FIELD_MAP = {
        "host_name": "Text19",      
        "host_area_code": "Text20", 
        "host_phone_num": "Text21", 
        "host_addr": "Text22",      
        "host_city": "Text23",      
        "host_prov": "Text24",      
        "host_postal": "Text25",    
        "facility_name": "Text29"   
    }

    # --- CANDIDATE MAPPING (With Super Shotgun Fix) ---
    candidate_map = [
        # === PAGE 1 (Candidates 1-6) ===
        {"p": "", "s": ".0"},           # 1
        {"p": "", "s": ".1.0"},         # 2
        {"p": "", "s": ".1.1.0"},       # 3 
        {"p": "", "s": ".1.1.1.0"},     # 4
        {"p": "", "s": ".1.1.1.1.0"},   # 5
        {"p": "", "s": ".1.1.1.1.1"},   # 6

        # === PAGE 2 (Candidates 7-13) ===
        {"p": "7", "s": ".0"},          # 7
        {"p": "8", "s": ".1.0"},        # 8
        
        # 9: SUPER SHOTGUN FIX
        {"p": "9", "s": ".1.1.0", "addr_override": [
            "Address1.1.1.0X",  # Most likely ghost field
            "Address1.1.1",     # Likely fallback
            "Address1.1.1.0",   # Possible conflict with Cand 3
            "Address1.1.0",     # Orphan
            "Address1.0",       # Orphan
            "Address1",         # Orphan
            "Address1.1",       # Orphan
            "Text2",            # Generic field often found in this section
            "Text16", "Text17", # Common generic backups
        ]},
        
        {"p": "10", "s": ".1.1.1.0", "name_override": "10"}, # 10
        {"p": "11", "s": ".1.1.1.1.0"},     # 11
        {"p": "12", "s": ".1.1.1.1.1"},     # 12
        {"p": "13", "s": ".1.1.1.1.1"},     # 13
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

        # --- 1. APPLY HOST & FACILITY DATA ---
        for key, pdf_field in HOST_FIELD_MAP.items():
            data_map[pdf_field] = HOST_DATA[key]

        # --- 2. APPLY CANDIDATE DATA ---
        for i, (idx, row) in enumerate(batch_df.iterrows()):
            if i >= len(candidate_map): break
            
            slot = candidate_map[i]
            p = slot.get("p", "") 
            s = slot.get("s", "") 
            prefix_str = p if p else ""
            
            # Build Standard Field Names
            f_name = f"{prefix_str}Name1{s}"
            f_addr = f"{prefix_str}Address1{s}"
            f_city = f"{prefix_str}City1{s}"
            f_zip  = f"{prefix_str}Postal1{s}"
            f_email = f"{prefix_str}Email1{s}"
            f_phone = f"{prefix_str}Phone1{s}"
            f_dd = f"{prefix_str}DOBD1{s}"
            f_mm = f"{prefix_str}DOBM1{s}"
            f_yy = f"{prefix_str}DOBY1{s}"

            # Handle Name Override
            if "name_override" in slot:
                f_name = slot["name_override"]

            # Data Preparation
            full_name = clean_name(row.get("AttendeeName", ""))
            address_val = str(row.get("Street", ""))
            
            # Use utility function for date parsing (False = 2 digit year)
            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=False)

            # Map Standard Data
            data_map[f_name] = full_name
            data_map[f_city] = str(row.get("City", ""))
            data_map[f_zip] = str(row.get("PostalCode", ""))
            data_map[f_email] = str(row.get("E-mail", ""))
            data_map[f_phone] = str(row.get("AttendeePhone", ""))
            data_map[f_dd] = dd
            data_map[f_mm] = mm
            data_map[f_yy] = yy

            # --- ADDRESS HANDLING (Shotgun Logic) ---
            if "addr_override" in slot:
                override = slot["addr_override"]
                if isinstance(override, list):
                    # Write the address to EVERY field in the list
                    for field in override:
                        data_map[field] = address_val
                else:
                    data_map[override] = address_val
            else:
                data_map[f_addr] = address_val

        for page in writer.pages:
            writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"BronzeCross_Batch_{b+1}.pdf")
        with open(out_name, "wb") as f:
            writer.write(f)
        generated_files.append(out_name)
    
    return generated_files

# --- BRONZE STAR LOGIC (Based on your provided script) ---

def process_bronze_star(df, template_path, output_folder):
    # --- INVOICING DATA (HOST & FACILITY) ---
    HOST_DATA = {
        "host_name": "City of Markham",
        "host_area_code": "905",
        "host_phone_num": "4703590 EXT 4342",
        "host_addr": "8600 McCowan Road",
        "host_city": "Markham",
        "host_prov": "ON",
        "host_postal": "L3P 3M2",
        "facility_name": "Centennial C.C."
    }

    # --- FIELD MAPPING FOR INVOICE ---
    HOST_FIELD_MAP = {
        "host_name": "Text19",      
        "host_area_code": "Text20", 
        "host_phone_num": "Text21", 
        "host_addr": "Text22",      
        "host_city": "Text23",      
        "host_prov": "Text24",      
        "host_postal": "Text25",    
        "facility_name": "Text29"   
    }

    # --- CANDIDATE MAPPING ---
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

        # --- 1. APPLY HOST & FACILITY DATA ---
        for key, pdf_field in HOST_FIELD_MAP.items():
            data_map[pdf_field] = HOST_DATA[key]

        # --- 2. APPLY CANDIDATE DATA ---
        for i, (idx, row) in enumerate(batch_df.iterrows()):
            if i >= len(candidate_map): break

            slot = candidate_map[i]
            suffix = slot["s"]

            # --- DATA PREP ---
            full_name = clean_name(row.get("AttendeeName", ""))
            
            # Use utility function for date parsing (False = 2 digit year)
            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=False)

            # --- BUILD FIELD NAMES & MAP ---
            # Whether "explicit" (1) or "dot" (.0), the format Name{suffix} works for both
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
        with open(out_name, "wb") as f: 
            writer.write(f)
        generated_files.append(out_name)

    return generated_files
    # --- STANDARD FIRST AID LOGIC ---

def process_sfa(df, template_path, output_folder):
    # --- HOST & FACILITY DATA ---
    # Based on your snippet, the PDF field names match these keys exactly
    HOST_DATA = {
        "Host Name": "City of Markham",
        "Host Phone": "9054703590 EXT 4342",
        "Host Address": "8600 McCowan Road",
        "Host City": "Markham",
        "Host Province": "ON",
        "Host Postal Code": "L3P 3M2",
        "Facility Name": "Centennial C.C."
    }

    # --- CANDIDATE MAPPING ---
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

        # --- 1. APPLY HOST & FACILITY DATA ---
        for field, value in HOST_DATA.items():
            data_map[field] = value

        # --- 2. APPLY CANDIDATE DATA ---
        for i, (idx, row) in enumerate(batch_df.iterrows()):
            if i >= len(candidate_map): break
            fields = candidate_map[i]
            
            full_name = clean_name(row.get("AttendeeName", ""))
            
            # Note: Your snippet used 2-digit year ([-2:]) for this specific form
            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=False)

            data_map[fields["name"]] = full_name
            data_map[fields["addr"]] = str(row.get("Street", ""))
            data_map[fields["apt"]] = ""  # CSV lacks Apt column
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
        with open(out_name, "wb") as f: 
            writer.write(f)
        generated_files.append(out_name)

    return generated_files
 
