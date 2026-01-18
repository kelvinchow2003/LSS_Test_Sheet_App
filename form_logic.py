import pandas as pd
from pypdf.generic import BooleanObject, NameObject, DictionaryObject
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

    # --- THE MAPPING ---
    candidate_map = []
    # Generate the map for candidates 1 to 10
    for i in range(1, 11):
        suffix = str(i)
        
        entry = {
            "name": f"Name {suffix}",
            "addr": f"Address {suffix}",
            "apt":  f"apt {suffix}",      # Lowercase "apt" to match your form
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

        # 1. APPLY HOST & FACILITY DATA
        for field, value in HOST_DATA.items():
            data_map[field] = value

        # 2. APPLY CANDIDATE DATA
        for i, (idx, row) in enumerate(batch_df.iterrows()):
            if i >= len(candidate_map): break
            
            fields = candidate_map[i]
            
            full_name = clean_name(row.get("AttendeeName", ""))
            
            # Date Parsing (Matches your script: 2-digit year)
            # We use the helper if available, or manual logic here to be safe
            raw_dob = row.get("DateOfBirth", "")
            dd, mm, yy = "", "", ""
            if pd.notna(raw_dob):
                try:
                    dt = pd.to_datetime(raw_dob, dayfirst=True)
                    dd = str(dt.day).zfill(2)
                    mm = str(dt.month).zfill(2)
                    yy = str(dt.year)
                except: pass

            data_map[fields["name"]] = full_name
            data_map[fields["addr"]] = str(row.get("Street", ""))
            data_map[fields["apt"]] = "" # Your CSV has no Apt column
            data_map[fields["city"]] = str(row.get("City", ""))
            data_map[fields["zip"]] = str(row.get("PostalCode", ""))
            data_map[fields["email"]] = str(row.get("E-mail", ""))
            data_map[fields["phone"]] = str(row.get("AttendeePhone", ""))
            data_map[fields["dd"]] = dd
            data_map[fields["mm"]] = mm
            data_map[fields["yy"]] = yy

        # Apply to all pages
        for page in writer.pages:
            writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"EFA_Test_Sheet_{b+1}.pdf")
        with open(out_name, "wb") as f: 
            writer.write(f)
        generated_files.append(out_name)
    
    return generated_files

# --- BRONZE MEDALLION LOGIC ---
def process_bronze_med(df, template_path, output_folder):
    # --- INVOICING DATA (HOST & FACILITY) ---
    HOST_DATA = {
        "host_name": "City of Markham",
        "host_area_code": "905",               
        "host_phone_num": "4703590 EXT 4342",  
        "host_addr": "8600 McCowan Road",
        "host_city": "Markham",
        "host_prov": "ON",
        "host_postal": "L3P 3M2",
        "facility_name": "Centennial C.C.",
        
        # EXAM TELEPHONE (Same as Host)
        "exam_area_code": "905",
        "exam_phone_num": "4703590 EXT 4342"
    }

    # --- FIELD MAPPING ---
    HOST_FIELD_MAP = {
        "host_name": "Text19",      
        "host_area_code": "Text20", 
        "host_phone_num": "Text21", 
        "host_addr": "Text22",      
        "host_city": "Text23",      
        "host_prov": "Text24",      
        "host_postal": "Text25",    
        "facility_name": "Text29",
        "exam_area_code": "Text30", 
        "exam_phone_num": "Text31"  
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
            b_val = slot["base"]
            s_val = slot["s"]
            
            # Clean Name
            full_name = clean_name(row.get("AttendeeName", ""))
            
            # Parse Date (2-digit year)
            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=False)

            # Map Data
            data_map[f"Name{b_val}{s_val}"] = full_name
            data_map[f"Address{b_val}{s_val}"] = str(row.get("Street", ""))
            data_map[f"City{b_val}{s_val}"] = str(row.get("City", ""))
            data_map[f"Postal{b_val}{s_val}"] = str(row.get("PostalCode", ""))
            data_map[f"Email{b_val}{s_val}"] = str(row.get("E-mail", ""))
            data_map[f"Phone{b_val}{s_val}"] = str(row.get("AttendeePhone", ""))
            data_map[f"DOBD{b_val}{s_val}"] = dd
            data_map[f"DOBM{b_val}{s_val}"] = mm
            data_map[f"DOBY{b_val}{s_val}"] = yy

        # Apply to all pages
        for page in writer.pages:
            writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"BronzeMed_Batch_{b+1}.pdf")
        with open(out_name, "wb") as f:
            writer.write(f)
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
        "facility_name": "Centennial C.C.",
        
        # EXAM TELEPHONE (Same as Host)
        # Using Text30/Text31 which follow the Facility Name
        "exam_area_code": "905",
        "exam_phone_num": "4703590 EXT 4342"
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
        "facility_name": "Text29",
        "exam_area_code": "Text30", # New for Exam Phone
        "exam_phone_num": "Text31"  # New for Exam Phone
    }

    # --- CANDIDATE MAPPING ---
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
        
        # 9: FIXED (Only targeting correct Address fields)
        {"p": "9", "s": ".1.1.0", "addr_override": [
            "9Address1.1.1.0",  # Logical field for 9
            "Address1.1.1.0X",  # Ghost field
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
            
            # Parse Date (2-digit year)
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

            # --- ADDRESS HANDLING ---
            if "addr_override" in slot:
                override = slot["addr_override"]
                if isinstance(override, list):
                    # Write to all fields in the safe list
                    for field in override:
                        data_map[field] = address_val
                else:
                    data_map[override] = address_val
            else:
                data_map[f_addr] = address_val

        # Apply to all pages
        for page in writer.pages:
            writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"BronzeCross_Batch_{b+1}.pdf")
        with open(out_name, "wb") as f:
            writer.write(f)
        generated_files.append(out_name)
    
    return generated_files
# --- BRONZE STAR LOGIC ---
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
        "facility_name": "Centennial C.C.",
        
        # EXAM TELEPHONE (Same as Host)
        "exam_area_code": "905",
        "exam_phone_num": "4703590 EXT 4342"
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
        "facility_name": "Text29",
        "exam_area_code": "Text30", 
        "exam_phone_num": "Text31"
    }

    # --- CANDIDATE MAPPING ---
    candidate_map = [
        # === PAGE 1 (Candidates 1-6) ===
        {"type": "explicit", "s": "1"}, # 1
        {"type": "explicit", "s": "2"}, # 2
        {"type": "explicit", "s": "3"}, # 3
        {"type": "explicit", "s": "4"}, # 4
        {"type": "explicit", "s": "5"}, # 5
        {"type": "explicit", "s": "6"}, # 6

        # === PAGE 2 (Candidates 7-13) ===
        {"type": "dot", "s": ".0"},           # 7
        {"type": "dot", "s": ".1.0"},         # 8
        {"type": "dot", "s": ".1.1.0"},       # 9
        {"type": "dot", "s": ".1.1.1.0"},     # 10
        {"type": "dot", "s": ".1.1.1.1.0"},   # 11
        {"type": "dot", "s": ".1.1.1.1.1.0"}, # 12
        {"type": "dot", "s": ".1.1.1.1.1.1"}, # 13
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
            
            # --- BUILD FIELD NAMES ---
            # Logic handles both "explicit" (Name1) and "dot" (Name.0) patterns
            s = slot["s"]
            f_name = f"Name{s}"
            f_addr = f"Address{s}"
            f_city = f"City{s}"
            f_zip  = f"Postal{s}"
            f_email = f"Email{s}"
            f_phone = f"Phone{s}"
            f_dd = f"DOBD{s}"
            f_mm = f"DOBM{s}"
            f_yy = f"DOBY{s}"

            # DATA PREP
            full_name = clean_name(row.get("AttendeeName", ""))
            
            # Parse Date (2-digit year)
            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=False)

            # MAPPING
            data_map[f_name] = full_name
            data_map[f_addr] = str(row.get("Street", ""))
            data_map[f_city] = str(row.get("City", ""))
            data_map[f_zip] = str(row.get("PostalCode", ""))
            data_map[f_email] = str(row.get("E-mail", ""))
            data_map[f_phone] = str(row.get("AttendeePhone", ""))
            data_map[f_dd] = dd
            data_map[f_mm] = mm
            data_map[f_yy] = yy

        # Apply to all pages
        for page in writer.pages:
            writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"BronzeStar_Batch_{b+1}.pdf")
        with open(out_name, "wb") as f:
            writer.write(f)
        generated_files.append(out_name)
    
    return generated_files
# --- STANDARD FIRST AID LOGIC ---
def process_sfa(df, template_path, output_folder):
    # --- CONSTANT DATA (HOST & FACILITY) ---
    HOST_DATA = {
        "Host Name": "City of Markham",
        "Host Phone": "9054703590 EXT 4342",
        "Host Address": "8600 McCowan Road",
        "Host City": "Markham",
        "Host Province": "ON",
        "Host Postal Code": "L3P 3M2",
        "Facility Name": "Centennial C.C.",
        "Facility Phone": "9054703590 EXT 4342"  # Added Facility Phone
    }

    # --- THE MAPPING ---
    candidate_map = []
    # Generate the map for candidates 1 to 10
    for i in range(1, 11):
        suffix = str(i)
        entry = {
            "name": f"NAME {suffix}",
            "addr": f"Address {suffix}",
            "apt":  f"Apt# {suffix}",
            "city": f"City {suffix}",
            "zip":  f"Postal Code {suffix}",
            "email": f"Email {suffix}",
            "phone": f"Phone {suffix}",
            "dd": f"Day {suffix}",
            "mm": f"Month {suffix}",
            "yy": f"Year {suffix}",
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

        # 1. APPLY HOST & FACILITY DATA
        for field, value in HOST_DATA.items():
            data_map[field] = value
        
        # 2. APPLY CANDIDATE DATA
        for i, (idx, row) in enumerate(batch_df.iterrows()):
            if i >= len(candidate_map): break
            
            fields = candidate_map[i]
            
            full_name = clean_name(row.get("AttendeeName", ""))
            
            # Parse Date (4-digit year to match your SFA script)
            # We bypass the helper to ensure 4-digit year is forced as per your snippet: yy = str(dt.year)
            raw_dob = row.get("DateOfBirth", "")
            dd, mm, yy = "", "", ""
            if pd.notna(raw_dob):
                try:
                    dt = pd.to_datetime(raw_dob, dayfirst=True)
                    dd = str(dt.day).zfill(2)
                    mm = str(dt.month).zfill(2)
                    yy = str(dt.year) # 4-digit year
                except: pass

            data_map[fields["name"]] = full_name
            data_map[fields["addr"]] = str(row.get("Street", ""))
            data_map[fields["apt"]] = "" # No Apt column in standard CSV
            data_map[fields["city"]] = str(row.get("City", ""))
            data_map[fields["zip"]] = str(row.get("PostalCode", ""))
            data_map[fields["email"]] = str(row.get("E-mail", ""))
            data_map[fields["phone"]] = str(row.get("AttendeePhone", ""))
            
            data_map[fields["dd"]] = dd
            data_map[fields["mm"]] = mm
            data_map[fields["yy"]] = yy

        # Apply to all pages
        for page in writer.pages:
            writer.update_page_form_field_values(page, data_map)

        out_name = os.path.join(output_folder, f"SFA_Test_Sheet_{b+1}.pdf")
        with open(out_name, "wb") as f:
            writer.write(f)
        generated_files.append(out_name)
    
    return generated_files
 
# --- AIRWAY MANAGEMENT LOGIC ---
def process_airway_management(df, template_path, output_folder):
    # --- INVOICING DATA (HOST & FACILITY) ---
    HOST_FIELD_MAP = {
        # FRONT PAGE
        "Host Name": "City of Markham",
        "Host Area Code": "905",
        "Host Telephone #": "4703590 EXT 4342",
        "Host Address": "8600 McCowan Road",
        "Host City": "Markham",
        "Host Prov": "ON",
        "Host Postal Code": "L3P 3M2",
        "Facility Name": "Centennial C.C.",
        "Facility Area Code": "905",
        "Facility Telephone #": "4703590 EXT 4342",

        # REVERSE PAGE
        "Host Name Reverse": "City of Markham",
        "Host Area Code Reverse": "905",
        "Host Telephone # Reverse": "4703590 EXT 4342",
        "Facility Name Reverse": "Centennial C.C.",
        "Facility Area Code Reverse": "905",
        "Facility Telephone # Reverse": "4703590 EXT 4342"
    }

    # --- CANDIDATE MAPPING ---
    candidate_map = []
    for i in range(1, 11):
        s = str(i)
        
        # HANDLE TYPO IN PDF: Candidate 5 has "postal code5" (no space)
        if i == 5:
            p_code = "postal code5"
        else:
            p_code = f"postal code {s}"

        entry = {
            "name": f"Name {s}",
            "addr": f"address {s}", 
            "apt":  f"apt# {s}",
            "city": f"city {s}",
            "zip":  p_code,
            "email": f"email {s}",
            "phone": f"phone {s}",
            "dd": f"day {s}",
            "mm": f"month {s}",
            "yy": f"year {s}"
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
        
        # 1. APPLY HOST & FACILITY DATA
        for field_name, value in HOST_FIELD_MAP.items():
            data_map[field_name] = value

        # 2. APPLY CANDIDATE DATA
        for i, (idx, row) in enumerate(batch_df.iterrows()):
            if i >= len(candidate_map): break
            fields = candidate_map[i]
            
            full_name = clean_name(row.get("AttendeeName", ""))
            
            # Script uses 2-digit year
            dd, mm, yy = parse_date(row.get("DateOfBirth", ""), use_full_year=False)

            data_map[fields["name"]] = full_name
            data_map[fields["addr"]] = str(row.get("Street", ""))
            data_map[fields["apt"]] = "" # No Apt in CSV usually
            data_map[fields["city"]] = str(row.get("City", ""))
            data_map[fields["zip"]] = str(row.get("PostalCode", ""))
            data_map[fields["email"]] = str(row.get("E-mail", ""))
            data_map[fields["phone"]] = str(row.get("AttendeePhone", ""))
            data_map[fields["dd"]] = dd
            data_map[fields["mm"]] = mm
            data_map[fields["yy"]] = yy

        # Apply data to all pages
        for page in writer.pages:
            writer.update_page_form_field_values(page, data_map)

        # =========================================================
        # CRITICAL FIX 1: Fix "Floating Text" / Font Issues
        #Forces viewer to regenerate field appearance using native fonts
        # =========================================================
        if "/AcroForm" not in writer.root_object:
            writer.root_object.update({
                NameObject("/AcroForm"): DictionaryObject()
            })
        writer.root_object["/AcroForm"][NameObject("/NeedAppearances")] = BooleanObject(True)

        # =========================================================
        # CRITICAL FIX 2: Fix "French text on top of English"
        # Copies Layer settings to ensure hidden layers stay hidden
        # =========================================================
        if "/OCProperties" in reader.root_object:
            writer.root_object[NameObject("/OCProperties")] = \
                reader.root_object["/OCProperties"].clone(writer)

        out_name = os.path.join(output_folder, f"Airway_Mgmt_Batch_{b+1}.pdf")
        with open(out_name, "wb") as f: writer.write(f)
        generated_files.append(out_name)
    
    return generated_files



# --- LEADERSHIP MASTERSHEET LOGIC ---
def process_leadership_mastersheet(df, template_path, output_folder):
    # --- HOST DATA ---
    HOST_DATA = {
        "Host Name": "City of Markham",
        "Host Area": "905",
        "Host Phone": "4703590 EXT 4342",
        "Host Street": "8600 McCowan Road",
        "Host City": "Markham",
        "Host Province": "ON",
        "Host Postal": "L3P 3M2",
        "Host Facility": "Centennial C.C.",
        "Host Facility Area": "905",
        "Host Facility Phone": "4703590 EXT 4342",
        "Exam Fees Attached": "/Yes"
    }

    total_candidates = len(df)
    generated_files = []

    # Helper to get slot data
    def get_slot_data(row, field_id, visible_number):
        p = str(field_id)      # The PDF field ID (4, 5, 6...)
        num_str = str(visible_number) # The text to type (10, 11, 12...)
        
        full_name = clean_name(row.get("AttendeeName", ""))
        street = str(row.get("Street", ""))
        city = str(row.get("City", ""))
        zip_code = str(row.get("PostalCode", ""))
        full_address = f"{street}, {city} {zip_code}".strip(", ")

        # DOB Formatting: YY/MM/DD
        raw_dob = row.get("DateOfBirth", "")
        formatted_dob = ""
        if pd.notna(raw_dob):
            try:
                dt = pd.to_datetime(raw_dob, dayfirst=True)
                formatted_dob = dt.strftime("%y/%m/%d")
            except: pass

        data = {
            f"{p}.1": full_name,
            f"{p}.2": full_address,
            f"{p}.3": str(row.get("AttendeePhone", "")),
            f"{p}.4": str(row.get("E-mail", "")),
            f"{p}.5": formatted_dob
        }

        # Write the GLOBAL candidate number into the box (e.g. '10', '11')
        # Only for slots > 3 (The back page) or if explicitly needed
        if int(field_id) > 3:
            data[f"{p}.0"] = num_str

        return data

    # Helper to save file
    def _finalize_and_save(writer, reader, data_map, filename):
        for page in writer.pages:
            writer.update_page_form_field_values(page, data_map)

        # Force Fonts & Layers (Critical for new PDF forms)
        if "/AcroForm" not in writer.root_object:
            writer.root_object.update({NameObject("/AcroForm"): DictionaryObject()})
        writer.root_object["/AcroForm"][NameObject("/NeedAppearances")] = BooleanObject(True)

        if "/OCProperties" in reader.root_object:
            writer.root_object[NameObject("/OCProperties")] = \
                reader.root_object["/OCProperties"].clone(writer)

        out_path = os.path.join(output_folder, filename)
        with open(out_path, "wb") as f:
            writer.write(f)
        generated_files.append(out_path)

    # --- 1. MASTER FILE (Candidates 1-9) ---
    batch1 = df.iloc[0:9]
    if not batch1.empty:
        reader = PdfReader(template_path)
        writer = PdfWriter()
        writer.append(reader) 

        data_map = HOST_DATA.copy()
        data_map["Total Enrolled"] = str(total_candidates)

        for i, (idx, row) in enumerate(batch1.iterrows()):
            current_num = i + 1  # 1, 2, 3... 9
            # For the Master sheet, Field ID and Candidate Number are the same
            data_map.update(get_slot_data(row, field_id=current_num, visible_number=current_num))

        _finalize_and_save(writer, reader, data_map, "Leadership_Master_1.pdf")

    # --- 2. CONTINUATION FILES (Candidates 10+) ---
    start_index = 9
    batch_counter = 2
    
    while start_index < total_candidates:
        reader = PdfReader(template_path)
        writer = PdfWriter()
        
        # Copy PDF and remove Page 1 (Front page)
        writer.append(reader)
        if len(writer.pages) > 0:
            del writer.pages[0]

        batch_next = df.iloc[start_index : start_index + 6]
        data_map = HOST_DATA.copy()
        data_map["Total Enrolled"] = str(total_candidates)

        # Loop through the batch (up to 6 people)
        for i, (idx, row) in enumerate(batch_next.iterrows()):
            # The PDF Field IDs are HARDCODED to 4, 5, 6, 7, 8, 9 on the back page
            pdf_field_id = i + 4 
            if pdf_field_id > 9: break 
            
            # The Visible Number continues counting (10, 11, 12...)
            actual_candidate_num = (start_index + 1) + i
            
            data_map.update(get_slot_data(row, field_id=pdf_field_id, visible_number=actual_candidate_num))

        _finalize_and_save(writer, reader, data_map, f"Leadership_Continuation_{batch_counter}.pdf")
        
        start_index += 6
        batch_counter += 1

    return generated_files