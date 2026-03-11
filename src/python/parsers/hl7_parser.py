import pandas as pd
import os
from datetime import datetime
import uuid

# --- Configuration ---
# Pointing to the secure local directories defined in your architecture
CSV_INPUT_PATH = '../health-tech-master-pipeline/data/raw_csv/patients.csv'
HL7_OUTPUT_DIR = '../health-tech-master-pipeline/data/raw_hl7/'

# Facility identifiers for the HL7 MSH segment
SENDING_APP = 'SYNTHEA_GEN'
SENDING_FACILITY = 'MARIETTA_CLINIC'
RECEIVING_APP = 'MIRTH_ENGINE'
RECEIVING_FACILITY = 'LOCAL_HOSPITAL'

def clean_date(date_str):
    """Converts Synthea YYYY-MM-DD to HL7 YYYYMMDD format."""
    if pd.isna(date_str):
        return ""
    return str(date_str).replace('-', '')

def generate_hl7_adt_a01(row):
    """Constructs a raw ADT^A01 text message from a dataframe row."""
    # 1. Generate standard timestamps and unique IDs
    current_time = datetime.now().strftime('%Y%m%d%H%M%S')
    message_control_id = str(uuid.uuid4()).replace('-', '')[:20] # Keep it within HL7 length limits
    
    # 2. Extract and clean patient data
    pat_id = row.get('Id', 'UNKNOWN')
    last_name = row.get('LAST', '').upper()
    first_name = row.get('FIRST', '').upper()
    dob = clean_date(row.get('BIRTHDATE', ''))
    gender = row.get('GENDER', 'U') # M, F, or U for Unknown
    
    address = row.get('ADDRESS', '')
    city = row.get('CITY', '')
    state = row.get('STATE', '')
    zip_code = row.get('ZIP', '')
    
    # Handle NaN values from pandas
    address = address if pd.notna(address) else ""
    city = city if pd.notna(city) else ""
    state = state if pd.notna(state) else ""
    zip_code = zip_code if pd.notna(zip_code) else ""

    # 3. Construct the Segments
    # MSH: Message Header (Delimiters, Routing, Timestamps)
    msh = f"MSH|^~\\&|{SENDING_APP}|{SENDING_FACILITY}|{RECEIVING_APP}|{RECEIVING_FACILITY}|{current_time}||ADT^A01|{message_control_id}|P|2.3"
    
    # EVN: Event Type (Admission)
    evn = f"EVN|A01|{current_time}"
    
    # PID: Patient Identification (Demographics)
    # PID-3 is Patient ID. PID-5 is Name (Last^First). PID-7 is DOB. PID-8 is Gender. PID-11 is Address.
    pid = f"PID|1||{pat_id}||{last_name}^{first_name}||{dob}|{gender}|||{address}^^{city}^{state}^{zip_code}|||||||"
    
    # PV1: Patient Visit (Outpatient default for simulation)
    pv1 = f"PV1|1|O||||||||||||||||||||||||||||||||||||||||||||||||||"
    
    # 4. Assemble with carriage returns (standard HL7 segment terminator)
    hl7_message = f"{msh}\r{evn}\r{pid}\r{pv1}\r"
    return hl7_message, pat_id

def main():
    print(f"Loading Synthea data from {CSV_INPUT_PATH}...")
    
    try:
        df = pd.read_csv(CSV_INPUT_PATH)
    except FileNotFoundError:
        print(f"Error: Could not find {CSV_INPUT_PATH}. Did you run Synthea and place the CSV in the correct folder?")
        return

    # Ensure output directory exists
    os.makedirs(HL7_OUTPUT_DIR, exist_ok=True)
    
    success_count = 0
    
    for index, row in df.iterrows():
        hl7_payload, patient_id = generate_hl7_adt_a01(row)
        
        # Save each message as an individual .hl7 file
        filename = f"{patient_id}_ADT_A01.hl7"
        filepath = os.path.join(HL7_OUTPUT_DIR, filename)
        
        with open(filepath, 'w') as f:
            f.write(hl7_payload)
            
        success_count += 1
        
    print(f"Success! Generated {success_count} HL7 ADT^A01 messages in {HL7_OUTPUT_DIR}.")

if __name__ == "__main__":
    main()