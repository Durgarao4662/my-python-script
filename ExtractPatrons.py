import pandas as pd
from simple_salesforce import Salesforce
import math
import time
import csv

# ----------------------------
# CONFIGURATION
# ----------------------------

# Salesforce login credentials
SF_USERNAME = 'syed.sajidnlb@verticurl.com'
SF_PASSWORD = 'verticurl@1234'
SF_SECURITY_TOKEN = 'oiLqzlg7ghy1dbUgVnyntleC'

# Optional: use domain='test' for Sandbox
SF_DOMAIN = 'login'   # or 'test' for Sandbox

# Salesforce object and fields to extract
OBJECT_NAME = 'Contact'
FIELDS = ['Id', 'PDVID__c']

# Input / Output file paths
INPUT_CSV = 'PDVID_Uncovered.csv'      # your input file containing the Id column
OUTPUT_CSV = 'Available_PdvidsIn_SFSC.csv'    # where combined results will be saved

# ----------------------------
# MAIN SCRIPT
# ----------------------------

def connect_to_salesforce():
    print("🔐 Connecting to Salesforce...")
    sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD,
                    security_token=SF_SECURITY_TOKEN, domain=SF_DOMAIN)
    print("✅ Connected as:", SF_USERNAME)
    return sf

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def query_salesforce(sf, ids_batch):
    #old str = ",".join(["'%s'" % i for i in ids_batch])
    id_list_str = ",".join([str(i) for i in ids_batch])
    soql = f"SELECT {', '.join(FIELDS)} FROM {OBJECT_NAME} WHERE PDVID__c IN ({id_list_str})"
    results = sf.query_all(soql)
    return results['records']

def extract_data():
# Load PDVID__c IDs
    df = pd.read_csv(INPUT_CSV)

# Drop missing values and ensure numeric type
    ids = df['USER_ID'].dropna().astype(float).unique().tolist()
    print(f"📦 Loaded {len(ids)} Contact PDVID__c values from {INPUT_CSV}")
    sf = connect_to_salesforce()
    all_records = []
    batch_size = 1000

    print(f"🚀 Starting extraction in batches of {batch_size}...")

    for i, batch in enumerate(chunk_list(ids, batch_size), start=1):
        print(f"🔹 Querying batch {i} with {len(batch)} IDs...")
        try:
            records = query_salesforce(sf, batch)
            all_records.extend(records)
            print(f"✅ Retrieved {len(records)} records in batch {i}")
        except Exception as e:
            print(f"⚠️ Error in batch {i}: {e}")
        time.sleep(1)  # polite pause to avoid hitting API rate limits

    print(f"\n💾 Writing {len(all_records)} total records to {OUTPUT_CSV}")
    pd.DataFrame(all_records).to_csv(OUTPUT_CSV, index=False, quoting=csv.QUOTE_ALL)
    print("✅ Done! Data saved to", OUTPUT_CSV)


if __name__ == "__main__":
    extract_data()
