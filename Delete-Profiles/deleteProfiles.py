import requests
import csv
import time

# Talon.One API configuration
BASE_URL = "https://verticurl.europe-west1.talon.one"
API_KEY = "5279880c6ea0ff0d56179dc6913b6fab4d4e367eb81da54952bd7db145d8afec"
HEADERS = {
    "Authorization": f"ApiKey-v1 {API_KEY}",
    "Content-Type": "application/json"
}

# Path to your CSV file
CSV_FILE = "deleteCustomers.csv"

def delete_customer_profile(integration_id):
    """Deletes a customer profile from Talon.One by IntegrationId"""
    url = f"{BASE_URL}/v1/customer_data/{integration_id}"
    response = requests.delete(url, headers=HEADERS)

    if response.status_code == 204:
        print(f"✅ Deleted profile: {integration_id}")
    elif response.status_code == 404:
        print(f"⚠️ Profile not found: {integration_id}")
    else:
        print(f"❌ Failed to delete {integration_id}: {response.status_code}, {response.text}")

def process_csv(file_path):
    """Reads IntegrationIds from CSV and deletes profiles"""
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            integration_id = row[0].strip()
            if integration_id.lower() == "integrationid":  # skip header
                continue
            delete_customer_profile(integration_id)
            time.sleep(0.2)  # Small delay to avoid rate limiting (adjust as needed)

if __name__ == "__main__":
    process_csv(CSV_FILE)
