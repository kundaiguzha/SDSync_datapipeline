import requests
import os
import json

# Base URL
BASE_URL = "https://commonchemistry.cas.org/api"

# Directory to store JSON files
# Update paths to use Airflow's mounted directory
SAVE_DIR_RESULTS = "/opt/airflow/s3-drive/bronze_data/results/"
SAVE_DIR_DETAILS = "/opt/airflow/s3-drive/bronze_data/details/"

# Ensure directories exist
os.makedirs(SAVE_DIR_RESULTS, exist_ok=True)
os.makedirs(SAVE_DIR_DETAILS, exist_ok=True)

# Function to search for substances
def search_substances(query, page=1):
    """Search for substances and save the results to a JSON file."""
    endpoint = f"{BASE_URL}/search"
    params = {"q": query, "page": page}
    response = requests.get(endpoint, params=params)

    if response.status_code == 200:
        data = response.json()
        file_path = os.path.join(SAVE_DIR_RESULTS, f"search_results_{query}.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        print(f"✅ Search results saved: {file_path}")

        # Extract CAS RNs
        cas_rns = [item["rn"] for item in data.get("results", [])]
        return cas_rns
    else:
        print(f"❌ Error {response.status_code}: {response.text}")
        return []

# Function to get substance details
def get_substance_details(cas_rn):
    """Retrieve substance details and save to a JSON file."""
    endpoint = f"{BASE_URL}/detail"
    params = {"cas_rn": cas_rn}
    response = requests.get(endpoint, params=params)

    if response.status_code == 200:
        data = response.json()
        file_path = os.path.join(SAVE_DIR_DETAILS, f"details_{cas_rn}.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        print(f"✅ Details saved: {file_path}")
    else:
        print(f"❌ Error {response.status_code}: {response.text}")

# Main execution
if __name__ == "__main__":
    queries=['7732-18-5','7647-14-5','64-17-5','50-78-2','7664-93-9','7647-01-0','58-08-2','50-99-7','67-64-1']
    for query in queries : 
        
        # Step 1: Search for substances
        cas_rns = search_substances(query)

        # Step 2: Fetch details for each CAS RN
        if cas_rns:
            for cas_rn in cas_rns:
                get_substance_details(cas_rn)
        else:
            print("⚠️ No CAS RNs found.")