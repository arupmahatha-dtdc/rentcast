import requests
from bs4 import BeautifulSoup
import csv
import time
import re
import os

def get_geoiq_url_for_pincode(pincode):
    """Get the correct GeoIQ URL for a pincode using their search API"""
    api_url = "https://prodapis-in.geoiq.ai/places/prod/v1.0/data_explore_search"
    payload = {"keyword": str(pincode)}
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    
    try:
        resp = requests.post(api_url, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        
        # Find the exact pincode match in the response
        for place in data.get("body", []):
            if place["name"].startswith(str(pincode)):
                # Build the pretty URL using pincode, place name, and id
                name = place["name"]
                pincode_part, place_part = name.split(" - ", 1)
                place_part = place_part.replace(",", "").replace(" ", "-")
                return f"https://geoiq.io/places/{pincode_part}---{place_part}/{place['id']}"
        return None
    except Exception as e:
        print(f"Error getting URL for pincode {pincode}: {e}")
        return None

def scrape_geoiq(url, pincode):
    """Scrape population, area, and demographic data from a GeoIQ page"""
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract pincode and place name from URL or title
        pincode_part = pincode
        place_name = ""
        
        # Get the main heading (pincode and place)
        heading = soup.find('h1')
        if heading:
            title_text = heading.text.strip()
            data = {"url": url, "pincode": pincode_part, "place_name": title_text}
        else:
            data = {"url": url, "pincode": pincode_part, "place_name": ""}

        # Get all text content
        text = soup.get_text(separator=" ")

        # Extract population (look for "population" followed by a number)
        population_match = re.search(r'population\s+(\d+)', text, re.IGNORECASE)
        data['population'] = population_match.group(1) if population_match else ""

        # Extract area (look for "square kilometer" preceded by a number)
        area_match = re.search(r'(\d+\.?\d*)\s+square\s+kilometer', text, re.IGNORECASE)
        data['area_km2'] = area_match.group(1) if area_match else ""

        # Extract male population (look for "male populations are" followed by a number)
        male_match = re.search(r'male\s+populations?\s+are?\s+(\d+)', text, re.IGNORECASE)
        data['male_population'] = male_match.group(1) if male_match else ""

        # Extract female population (look for "and" followed by a number, then "respectively")
        female_match = re.search(r'and\s+(\d+)\s+respectively', text, re.IGNORECASE)
        data['female_population'] = female_match.group(1) if female_match else ""

        return data
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return {
            "url": url, "pincode": pincode, "place_name": "", "population": "", "area_km2": "", 
            "male_population": "", "female_population": ""
        }

def get_pincodes_with_missing_data(csv_path):
    """Read pincodes that have missing data from the existing CSV file"""
    missing_pincodes = []
    
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Check if any of the key fields are missing
            population = row.get('population', '').strip()
            area_km2 = row.get('area_km2', '').strip()
            male_population = row.get('male_population', '').strip()
            female_population = row.get('female_population', '').strip()
            
            if not population or not area_km2 or not male_population or not female_population:
                missing_pincodes.append(row['pincode'])
    
    return missing_pincodes

def update_existing_data(existing_csv_path, new_data, output_csv_path):
    """Update the existing CSV with new data for missing pincodes"""
    # Read existing data
    existing_data = {}
    with open(existing_csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_data[row['pincode']] = row
    
    # Update with new data
    for new_row in new_data:
        pincode = new_row['pincode']
        if pincode in existing_data:
            # Only update fields that were missing and now have data
            existing_row = existing_data[pincode]
            for field in ['population', 'area_km2', 'male_population', 'female_population', 'place_name', 'url']:
                if not existing_row.get(field, '').strip() and new_row.get(field, '').strip():
                    existing_row[field] = new_row[field]
    
    # Write updated data back to CSV
    with open(output_csv_path, "w", newline="", encoding='utf-8') as f:
        fieldnames = [
            'url', 'pincode', 'place_name', 'population', 'area_km2', 'male_population', 
            'female_population'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing_data.values())
    
    return len(existing_data)

def scrape_missing_pincodes(existing_csv_path, output_csv_path):
    """Main function to scrape only pincodes with missing data"""
    # Get pincodes with missing data
    missing_pincodes = get_pincodes_with_missing_data(existing_csv_path)
    
    if not missing_pincodes:
        print("✅ No pincodes with missing data found!")
        return
    
    print(f"Found {len(missing_pincodes)} pincodes with missing data")
    print("Starting to scrape missing data...")
    
    results = []
    total = len(missing_pincodes)
    
    for i, pincode in enumerate(missing_pincodes, 1):
        print(f"Processing {i}/{total}: {pincode}")
        
        url = get_geoiq_url_for_pincode(pincode)
        
        if url:
            data = scrape_geoiq(url, pincode)
            results.append(data)
            
            # Add a small delay to be respectful to the server
            time.sleep(1)
        else:
            # Add empty data for pincodes without URLs
            results.append({
                "url": "", "pincode": pincode, "place_name": "", "population": "", 
                "area_km2": "", "male_population": "", "female_population": ""
            })
        
        # Show progress every 50 pincodes
        if i % 50 == 0:
            successful_count = len([r for r in results if r.get('population')])
            print(f"Progress: {i}/{total} pincodes processed ({successful_count} successful)")
    
    # Update the existing CSV with new data
    if results:
        total_records = update_existing_data(existing_csv_path, results, output_csv_path)
        successful_count = len([r for r in results if r.get('population')])
        print(f"\n✅ Done! Data updated in {output_csv_path}")
        print(f"📊 Successfully scraped {successful_count}/{total} missing pincodes")
        print(f"📁 Total records in file: {total_records}")
    else:
        print("❌ No data scraped.")

if __name__ == "__main__":
    # Use the existing GeoIQ data file
    existing_csv = os.path.join("processed_data", "geoiq_pincode_data.csv")
    output_csv = os.path.join("processed_data", "geoiq_pincode_data.csv")
    
    if not os.path.exists(existing_csv):
        print(f"❌ Existing CSV not found: {existing_csv}")
        print("Please run the original scraper first to create the initial data file.")
    else:
        scrape_missing_pincodes(existing_csv, output_csv)