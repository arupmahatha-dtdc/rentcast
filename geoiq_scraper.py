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

def get_all_pincodes_from_csv(csv_path):
    """Read all unique pincodes from the given CSV file"""
    pincodes = set()
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pincode = row.get('pincode') or row.get('Pincode') or row.get('PINCODE')
            if pincode:
                pincodes.add(str(pincode).strip())
    return sorted(pincodes)

def scrape_pincodes(pincodes, output_file):
    """Main function to scrape multiple pincodes and save to CSV"""
    results = []
    total = len(pincodes)
    
    print(f"Starting to scrape {total} pincodes...")
    
    for i, pincode in enumerate(pincodes, 1):
        url = get_geoiq_url_for_pincode(pincode)
        
        if url:
            data = scrape_geoiq(url, pincode)
            results.append(data)
        else:
            # Add empty data for pincodes without URLs
            results.append({
                "url": "", "pincode": pincode, "place_name": "", "population": "", 
                "area_km2": "", "male_population": "", "female_population": ""
            })
        
        # Show progress every 100 pincodes
        if i % 100 == 0:
            successful_count = len([r for r in results if r.get('population')])
            print(f"Progress: {i}/{total} pincodes processed ({successful_count} successful)")
    
    # Save to CSV
    if results:
        with open(output_file, "w", newline="", encoding='utf-8') as f:
            fieldnames = [
                'url', 'pincode', 'place_name', 'population', 'area_km2', 'male_population', 
                'female_population'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        successful_count = len([r for r in results if r.get('population')])
        print(f"\n✅ Done! Data saved to {output_file}")
        print(f"📊 Successfully scraped {successful_count}/{total} pincodes")
    else:
        print("❌ No data scraped.")

if __name__ == "__main__":
    # Read all pincodes from processed data/pincode_directory.csv
    input_csv = os.path.join("processed_data", "pincode_directory.csv")
    output_csv = os.path.join("processed_data", "geoiq_pincode_data.csv")
    if not os.path.exists(input_csv):
        print(f"❌ Input CSV not found: {input_csv}")
    else:
        pincodes = get_all_pincodes_from_csv(input_csv)
        scrape_pincodes(pincodes, output_csv)