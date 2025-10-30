#!/usr/bin/env python3
"""
OSM to Directory JSON Converter
================================
Converts the motorcycle_repair_shops.csv from OSM extraction
into directory_data.json format for the EU Motorcycle Directory website.

This script:
1. Reads motorcycle_repair_shops.csv (from OSM extractor)
2. Transforms data to match your website's JSON format
3. Generates directory_data.json for your website

Usage:
    python convert_osm_to_json.py
    
Or with custom input/output:
    python convert_osm_to_json.py --input shops.csv --output data/directory_data.json
"""

import csv
import json
import sys
import argparse
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any


def parse_csv_row(row: Dict[str, str]) -> Dict[str, Any]:
    """
    Convert a row from OSM CSV to website JSON format.
    
    OSM CSV columns:
    - osm_id, osm_type, latitude, longitude, name
    - address_street, address_housenumber, address_city, address_postcode, address_country
    - phone, mobile, email, website, opening_hours
    - shop_type, amenity, craft, brand, operator
    
    Website JSON format:
    {
        "name": "Shop Name",
        "address": "Street address",
        "city": "City, Country",
        "phone": "Phone number",
        "website": "URL",
        "rating": "4.5",
        "reviews_count": "123",
        "hours": "Mon-Fri 9-5",
        "latitude": 52.52,
        "longitude": 13.405,
        "business_type": "Motorcycle Repair Shop"
    }
    """
    
    # Build full address
    address_parts = []
    if row.get('address_street'):
        street = row['address_street']
        housenumber = row.get('address_housenumber', '')
        if housenumber:
            address_parts.append(f"{street} {housenumber}")
        else:
            address_parts.append(street)
    
    address = ', '.join(filter(None, address_parts)) if address_parts else 'Address not available'
    
    # Build city location - Use source_country as fallback
    city_name = row.get('address_city', '').strip()
    country_code = row.get('address_country', '').strip()
    source_country = row.get('source_country', '').strip()  # NEW: From PBF filename
    
    # Determine country name (priority: OSM data, then source file)
    if country_code:
        country_name = get_country_name(country_code)
    elif source_country:
        country_name = source_country  # Use country from filename
    else:
        country_name = ''
    
    # Build city string: "City, Country" or just "Country" if no city
    if city_name and country_name:
        city = f"{city_name}, {country_name}"
    elif country_name:
        city = f"Unknown City, {country_name}"  # Has country but no city
    elif city_name:
        city = f"{city_name}, Unknown Country"
    else:
        city = "Unknown City, Unknown Country"
    
    # Get contact info
    phone = row.get('phone') or row.get('mobile') or 'N/A'
    website = row.get('website') or 'N/A'
    
    # Parse opening hours (OSM format can be complex, so we keep it simple)
    hours = row.get('opening_hours') or 'Hours not available'
    
    # Get coordinates
    latitude = float(row['latitude']) if row.get('latitude') else None
    longitude = float(row['longitude']) if row.get('longitude') else None
    
    # Determine business type
    business_type = determine_business_type(row)
    
    # Get name (fallback to operator or brand if no name)
    name = row.get('name') or row.get('operator') or row.get('brand') or 'Unnamed Shop'
    
    # Create shop object
    shop = {
        'name': name.strip(),
        'address': address,
        'city': city,
        'phone': phone,
        'website': website,
        'rating': 'N/A',  # OSM doesn't have ratings (would need Google Places API)
        'reviews_count': '0',  # OSM doesn't have reviews
        'hours': hours,
        'latitude': latitude,
        'longitude': longitude,
        'business_type': business_type
    }
    
    return shop


def determine_business_type(row: Dict[str, str]) -> str:
    """Determine the business type from OSM tags"""
    shop_type = row.get('shop_type', '')
    amenity = row.get('amenity', '')
    craft = row.get('craft', '')
    
    if shop_type == 'motorcycle_repair' or amenity == 'motorcycle_repair' or craft == 'motorcycle_repair':
        return 'Motorcycle Repair Shop'
    elif shop_type == 'motorcycle':
        return 'Motorcycle Dealership'
    elif 'repair' in shop_type.lower() or 'repair' in amenity.lower():
        return 'Repair Service'
    else:
        return 'Motorcycle Shop'


def get_country_name(country_code: str) -> str:
    """Convert country code to full name"""
    country_map = {
        'AT': 'Austria',
        'BE': 'Belgium',
        'BG': 'Bulgaria',
        'HR': 'Croatia',
        'CY': 'Cyprus',
        'CZ': 'Czech Republic',
        'DK': 'Denmark',
        'EE': 'Estonia',
        'FI': 'Finland',
        'FR': 'France',
        'DE': 'Germany',
        'GR': 'Greece',
        'HU': 'Hungary',
        'IE': 'Ireland',
        'IT': 'Italy',
        'LV': 'Latvia',
        'LT': 'Lithuania',
        'LU': 'Luxembourg',
        'MT': 'Malta',
        'NL': 'Netherlands',
        'PL': 'Poland',
        'PT': 'Portugal',
        'RO': 'Romania',
        'SK': 'Slovakia',
        'SI': 'Slovenia',
        'ES': 'Spain',
        'SE': 'Sweden',
    }
    return country_map.get(country_code.upper(), country_code)


def build_countries_dict(shops: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """
    Build the countries dictionary with cities.
    Format: {"Country": ["City1", "City2", ...]}
    """
    countries = defaultdict(set)
    
    for shop in shops:
        city_field = shop.get('city', '')
        if ', ' in city_field:
            parts = city_field.split(', ')
            city = parts[0].strip()
            country = parts[-1].strip()
            if city and country:
                countries[country].add(city)
    
    # Convert sets to sorted lists
    return {country: sorted(list(cities)) for country, cities in sorted(countries.items())}


def convert_csv_to_json(input_csv: str, output_json: str, skip_unnamed: bool = True):
    """
    Main conversion function.
    
    Args:
        input_csv: Path to motorcycle_repair_shops.csv
        output_json: Path to output directory_data.json
        skip_unnamed: Skip shops without proper names
    """
    
    input_path = Path(input_csv)
    output_path = Path(output_json)
    
    # Check if input exists
    if not input_path.exists():
        print(f"❌ Error: Input file not found: {input_csv}")
        print(f"   Make sure you've run the OSM extractor first:")
        print(f"   python osm_mc_repair_extractor.py [countries]")
        sys.exit(1)
    
    print("=" * 70)
    print("OSM CSV TO DIRECTORY JSON CONVERTER")
    print("=" * 70)
    print(f"\n📂 Input:  {input_path.absolute()}")
    print(f"📂 Output: {output_path.absolute()}")
    print()
    
    shops = []
    skipped_count = 0
    
    # Read CSV
    print("📖 Reading CSV file...")
    with open(input_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        total_rows = len(rows)
        print(f"   Found {total_rows:,} rows")
    
    # Convert each row
    print("\n🔄 Converting to JSON format...")
    for i, row in enumerate(rows, 1):
        if i % 500 == 0:
            print(f"   Processing row {i:,}/{total_rows:,}...", end='\r')
        
        try:
            shop = parse_csv_row(row)
            
            # Skip unnamed shops if requested
            if skip_unnamed and (shop['name'] == 'Unnamed Shop' or not shop['name']):
                skipped_count += 1
                continue
            
            # Skip shops without location
            if not shop['latitude'] or not shop['longitude']:
                skipped_count += 1
                continue
            
            shops.append(shop)
        except Exception as e:
            print(f"\n⚠️  Warning: Error processing row {i}: {e}")
            skipped_count += 1
            continue
    
    print(f"\n   ✓ Converted {len(shops):,} shops")
    if skipped_count > 0:
        print(f"   ⚠ Skipped {skipped_count:,} shops (missing data)")
    
    # Build countries dictionary
    print("\n🌍 Building countries/cities index...")
    countries = build_countries_dict(shops)
    print(f"   ✓ Found {len(countries)} countries:")
    for country, cities in sorted(countries.items()):
        print(f"      • {country}: {len(cities)} cities")
    print(f"   ✓ Total cities: {sum(len(cities) for cities in countries.values())}")
    
    # Create final JSON structure
    data = {
        'shops': shops,
        'countries': countries
    }
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write JSON
    print("\n💾 Writing JSON file...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    file_size = output_path.stat().st_size / 1024  # KB
    print(f"   ✓ File written: {file_size:.1f} KB")
    
    # Summary
    print("\n" + "=" * 70)
    print("✅ CONVERSION COMPLETE!")
    print("=" * 70)
    print(f"Total shops:     {len(shops):,}")
    print(f"Countries:       {len(countries)}")
    print(f"Cities:          {sum(len(cities) for cities in countries.values())}")
    print(f"Output file:     {output_path.absolute()}")
    print("\n📌 Next step: Open your index.html in a browser to view the directory!")
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Convert OSM motorcycle repair shops CSV to directory JSON format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard conversion
  python convert_osm_to_json.py
  
  # Custom input/output
  python convert_osm_to_json.py --input my_shops.csv --output data/my_data.json
  
  # Include unnamed shops
  python convert_osm_to_json.py --include-unnamed
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        default='motorcycle_repair_shops.csv',
        help='Input CSV file from OSM extractor (default: motorcycle_repair_shops.csv)'
    )
    parser.add_argument(
        '--output', '-o',
        default='data/directory_data.json',
        help='Output JSON file for website (default: data/directory_data.json)'
    )
    parser.add_argument(
        '--include-unnamed',
        action='store_true',
        help='Include shops without proper names (default: skip them)'
    )
    
    args = parser.parse_args()
    
    convert_csv_to_json(
        input_csv=args.input,
        output_json=args.output,
        skip_unnamed=not args.include_unnamed
    )


if __name__ == '__main__':
    main()