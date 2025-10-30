#!/usr/bin/env python3
"""
OSM Motorcycle Repair Shop Extractor
=====================================
Downloads OpenStreetMap data from Geofabrik for multiple EU countries
and extracts motorcycle repair shops to a CSV file.

Author: Generated for MC Repair Directory Project
Date: 2025-10-29
"""

import os
import sys
import csv
import argparse
from pathlib import Path
from typing import List, Dict, Set
import requests
from urllib.parse import urljoin
import osmium


class MotorcycleRepairShopHandler(osmium.SimpleHandler):
    """
    Handler to extract motorcycle repair shops from OSM data.
    
    OSM Tags we're looking for:
    - shop=motorcycle_repair
    - shop=motorcycle
    - service:vehicle:motorcycle=yes (with repair capability)
    - amenity=motorcycle_repair
    """
    
    def __init__(self, source_country=None):
        osmium.SimpleHandler.__init__(self)
        self.repair_shops = []
        self.processed_count = 0
        self.source_country = source_country  # Track which country file this is from
        
    def node(self, n):
        """Process OSM nodes (point features)"""
        self.processed_count += 1
        if self.processed_count % 100000 == 0:
            print(f"  Processed {self.processed_count:,} nodes...", end='\r')
        
        if self._is_motorcycle_repair_shop(n.tags):
            self._add_shop(n, 'node')
    
    def way(self, w):
        """Process OSM ways (linear/polygon features)"""
        if self._is_motorcycle_repair_shop(w.tags):
            # Calculate centroid for ways
            try:
                location = w.nodes[0].location if w.nodes else None
                self._add_shop(w, 'way', location)
            except:
                # If location data is not available, skip
                pass
    
    def area(self, a):
        """Process OSM areas (polygon features)"""
        if self._is_motorcycle_repair_shop(a.tags):
            try:
                # Get center point of the area
                self._add_shop(a, 'area', a.center_location())
            except:
                pass
    
    def _is_motorcycle_repair_shop(self, tags) -> bool:
        """
        Determine if an OSM element is a motorcycle repair shop.
        
        Criteria:
        1. shop=motorcycle_repair (primary tag)
        2. shop=motorcycle (motorcycle shops often do repairs)
        3. amenity=motorcycle_repair
        4. service:vehicle:motorcycle=yes + any repair-related tags
        """
        # Direct motorcycle repair tags
        if tags.get('shop') in ['motorcycle_repair', 'motorcycle']:
            return True
        
        if tags.get('amenity') == 'motorcycle_repair':
            return True
        
        # Service tags with motorcycle
        if tags.get('service:vehicle:motorcycle') == 'yes':
            # Check if it has repair capabilities
            if (tags.get('service:vehicle:repair') == 'yes' or 
                tags.get('repair') == 'yes' or
                'repair' in tags.get('craft', '')):
                return True
        
        # Additional check for craft=motorcycle_repair
        if tags.get('craft') == 'motorcycle_repair':
            return True
        
        return False
    
    def _add_shop(self, element, element_type: str, location=None):
        """Extract relevant information and add to our list"""
        tags = element.tags
        
        # Get location
        if location is None:
            try:
                location = element.location
            except:
                return  # Skip if no location available
        
        shop_data = {
            'osm_id': element.id,
            'osm_type': element_type,
            'latitude': location.lat if location else None,
            'longitude': location.lon if location else None,
            'name': tags.get('name', ''),
            'shop_type': tags.get('shop', ''),
            'amenity': tags.get('amenity', ''),
            'craft': tags.get('craft', ''),
            'brand': tags.get('brand', ''),
            'operator': tags.get('operator', ''),
            'address_street': tags.get('addr:street', ''),
            'address_housenumber': tags.get('addr:housenumber', ''),
            'address_city': tags.get('addr:city', ''),
            'address_postcode': tags.get('addr:postcode', ''),
            'address_country': tags.get('addr:country', ''),
            'phone': tags.get('phone', ''),
            'mobile': tags.get('mobile', ''),
            'email': tags.get('email', ''),
            'website': tags.get('website', ''),
            'opening_hours': tags.get('opening_hours', ''),
            'service_motorcycle': tags.get('service:vehicle:motorcycle', ''),
            'service_repair': tags.get('service:vehicle:repair', ''),
            'source_country': self.source_country,  # NEW: Track source file
        }
        
        self.repair_shops.append(shop_data)


class GeofabrikDownloader:
    """Handle downloading of OSM PBF files from Geofabrik"""
    
    BASE_URL = "https://download.geofabrik.de"
    
    # Common EU country codes and their Geofabrik paths
    EU_COUNTRIES = {
        'austria': 'europe/austria-latest.osm.pbf',
        'belgium': 'europe/belgium-latest.osm.pbf',
        'bulgaria': 'europe/bulgaria-latest.osm.pbf',
        'croatia': 'europe/croatia-latest.osm.pbf',
        'cyprus': 'europe/cyprus-latest.osm.pbf',
        'czech-republic': 'europe/czech-republic-latest.osm.pbf',
        'denmark': 'europe/denmark-latest.osm.pbf',
        'estonia': 'europe/estonia-latest.osm.pbf',
        'finland': 'europe/finland-latest.osm.pbf',
        'france': 'europe/france-latest.osm.pbf',
        'germany': 'europe/germany-latest.osm.pbf',
        'greece': 'europe/greece-latest.osm.pbf',
        'hungary': 'europe/hungary-latest.osm.pbf',
        'ireland': 'europe/ireland-and-northern-ireland-latest.osm.pbf',
        'italy': 'europe/italy-latest.osm.pbf',
        'latvia': 'europe/latvia-latest.osm.pbf',
        'lithuania': 'europe/lithuania-latest.osm.pbf',
        'luxembourg': 'europe/luxembourg-latest.osm.pbf',
        'malta': 'europe/malta-latest.osm.pbf',
        'netherlands': 'europe/netherlands-latest.osm.pbf',
        'poland': 'europe/poland-latest.osm.pbf',
        'portugal': 'europe/portugal-latest.osm.pbf',
        'romania': 'europe/romania-latest.osm.pbf',
        'slovakia': 'europe/slovakia-latest.osm.pbf',
        'slovenia': 'europe/slovenia-latest.osm.pbf',
        'spain': 'europe/spain-latest.osm.pbf',
        'sweden': 'europe/sweden-latest.osm.pbf',
    }
    
    def __init__(self, download_dir: str = "osm_downloads"):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
    
    def download_country(self, country: str, force_redownload: bool = False) -> Path:
        """
        Download OSM PBF file for a country from Geofabrik.
        
        Args:
            country: Country name (e.g., 'germany', 'france')
            force_redownload: If True, download even if file exists
            
        Returns:
            Path to the downloaded file
        """
        country = country.lower()
        
        if country not in self.EU_COUNTRIES:
            raise ValueError(f"Country '{country}' not found. Available: {', '.join(self.EU_COUNTRIES.keys())}")
        
        relative_path = self.EU_COUNTRIES[country]
        url = urljoin(self.BASE_URL + '/', relative_path)
        filename = Path(relative_path).name
        output_path = self.download_dir / filename
        
        # Check if file already exists
        if output_path.exists() and not force_redownload:
            print(f"✓ File already exists: {output_path}")
            return output_path
        
        print(f"\nDownloading {country.title()} OSM data...")
        print(f"URL: {url}")
        print(f"Destination: {output_path}")
        
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            block_size = 8192
            downloaded = 0
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=block_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            mb_downloaded = downloaded / (1024 * 1024)
                            mb_total = total_size / (1024 * 1024)
                            print(f"  Progress: {progress:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end='\r')
            
            print(f"\n✓ Downloaded successfully: {output_path}")
            return output_path
            
        except requests.exceptions.RequestException as e:
            print(f"\n✗ Error downloading {country}: {e}")
            if output_path.exists():
                output_path.unlink()  # Remove partial download
            raise
    
    def download_countries(self, countries: List[str], force_redownload: bool = False) -> List[Path]:
        """Download multiple countries"""
        downloaded_files = []
        
        for country in countries:
            try:
                file_path = self.download_country(country, force_redownload)
                downloaded_files.append(file_path)
            except Exception as e:
                print(f"Failed to download {country}: {e}")
                continue
        
        return downloaded_files
    
    @classmethod
    def list_available_countries(cls):
        """Print all available EU countries"""
        print("\nAvailable EU Countries:")
        print("=" * 50)
        for country in sorted(cls.EU_COUNTRIES.keys()):
            print(f"  • {country}")
        print()


def process_pbf_file(pbf_file: Path) -> List[Dict]:
    """
    Process a PBF file and extract motorcycle repair shops.
    
    Args:
        pbf_file: Path to the .osm.pbf file
        
    Returns:
        List of dictionaries containing repair shop data
    """
    print(f"\nProcessing: {pbf_file.name}")
    
    # Extract country name from filename (e.g., "luxembourg-latest.osm.pbf" -> "Luxembourg")
    country_name = pbf_file.stem.replace('-latest', '').replace('.osm', '')
    country_name = country_name.replace('-', ' ').title()  # "Czech-Republic" -> "Czech Republic"
    
    # Map common country names
    country_mapping = {
        'Ireland And Northern Ireland': 'Ireland',
    }
    country_name = country_mapping.get(country_name, country_name)
    
    print(f"Source country: {country_name}")
    
    handler = MotorcycleRepairShopHandler(source_country=country_name)
    
    try:
        # Apply the handler to the PBF file
        handler.apply_file(str(pbf_file), locations=True)
        print(f"\n✓ Found {len(handler.repair_shops)} motorcycle repair shops")
        return handler.repair_shops
    except Exception as e:
        print(f"\n✗ Error processing {pbf_file}: {e}")
        return []


def export_to_csv(repair_shops: List[Dict], output_file: str = "motorcycle_repair_shops.csv"):
    """
    Export repair shops to CSV file.
    
    Args:
        repair_shops: List of repair shop dictionaries
        output_file: Output CSV filename
    """
    if not repair_shops:
        print("No repair shops to export.")
        return
    
    # Define CSV columns
    fieldnames = [
        'osm_id', 'osm_type', 'latitude', 'longitude',
        'name', 'shop_type', 'amenity', 'craft', 'brand', 'operator',
        'address_street', 'address_housenumber', 'address_city', 
        'address_postcode', 'address_country',
        'phone', 'mobile', 'email', 'website', 'opening_hours',
        'service_motorcycle', 'service_repair', 'source_country'  # NEW: Added source_country
    ]
    
    output_path = Path(output_file)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(repair_shops)
    
    print(f"\n✓ Exported {len(repair_shops)} repair shops to: {output_path.absolute()}")
    print(f"  File size: {output_path.stat().st_size / 1024:.1f} KB")


def main():
    parser = argparse.ArgumentParser(
        description='Extract motorcycle repair shops from OpenStreetMap data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download and process Germany and France
  python osm_mc_repair_extractor.py germany france
  
  # Process already downloaded files
  python osm_mc_repair_extractor.py --skip-download germany
  
  # List available countries
  python osm_mc_repair_extractor.py --list-countries
  
  # Custom output file
  python osm_mc_repair_extractor.py germany --output my_shops.csv
        """
    )
    
    parser.add_argument(
        'countries',
        nargs='*',
        help='Country names to process (e.g., germany france spain)'
    )
    parser.add_argument(
        '--output', '-o',
        default='motorcycle_repair_shops.csv',
        help='Output CSV filename (default: motorcycle_repair_shops.csv)'
    )
    parser.add_argument(
        '--download-dir',
        default='osm_downloads',
        help='Directory for downloaded PBF files (default: osm_downloads)'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip downloading, only process existing files'
    )
    parser.add_argument(
        '--force-redownload',
        action='store_true',
        help='Force re-download even if files exist'
    )
    parser.add_argument(
        '--list-countries',
        action='store_true',
        help='List all available countries and exit'
    )
    
    args = parser.parse_args()
    
    # Handle --list-countries
    if args.list_countries:
        GeofabrikDownloader.list_available_countries()
        return
    
    # Validate that countries were provided
    if not args.countries:
        parser.print_help()
        print("\nError: Please specify at least one country or use --list-countries")
        sys.exit(1)
    
    downloader = GeofabrikDownloader(args.download_dir)
    
    # Download files if needed
    if not args.skip_download:
        print("=" * 60)
        print("STEP 1: DOWNLOADING OSM DATA FROM GEOFABRIK")
        print("=" * 60)
        pbf_files = downloader.download_countries(args.countries, args.force_redownload)
    else:
        # Find existing files
        pbf_files = []
        for country in args.countries:
            country = country.lower()
            if country in GeofabrikDownloader.EU_COUNTRIES:
                filename = Path(GeofabrikDownloader.EU_COUNTRIES[country]).name
                file_path = Path(args.download_dir) / filename
                if file_path.exists():
                    pbf_files.append(file_path)
                else:
                    print(f"Warning: File not found for {country}: {file_path}")
    
    if not pbf_files:
        print("\nNo PBF files to process. Exiting.")
        sys.exit(1)
    
    # Process PBF files
    print("\n" + "=" * 60)
    print("STEP 2: EXTRACTING MOTORCYCLE REPAIR SHOPS")
    print("=" * 60)
    
    all_repair_shops = []
    for pbf_file in pbf_files:
        shops = process_pbf_file(pbf_file)
        all_repair_shops.extend(shops)
    
    # Export to CSV
    print("\n" + "=" * 60)
    print("STEP 3: EXPORTING TO CSV")
    print("=" * 60)
    
    export_to_csv(all_repair_shops, args.output)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Countries processed: {len(pbf_files)}")
    print(f"Total repair shops found: {len(all_repair_shops)}")
    print(f"Output file: {Path(args.output).absolute()}")
    print("\n✓ Processing complete!")


if __name__ == '__main__':
    main()