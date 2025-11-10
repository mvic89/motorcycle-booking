#!/usr/bin/env python3
"""
Migrate Motorcycle Directory Data to Supabase
==============================================
Uploads your directory_data.json to Supabase database.

Installation:
    pip install supabase python-dotenv

Usage:
    # Set environment variables first (recommended)
    export SUPABASE_URL="https://your-project.supabase.co"
    export SUPABASE_KEY="your-service-role-key"
    
    python migrate_to_supabase.py
    
    # Or pass credentials directly (less secure)
    python migrate_to_supabase.py --url "https://..." --key "..."
"""

import json
import os
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Optional
from collections import defaultdict

try:
    from supabase import create_client, Client
except ImportError:
    print("❌ Error: supabase-py not installed")
    print("   Install with: pip install supabase")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    # Load environment variables from .env file
    load_dotenv()
except ImportError:
    print("⚠️  Warning: python-dotenv not installed")
    print("   Install with: pip install python-dotenv")
    print("   Or provide credentials via command line arguments")


class SupabaseMigrator:
    """Handle migration of motorcycle shop data to Supabase"""
    
    def __init__(self, supabase_url: str, supabase_key: str):
        """Initialize Supabase client"""
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.stats = {
            'shops_migrated': 0,
            'shops_failed': 0,
            'countries_created': 0,
            'cities_created': 0,
        }
    
    def load_json_data(self, json_path: str) -> Dict:
        """Load directory_data.json"""
        print(f"📖 Loading data from {json_path}...")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        shops = data.get('shops', [])
        countries = data.get('countries', {})
        
        print(f"   ✓ Found {len(shops)} shops across {len(countries)} countries")
        
        return data
    
    def transform_shop(self, shop: Dict) -> Optional[Dict]:
        """Transform shop data to match Supabase schema"""
        
        try:
            # Extract city and country from "City, Country" format
            city_field = shop.get('city', '')
            if ', ' in city_field:
                parts = city_field.split(', ')
                city_name = parts[0].strip()
                country_name = parts[-1].strip()
            else:
                city_name = None
                country_name = None
            
            # Clean up "Unknown City" and "Unknown Country"
            if city_name == 'Unknown City':
                city_name = None
            if country_name == 'Unknown Country':
                country_name = None
            
            # Parse address
            address = shop.get('address', '')
            if address and address != 'Address not available':
                address_parts = address.split(', ')
                street = address_parts[0] if len(address_parts) > 0 else None
            else:
                street = None
            
            # Clean phone/website
            phone = shop.get('phone')
            if phone == 'N/A':
                phone = None
            
            website = shop.get('website')
            if website == 'N/A':
                website = None
            
            # Clean hours
            hours = shop.get('hours')
            if hours == 'Hours not available':
                hours = None
            
            return {
                'name': shop.get('name'),
                'business_type': shop.get('business_type', 'Motorcycle Shop'),
                'latitude': float(shop.get('latitude')) if shop.get('latitude') else None,
                'longitude': float(shop.get('longitude')) if shop.get('longitude') else None,
                'address_street': street,
                'address_city': city_name,
                'address_country': country_name,
                'phone': phone,
                'website': website,
                'opening_hours': hours,
            }
        
        except Exception as e:
            print(f"   ⚠️  Error transforming shop '{shop.get('name', 'Unknown')}': {e}")
            return None
    
    def clear_existing_data(self):
        """Clear existing data (optional - use with caution!)"""
        print("\n🗑️  Clearing existing data...")
        
        try:
            # Delete all shops
            self.supabase.table('shops').delete().neq('id', 0).execute()
            print("   ✓ Cleared shops table")
            
            # Delete all cities
            self.supabase.table('cities').delete().neq('id', 0).execute()
            print("   ✓ Cleared cities table")
            
            # Delete all countries
            self.supabase.table('countries').delete().neq('id', 0).execute()
            print("   ✓ Cleared countries table")
            
        except Exception as e:
            print(f"   ⚠️  Error clearing data: {e}")
    
    def migrate_countries_and_cities(self, data: Dict):
        """Create countries and cities lookup tables"""
        print("\n🌍 Migrating countries and cities...")
        
        countries_data = data.get('countries', {})
        country_map = {}  # name -> id mapping
        
        # Create countries
        for country_name in sorted(countries_data.keys()):
            try:
                response = self.supabase.table('countries').insert({
                    'name': country_name,
                    'code': self._country_to_code(country_name),
                    'shop_count': 0  # Will update later
                }).execute()
                
                country_id = response.data[0]['id']
                country_map[country_name] = country_id
                self.stats['countries_created'] += 1
                
            except Exception as e:
                print(f"   ⚠️  Error creating country '{country_name}': {e}")
                continue
        
        print(f"   ✓ Created {self.stats['countries_created']} countries")
        
        # Create cities
        for country_name, cities in countries_data.items():
            country_id = country_map.get(country_name)
            if not country_id:
                continue
            
            for city_name in cities:
                if city_name == 'Unknown City':
                    continue
                
                try:
                    self.supabase.table('cities').insert({
                        'name': city_name,
                        'country_id': country_id,
                        'shop_count': 0  # Will update later
                    }).execute()
                    
                    self.stats['cities_created'] += 1
                    
                except Exception as e:
                    # City might already exist - that's okay
                    pass
        
        print(f"   ✓ Created {self.stats['cities_created']} cities")
        
        return country_map
    
    def migrate_shops(self, shops: List[Dict], batch_size: int = 100):
        """Migrate shops to Supabase in batches"""
        print(f"\n🏍️  Migrating {len(shops)} shops...")
        
        # Transform all shops first
        transformed_shops = []
        for shop in shops:
            transformed = self.transform_shop(shop)
            if transformed and transformed['latitude'] and transformed['longitude']:
                transformed_shops.append(transformed)
            else:
                self.stats['shops_failed'] += 1
        
        print(f"   ✓ Prepared {len(transformed_shops)} valid shops")
        if self.stats['shops_failed'] > 0:
            print(f"   ⚠️  Skipped {self.stats['shops_failed']} shops (missing required data)")
        
        # Upload in batches
        total_batches = (len(transformed_shops) + batch_size - 1) // batch_size
        
        for i in range(0, len(transformed_shops), batch_size):
            batch = transformed_shops[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                response = self.supabase.table('shops').insert(batch).execute()
                self.stats['shops_migrated'] += len(batch)
                print(f"   ✓ Batch {batch_num}/{total_batches} uploaded ({self.stats['shops_migrated']}/{len(transformed_shops)})")
                
            except Exception as e:
                print(f"   ✗ Batch {batch_num} failed: {e}")
                continue
    
    def update_counts(self):
        """Update shop counts in countries and cities tables"""
        print("\n📊 Updating statistics...")
        
        try:
            # Update country counts
            self.supabase.rpc('update_country_counts').execute()
            
            # Update city counts
            self.supabase.rpc('update_city_counts').execute()
            
            print("   ✓ Statistics updated")
            
        except Exception as e:
            print(f"   ⚠️  Error updating statistics: {e}")
            print("   (You can update these manually in Supabase SQL Editor)")
    
    def print_summary(self):
        """Print migration summary"""
        print("\n" + "=" * 70)
        print("MIGRATION SUMMARY")
        print("=" * 70)
        print(f"✅ Shops migrated:      {self.stats['shops_migrated']}")
        print(f"❌ Shops failed:        {self.stats['shops_failed']}")
        print(f"🌍 Countries created:   {self.stats['countries_created']}")
        print(f"🏙️  Cities created:      {self.stats['cities_created']}")
        print("\n✅ Migration complete!")
    
    @staticmethod
    def _country_to_code(country_name: str) -> str:
        """Convert country name to ISO code"""
        codes = {
            'Austria': 'AT', 'Belgium': 'BE', 'Bulgaria': 'BG',
            'Croatia': 'HR', 'Cyprus': 'CY', 'Czech Republic': 'CZ',
            'Denmark': 'DK', 'Estonia': 'EE', 'Finland': 'FI',
            'France': 'FR', 'Germany': 'DE', 'Greece': 'GR',
            'Hungary': 'HU', 'Ireland': 'IE', 'Italy': 'IT',
            'Latvia': 'LV', 'Lithuania': 'LT', 'Luxembourg': 'LU',
            'Malta': 'MT', 'Netherlands': 'NL', 'Poland': 'PL',
            'Portugal': 'PT', 'Romania': 'RO', 'Slovakia': 'SK',
            'Slovenia': 'SI', 'Spain': 'ES', 'Sweden': 'SE',
        }
        return codes.get(country_name, 'XX')


def main():
    parser = argparse.ArgumentParser(
        description='Migrate motorcycle shop data to Supabase',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        '--url',
        help='Supabase project URL (or set SUPABASE_URL env var)',
        default=os.getenv('SUPABASE_URL')
    )
    parser.add_argument(
        '--key',
        help='Supabase service role key (or set SUPABASE_KEY env var)',
        default=os.getenv('SUPABASE_KEY')
    )
    parser.add_argument(
        '--json',
        default='data/directory_data.json',
        help='Path to directory_data.json (default: data/directory_data.json)'
    )
    parser.add_argument(
        '--clear',
        action='store_true',
        help='Clear existing data before migration (CAUTION!)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of shops per batch (default: 100)'
    )
    
    args = parser.parse_args()
    
    # Validate credentials
    if not args.url or not args.key:
        print("❌ Error: Supabase credentials not provided")
        print("\nPlease provide credentials via:")
        print("  1. Environment variables:")
        print("     export SUPABASE_URL='https://your-project.supabase.co'")
        print("     export SUPABASE_KEY='your-service-role-key'")
        print("\n  2. Command line arguments:")
        print("     python migrate_to_supabase.py --url '...' --key '...'")
        sys.exit(1)
    
    # Check if JSON file exists
    json_path = Path(args.json)
    if not json_path.exists():
        print(f"❌ Error: File not found: {json_path}")
        print("\nMake sure you've run convert_osm_to_json.py first to generate directory_data.json")
        sys.exit(1)
    
    # Start migration
    print("=" * 70)
    print("SUPABASE MIGRATION TOOL")
    print("=" * 70)
    print(f"\n📂 Source: {json_path.absolute()}")
    print(f"🌐 Target: {args.url}")
    
    if args.clear:
        print("\n⚠️  WARNING: This will DELETE all existing data in Supabase!")
        response = input("   Type 'yes' to continue: ")
        if response.lower() != 'yes':
            print("   Migration cancelled.")
            sys.exit(0)
    
    try:
        # Initialize migrator
        migrator = SupabaseMigrator(args.url, args.key)
        
        # Load data
        data = migrator.load_json_data(str(json_path))
        
        # Clear existing data if requested
        if args.clear:
            migrator.clear_existing_data()
        
        # Migrate countries and cities
        migrator.migrate_countries_and_cities(data)
        
        # Migrate shops
        migrator.migrate_shops(data['shops'], args.batch_size)
        
        # Update counts
        migrator.update_counts()
        
        # Print summary
        migrator.print_summary()
        
        print(f"\n🌐 View your data at: {args.url}")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()