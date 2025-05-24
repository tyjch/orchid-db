import os
import yaml
from dotenv import load_dotenv
import psycopg  # psycopg3
from abc import ABC, abstractmethod
import csv
import zipfile
import glob

# Load environment variables
load_dotenv()

def get_db_connection():
    """Get database connection using environment variables"""
    return psycopg.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'), 
        password=os.getenv('POSTGRES_PASSWORD'),
        port=os.getenv('POSTGRES_PORT', '5432')
    )

def load_plant_families_config(config_file='plant_families.yaml'):
    """Load plant families configuration from YAML file"""
    try:
        with open(config_file, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found!")
        print("Please create the YAML configuration file first.")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML configuration: {e}")
        return None

def get_families_from_config(config, include_categories=None, exclude_categories=None):
    """Extract family lists from configuration"""
    if not config:
        return {}
    
    # Categories that contain family lists (not macroalgae which has a different structure)
    family_categories = ['carnivorous_plants', 'ferns_and_allies', 'geophytes', 'aquatic_and_wetland']
    
    if include_categories:
        family_categories = [cat for cat in family_categories if cat in include_categories]
    
    if exclude_categories:
        family_categories = [cat for cat in family_categories if cat not in exclude_categories]
    
    result = {}
    all_families = []
    
    for category in family_categories:
        if category in config and 'families' in config[category]:
            families = config[category]['families']
            result[category] = families
            all_families.extend(families)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_families = []
    for family in all_families:
        if family not in seen:
            seen.add(family)
            unique_families.append(family)
    
    result['all'] = unique_families
    return result

class TaxonomicDataSource(ABC):
    """Abstract base class for taxonomic data sources"""
    
    @abstractmethod
    def find_dataset(self, family_name):
        """Find the data file for a given family"""
        pass
    
    @abstractmethod
    def extract_taxa(self, data_path, limit=None):
        """Extract taxonomic data from the source file"""
        pass
    
    @abstractmethod
    def get_source_name(self):
        """Return the name of this data source"""
        pass

class WFODarwinCoreSource(TaxonomicDataSource):
    """World Flora Online Darwin Core Archive source"""
    
    def __init__(self, base_path="datasets/families_dwc/www/downloads/dwc"):
        self.base_path = base_path
    
    def find_dataset(self, family_name):
        pattern = f"{self.base_path}/*{family_name}*.zip"
        files = glob.glob(pattern)
        return files[0] if files else None
    
    def extract_taxa(self, zip_path, limit=None, target_family=None):
        """Extract taxonomic data from a specific family"""
        taxa_data = []
        skipped_ranks = {}
        skipped_synonyms = 0
        skipped_non_target = 0
        
        with zipfile.ZipFile(zip_path, 'r') as z:
            with z.open('classification.csv') as f:
                content = f.read().decode('utf-8')
                csv_reader = csv.DictReader(content.splitlines())
                
                for i, row in enumerate(csv_reader):
                    if limit and i >= limit:
                        break
                    
                    # Skip records that don't belong to target family (if specified)
                    if target_family and row.get('family', '') != target_family:
                        skipped_non_target += 1
                        continue
                    
                    # Only skip synonyms and invalid - keep accepted, unresolved, unchecked
                    if row['taxonomicStatus'].lower() in ['synonym', 'invalid']:
                        skipped_synonyms += 1
                        continue
                    
                    # Map the rank - skip if unsupported
                    mapped_rank = self._map_rank(row['taxonRank'])
                    if mapped_rank is None:
                        skipped_rank = row['taxonRank']
                        skipped_ranks[skipped_rank] = skipped_ranks.get(skipped_rank, 0) + 1
                        continue
                    
                    taxon = {
                        'source_id': row['taxonID'],
                        'name': row['scientificName'],
                        'rank': mapped_rank,
                        'parent_source_id': row['parentNameUsageID'] if row['parentNameUsageID'] else None,
                        'status': self._map_status(row['taxonomicStatus']),
                        'source': 'WFO',
                        'family': row.get('family', '')
                    }
                    taxa_data.append(taxon)

        family_name = target_family or "Multiple families"
        print(f"Extracted {len(taxa_data)} {family_name} taxa")
        print(f"Skipped {skipped_synonyms} synonyms")
        if target_family:
            print(f"Skipped {skipped_non_target} non-{target_family} records")
        if skipped_ranks:
            print("Skipped ranks:")
            for rank, count in skipped_ranks.items():
                print(f"  {rank}: {count} records")

        return taxa_data
    
    def get_source_name(self):
        return "World Flora Online"
    
    def _map_rank(self, wfo_rank):
        """Map WFO ranks to our enum values, return None for unsupported ranks"""
        rank_mapping = {
            'family': 'family',
            'subfamily': 'subfamily',
            'tribe': 'tribe', 
            'genus': 'genus',
            'subgenus': 'subgenus',
            'section': 'section',
            'subsection': 'subsection',
            'species': 'species',
            'subspecies': 'subspecies',
            'variety': 'variety',
            'form': 'form'
        }
        return rank_mapping.get(wfo_rank.lower())
    
    def _map_status(self, wfo_status):
        status_mapping = {
            'accepted': 'accepted',
            'synonym': 'synonym',
            'invalid': 'invalid',
            'unresolved': 'unresolved'
        }
        return status_mapping.get(wfo_status.lower(), 'unresolved')

class TaxonomicImporter:
    """Main importer that can work with any data source"""
    
    def __init__(self, data_source: TaxonomicDataSource):
        self.data_source = data_source
    
    def import_family(self, family_name, conn, limit=None):
        """Import a family using the configured data source"""
        print(f"Importing {family_name} from {self.data_source.get_source_name()}")
        if limit:
            print(f"Limited to first {limit} records")
        
        # Find the dataset
        data_path = self.data_source.find_dataset(family_name)
        if not data_path:
            print(f"Dataset for {family_name} not found")
            return False
        
        # Extract the data
        taxa_data = self.data_source.extract_taxa(data_path, limit=limit, target_family=family_name)
        print(f"Extracted {len(taxa_data)} taxa")
        
        # Import to database
        return self._import_taxa_to_db(taxa_data, conn)
    
    def import_multiple_families(self, family_list, conn, limit=None):
        """Import multiple families in one operation"""
        print(f"Importing {len(family_list)} plant families:")
        for family in family_list:
            print(f"  - {family}")
        
        all_taxa = []
        successful_families = []
        failed_families = []
        
        for family_name in family_list:
            print(f"\n--- Processing {family_name} ---")
            
            # Find the dataset
            data_path = self.data_source.find_dataset(family_name)
            if not data_path:
                print(f"Dataset for {family_name} not found")
                failed_families.append(family_name)
                continue
            
            # Extract the data
            try:
                taxa_data = self.data_source.extract_taxa(data_path, limit=limit, target_family=family_name)
                if taxa_data:
                    all_taxa.extend(taxa_data)
                    successful_families.append(family_name)
                    print(f"✓ {family_name}: {len(taxa_data)} taxa")
                else:
                    print(f"✗ {family_name}: No taxa found")
                    failed_families.append(family_name)
            except Exception as e:
                print(f"✗ {family_name}: Error - {e}")
                failed_families.append(family_name)
        
        print(f"\n--- Import Summary ---")
        print(f"Total taxa collected: {len(all_taxa)}")
        print(f"Successful families ({len(successful_families)}): {', '.join(successful_families)}")
        if failed_families:
            print(f"Failed families ({len(failed_families)}): {', '.join(failed_families)}")
        
        # Import all taxa to database
        if all_taxa:
            success = self._import_taxa_to_db(all_taxa, conn)
            return success, successful_families, failed_families
        else:
            print("No taxa to import!")
            return False, successful_families, failed_families
    
    def _import_taxa_to_db(self, taxa_data, conn):
        """Database import logic with proper WFO ID handling - preserves existing data"""
        source_id_to_db_id = {}
        skipped_existing = 0
        
        try:
            with conn.cursor() as cur:
                # Pass 1: Insert all taxa using wfo_id as unique identifier, skip if already exists
                for taxon in taxa_data:
                    # Check if this WFO ID already exists
                    cur.execute("SELECT id FROM taxonomy.taxa WHERE wfo_id = %s", (taxon['source_id'],))
                    existing = cur.fetchone()
                    
                    if existing:
                        # Already exists, just map it
                        source_id_to_db_id[taxon['source_id']] = existing[0]
                        skipped_existing += 1
                    else:
                        # Insert new taxon
                        cur.execute("""
                            INSERT INTO taxonomy.taxa (name, rank, status, wfo_id) 
                            VALUES (%s, %s, %s, %s) 
                            RETURNING id
                        """, (taxon['name'], taxon['rank'], taxon['status'], taxon['source_id']))
                        
                        db_id = cur.fetchone()[0]
                        source_id_to_db_id[taxon['source_id']] = db_id
                
                inserted_count = len(taxa_data) - skipped_existing
                print(f"Pass 1: Inserted {inserted_count} new taxa, skipped {skipped_existing} existing")
                
                # Pass 2: Update parent relationships (for both new and existing taxa)
                updates_made = 0
                for taxon in taxa_data:
                    if taxon['parent_source_id'] and taxon['parent_source_id'] in source_id_to_db_id:
                        parent_db_id = source_id_to_db_id[taxon['parent_source_id']]
                        child_db_id = source_id_to_db_id[taxon['source_id']]
                        
                        # Check if parent relationship already exists
                        cur.execute("""
                            SELECT parent_id FROM taxonomy.taxa WHERE id = %s
                        """, (child_db_id,))
                        current_parent = cur.fetchone()[0]
                        
                        if current_parent != parent_db_id:
                            cur.execute("""
                                UPDATE taxonomy.taxa 
                                SET parent_id = %s 
                                WHERE id = %s
                            """, (parent_db_id, child_db_id))
                            updates_made += 1
                
                print(f"Pass 2: Updated {updates_made} parent relationships")
                conn.commit()
                return True
                
        except Exception as e:
            print(f"Error during import: {e}")
            conn.rollback()
            return False

def check_table_sizes(conn):
    """Check how much data is in each table"""
    try:
        with conn.cursor() as cur:
            tables = ['taxonomy.taxa', 'breeding.parents', 'taxonomy.valid_hierarchies']
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"{table}: {count} rows")
    except Exception as e:
        print(f"Error checking table sizes: {e}")

def display_config_summary(config):
    """Display a summary of the loaded configuration"""
    if not config:
        return
    
    print(f"\n{'='*60}")
    print("LOADED PLANT FAMILIES CONFIGURATION")
    print(f"{'='*60}")
    
    if 'metadata' in config:
        meta = config['metadata']
        print(f"Version: {meta.get('version', 'Unknown')}")
        print(f"Created: {meta.get('created', 'Unknown')}")
        print(f"Description: {meta.get('description', 'No description')}")
    
    # Display category summaries
    categories = {
        'carnivorous_plants': 'Carnivorous Plants',
        'ferns_and_allies': 'Ferns & Allies', 
        'geophytes': 'Geophytes',
        'aquatic_and_wetland': 'Aquatic & Wetland'
    }
    
    for key, name in categories.items():
        if key in config and 'families' in config[key]:
            count = len(config[key]['families'])
            desc = config[key].get('description', 'No description')
            print(f"\n{name}: {count} families")
            print(f"  {desc}")
    
    # Macroalgae note
    if 'macroalgae' in config:
        print(f"\nMacroalgae: Included for reference")
        print(f"  {config['macroalgae']['note']}")

if __name__ == "__main__":
    try:
        # Load configuration
        config = load_plant_families_config('plant_families.yaml')
        if not config:
            exit(1)
        
        # Display configuration summary
        display_config_summary(config)
        
        # Get database connection
        conn = get_db_connection()
        print(f"\n{'='*60}")
        print("DATABASE CONNECTION ESTABLISHED")
        print(f"{'='*60}")
        
        print("Preserving existing taxonomy data (including orchids)")
        print("Adding new plant families to existing data...")
        
        print(f"\nChecking current database contents:")
        check_table_sizes(conn)
        
        # Get families to import (excluding macroalgae as they won't be in WFO)
        families_config = get_families_from_config(config)
        
        if not families_config:
            print("No families found in configuration!")
            exit(1)
        
        print(f"\nTarget families by category:")
        for category, families in families_config.items():
            if category != 'all':
                cat_name = category.replace('_', ' ').title()
                print(f"  {cat_name}: {len(families)} families")
        
        print(f"  Total unique families: {len(families_config['all'])}")
        
        print(f"\nAll target families:")
        for i, family in enumerate(families_config['all'], 1):
            print(f"{i:2d}. {family}")
        
        # Use WFO data source
        wfo_source = WFODarwinCoreSource()
        importer = TaxonomicImporter(wfo_source)
        
        # Import all target families
        print(f"\n{'='*60}")
        print("STARTING COMPREHENSIVE PLANT FAMILY IMPORT")
        print("(Based on YAML configuration)")
        print(f"{'='*60}")
        
        success, successful_families, failed_families = importer.import_multiple_families(
            families_config['all'], 
            conn
        )
        
        print(f"\n{'='*60}")
        print("COMPREHENSIVE PLANT IMPORT RESULTS")
        print(f"{'='*60}")
        
        if success:
            print("✓ Database import completed successfully!")
            print(f"✓ Successfully imported {len(successful_families)} families:")
            
            # Categorize successful families
            carnivorous_success = [f for f in successful_families if f in families_config.get('carnivorous_plants', [])]
            fern_success = [f for f in successful_families if f in families_config.get('ferns_and_allies', [])]
            geophyte_success = [f for f in successful_families if f in families_config.get('geophytes', [])]
            aquatic_success = [f for f in successful_families if f in families_config.get('aquatic_and_wetland', [])]
            
            if carnivorous_success:
                print(f"\n  Carnivorous Plants ({len(carnivorous_success)}):")
                for family in carnivorous_success:
                    print(f"    ✓ {family}")
            
            if fern_success:
                print(f"\n  Ferns & Allies ({len(fern_success)}):")
                for family in fern_success:
                    print(f"    ✓ {family}")
            
            if geophyte_success:
                print(f"\n  Geophytes ({len(geophyte_success)}):")
                for family in geophyte_success:
                    print(f"    ✓ {family}")
            
            if aquatic_success:
                print(f"\n  Aquatic & Wetland Plants ({len(aquatic_success)}):")
                for family in aquatic_success:
                    print(f"    ✓ {family}")
        else:
            print("✗ Database import failed!")
        
        if failed_families:
            print(f"\n⚠ Failed to find/import {len(failed_families)} families:")
            for family in failed_families:
                print(f"  ✗ {family}")
            print("\nThese families may not be available in your WFO dataset,")
            print("or may be included within other family files.")
            print("Some fern families might not be in WFO as it focuses on flowering plants.")
        
        print(f"\nFinal database contents:")
        check_table_sizes(conn)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("\nDatabase connection closed.")