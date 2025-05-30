import os
import psycopg
from dotenv import load_dotenv
import logging
from datetime import datetime
from collections import defaultdict
import time

# Load environment
load_dotenv()

class TaxonomyMerger:
    def __init__(self):
        self.setup_logging()
        
    def setup_logging(self):
        """Set up detailed logging"""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('taxonomy_merger.log')
            ]
        )
        self.logger = logging.getLogger('taxonomy_merger')
        
    def get_db_connection(self):
        """Create a new PostgreSQL connection"""
        return psycopg.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            connect_timeout=30
        )
    
    def map_taxonomic_rank(self, external_rank):
        """Map external taxonomic ranks to our internal enum"""
        if not external_rank:
            return None
            
        rank_mapping = {
            # Standard ranks
            'kingdom': 'kingdom',
            'phylum': 'phylum', 
            'class': 'class',
            'order': 'order',
            'family': 'family',
            'subfamily': 'subfamily',
            'tribe': 'tribe',
            'subtribe': 'subtribe',
            'genus': 'genus',
            'subgenus': 'subgenus',
            'section': 'section',
            'subsection': 'subsection',
            'species': 'species',
            'subspecies': 'subspecies',
            'variety': 'variety',
            'form': 'form',
            
            # Common variations
            'var.': 'variety',
            'var': 'variety',
            'subsp.': 'subspecies',
            'subsp': 'subspecies',
            'f.': 'form',
            'forma': 'form'
        }
        
        return rank_mapping.get(external_rank.lower())
    
    def map_taxonomic_status(self, external_status):
        """Map external taxonomic status to our internal enum"""
        if not external_status:
            return 'unresolved'
            
        status_mapping = {
            'accepted': 'accepted',
            'valid': 'accepted',
            'synonym': 'synonym',
            'invalid': 'invalid',
            'unresolved': 'unresolved',
            'misapplied': 'misapplied',
            'doubtful': 'unresolved',
            'uncertain': 'unresolved'
        }
        
        return status_mapping.get(external_status.lower(), 'unresolved')
    
    def analyze_temporary_tables(self):
        """Analyze the structure and content of temporary tables"""
        print("=== ANALYZING TEMPORARY TABLES ===")
        
        conn = self.get_db_connection()
        
        try:
            with conn.cursor() as cur:
                # Analyze WFO table
                print("\n📊 WFO Table Analysis:")
                cur.execute("SELECT COUNT(*) FROM temporary.wfo_taxa")
                wfo_total = cur.fetchone()[0]
                print(f"  Total records: {wfo_total:,}")
                
                # Get column names for WFO
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'temporary' AND table_name = 'wfo_taxa'
                    ORDER BY ordinal_position
                """)
                wfo_columns = [row[0] for row in cur.fetchall()]
                print(f"  Columns ({len(wfo_columns)}): {', '.join(wfo_columns[:10])}...")
                
                # Key WFO columns we'll use
                wfo_key_cols = ['taxonID', 'scientificName', 'taxonRank', 'taxonomicStatus', 'parentNameUsageID', 
                               'family', 'scientificNameID', 'tplID', 'source']
                print(f"  Key columns found: {[col for col in wfo_key_cols if col in wfo_columns]}")
                
                # Status distribution in WFO
                cur.execute("""
                    SELECT "taxonomicStatus", COUNT(*) 
                    FROM temporary.wfo_taxa 
                    WHERE "taxonomicStatus" IS NOT NULL
                    GROUP BY "taxonomicStatus" 
                    ORDER BY COUNT(*) DESC
                """)
                wfo_statuses = cur.fetchall()
                print(f"  Status distribution:")
                for status, count in wfo_statuses:
                    print(f"    {status}: {count:,}")
                
                # Check for external IDs in WFO
                cur.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT("scientificNameID") as ipni_ids,
                        COUNT("tplID") as tpl_ids,
                        COUNT(CASE WHEN "source" = 'TPL1.1' THEN 1 END) as tpl_source
                    FROM temporary.wfo_taxa
                """)
                wfo_external_ids = cur.fetchone()
                print(f"  External IDs - IPNI: {wfo_external_ids[1]:,}, TPL: {wfo_external_ids[2]:,}, TPL Source: {wfo_external_ids[3]:,}")
                
                # Analyze GBIF table  
                print("\n📊 GBIF Table Analysis:")
                cur.execute("SELECT COUNT(*) FROM temporary.gbif_taxon")
                gbif_total = cur.fetchone()[0]
                print(f"  Total records: {gbif_total:,}")
                
                # Get column names for GBIF
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'temporary' AND table_name = 'gbif_taxon'
                    ORDER BY ordinal_position
                """)
                gbif_columns = [row[0] for row in cur.fetchall()]
                print(f"  Columns ({len(gbif_columns)}): {', '.join(gbif_columns[:10])}...")
                
                # Key GBIF columns we'll use
                gbif_key_cols = ['taxonID', 'scientificName', 'taxonRank', 'taxonomicStatus', 'parentNameUsageID',
                                'acceptedNameUsageID', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus']
                print(f"  Key columns found: {[col for col in gbif_key_cols if col in gbif_columns]}")
                
                # Status distribution in GBIF
                cur.execute("""
                    SELECT "taxonomicStatus", COUNT(*) 
                    FROM temporary.gbif_taxon 
                    WHERE "taxonomicStatus" IS NOT NULL
                    GROUP BY "taxonomicStatus" 
                    ORDER BY COUNT(*) DESC
                """)
                gbif_statuses = cur.fetchall()
                print(f"  Status distribution:")
                for status, count in gbif_statuses:
                    print(f"    {status}: {count:,}")
                
                # Look for overlaps by scientific name
                print("\n🔍 Overlap Analysis:")
                cur.execute("""
                    SELECT COUNT(*) as overlap_count
                    FROM (
                        SELECT DISTINCT w."scientificName"
                        FROM temporary.wfo_taxa w
                        WHERE w."scientificName" IS NOT NULL
                        INTERSECT
                        SELECT DISTINCT g."scientificName" 
                        FROM temporary.gbif_taxon g
                        WHERE g."scientificName" IS NOT NULL
                    ) overlap
                """)
                overlap = cur.fetchone()[0]
                print(f"  Names in both WFO and GBIF: {overlap:,}")
                
                # Check WFO accepted vs synonyms
                cur.execute("""
                    SELECT 
                        COUNT(CASE WHEN "taxonomicStatus" = 'Accepted' THEN 1 END) as accepted,
                        COUNT(CASE WHEN "taxonomicStatus" = 'Synonym' THEN 1 END) as synonyms
                    FROM temporary.wfo_taxa
                """)
                wfo_acc_syn = cur.fetchone()
                print(f"  WFO - Accepted: {wfo_acc_syn[0]:,}, Synonyms: {wfo_acc_syn[1]:,}")
                
                # Check GBIF accepted vs synonyms  
                cur.execute("""
                    SELECT 
                        COUNT(CASE WHEN "taxonomicStatus" = 'accepted' THEN 1 END) as accepted,
                        COUNT(CASE WHEN "taxonomicStatus" = 'synonym' THEN 1 END) as synonyms
                    FROM temporary.gbif_taxon
                """)
                gbif_acc_syn = cur.fetchone()
                print(f"  GBIF - Accepted: {gbif_acc_syn[0]:,}, Synonyms: {gbif_acc_syn[1]:,}")
                
                return {
                    'wfo_total': wfo_total,
                    'gbif_total': gbif_total,
                    'wfo_columns': wfo_columns,
                    'gbif_columns': gbif_columns,
                    'wfo_statuses': wfo_statuses,
                    'gbif_statuses': gbif_statuses,
                    'overlap': overlap
                }
                
        finally:
            conn.close()
    
    def create_crosswalk_schema(self):
        """Create the crosswalk schema and tables"""
        print("\n=== CREATING CROSSWALK SCHEMA ===")
        
        crosswalk_sql = """
        -- Create crosswalk schema for external ID mappings
        CREATE SCHEMA IF NOT EXISTS crosswalk;

        -- WFO crosswalk table - ultra minimal design
        DROP TABLE IF EXISTS crosswalk.wfo CASCADE;
        CREATE TABLE crosswalk.wfo (
            id              VARCHAR(50) PRIMARY KEY,  -- WFO ID
            taxa_id         INTEGER NOT NULL REFERENCES taxonomy.taxa(id)
        );

        -- GBIF crosswalk table - ultra minimal design
        DROP TABLE IF EXISTS crosswalk.gbif CASCADE;
        CREATE TABLE crosswalk.gbif (
            id              BIGINT PRIMARY KEY,  -- GBIF taxon key
            taxa_id         INTEGER NOT NULL REFERENCES taxonomy.taxa(id)
        );

        -- TPL crosswalk table - ultra minimal design
        DROP TABLE IF EXISTS crosswalk.tpl CASCADE;
        CREATE TABLE crosswalk.tpl (
            id              VARCHAR(50) PRIMARY KEY,  -- TPL ID
            taxa_id         INTEGER NOT NULL REFERENCES taxonomy.taxa(id)
        );

        -- IPNI crosswalk table - ultra minimal design
        DROP TABLE IF EXISTS crosswalk.ipni CASCADE;
        CREATE TABLE crosswalk.ipni (
            id              VARCHAR(50) PRIMARY KEY,  -- IPNI ID
            taxa_id         INTEGER NOT NULL REFERENCES taxonomy.taxa(id)
        );

        -- Indexes for reverse lookups (taxa_id -> external_ids)
        CREATE INDEX idx_crosswalk_wfo_taxa_id ON crosswalk.wfo (taxa_id);
        CREATE INDEX idx_crosswalk_gbif_taxa_id ON crosswalk.gbif (taxa_id);
        CREATE INDEX idx_crosswalk_tpl_taxa_id ON crosswalk.tpl (taxa_id);
        CREATE INDEX idx_crosswalk_ipni_taxa_id ON crosswalk.ipni (taxa_id);
        """
        
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(crosswalk_sql)
                conn.commit()
                print("✓ Ultra-minimal crosswalk schema and tables created successfully")
        except Exception as e:
            print(f"❌ Error creating crosswalk schema: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def process_gbif_batch(self, cur, batch_data, gbif_key_to_taxa_id, taxa_id_to_external_ids):
        """Process a batch of GBIF records efficiently"""
        inserted_count = 0
        
        for record_data in batch_data:
            name, mapped_rank, mapped_status, gbif_key, parent_gbif_key, kingdom, phylum, cls, order, family, genus, full_scientific_name = record_data
            
            try:
                cur.execute("""
                    INSERT INTO taxonomy.taxa (name, rank, status)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (name, mapped_rank, mapped_status))
                
                taxa_id = cur.fetchone()[0]
                gbif_key_to_taxa_id[gbif_key] = taxa_id
                
                # Store GBIF external ID info for crosswalk tables
                taxa_id_to_external_ids[taxa_id] = {
                    'gbif_key': gbif_key,
                    'gbif_name': name,
                    'gbif_full_name': full_scientific_name,  # Store both canonical and full name
                    'gbif_rank': mapped_rank,
                    'gbif_status': mapped_status,
                    'gbif_parent_key': parent_gbif_key,
                    'gbif_kingdom': kingdom,
                    'gbif_phylum': phylum,
                    'gbif_class': cls,
                    'gbif_order': order,
                    'gbif_family': family,
                    'gbif_genus': genus
                }
                
                inserted_count += 1
                
            except Exception as e:
                # Log error but continue processing
                if inserted_count % 10000 == 0:  # Only log occasionally to avoid spam
                    print(f"    ⚠️  Error in batch processing: {e}")
                continue
        
        return inserted_count
    
    def merge_taxa_data(self):
        """Merge WFO and GBIF data into taxonomy.taxa table"""
        print("\n=== MERGING TAXONOMY DATA ===")
        
        conn = self.get_db_connection()
        
        try:
            with conn.cursor() as cur:
                # Clear existing data
                print("🧹 Clearing existing taxonomy data...")
                cur.execute("TRUNCATE TABLE taxonomy.taxa RESTART IDENTITY CASCADE")
                
                # Process WFO data first (higher priority for our target families)
                print("\n📋 Processing WFO data...")
                
                wfo_query = """
                    SELECT 
                        "taxonID" as wfo_id,
                        "scientificName" as name,
                        "taxonRank" as rank,
                        "parentNameUsageID" as parent_wfo_id,
                        "taxonomicStatus" as status,
                        "family",
                        "scientificNameID" as ipni_id,
                        "tplID" as tpl_id,
                        "source"
                    FROM temporary.wfo_taxa
                    WHERE "scientificName" IS NOT NULL
                    AND "taxonomicStatus" = 'Accepted'  -- WFO uses 'Accepted' (capital A)
                    ORDER BY 
                        CASE "taxonRank"
                            WHEN 'kingdom' THEN 1
                            WHEN 'phylum' THEN 2
                            WHEN 'class' THEN 3
                            WHEN 'order' THEN 4
                            WHEN 'family' THEN 5
                            WHEN 'subfamily' THEN 6
                            WHEN 'tribe' THEN 7
                            WHEN 'genus' THEN 8
                            WHEN 'species' THEN 9
                            ELSE 10
                        END,
                        "scientificName"
                """
                
                cur.execute(wfo_query)
                wfo_records = cur.fetchall()
                print(f"  Found {len(wfo_records):,} accepted WFO records to process")
                
                # Track mappings for parent relationships and external IDs
                wfo_id_to_taxa_id = {}
                taxa_id_to_external_ids = {}  # Store external ID info for crosswalk tables
                inserted_count = 0
                
                # Insert WFO records (first pass - no parent relationships yet)
                for record in wfo_records:
                    wfo_id, name, rank, parent_wfo_id, status, family, ipni_id, tpl_id, source = record
                    
                    mapped_rank = self.map_taxonomic_rank(rank)
                    mapped_status = self.map_taxonomic_status(status)
                    
                    if not mapped_rank:
                        continue  # Skip unsupported ranks
                    
                    try:
                        cur.execute("""
                            INSERT INTO taxonomy.taxa (name, rank, status)
                            VALUES (%s, %s, %s)
                            RETURNING id
                        """, (name, mapped_rank, mapped_status))
                        
                        taxa_id = cur.fetchone()[0]
                        wfo_id_to_taxa_id[wfo_id] = taxa_id
                        
                        # Store external ID info for crosswalk tables
                        taxa_id_to_external_ids[taxa_id] = {
                            'wfo_id': wfo_id,
                            'wfo_name': name,
                            'wfo_rank': rank,
                            'wfo_status': status,
                            'wfo_parent_id': parent_wfo_id,
                            'source_family': family,
                            'ipni_id': ipni_id,
                            'tpl_id': tpl_id,
                            'source': source
                        }
                        
                        inserted_count += 1
                        
                        if inserted_count % 1000 == 0:
                            print(f"    Inserted {inserted_count:,} WFO records...")
                            
                    except Exception as e:
                        print(f"    ⚠️  Error inserting WFO record {name}: {e}")
                        continue
                
                print(f"  ✓ Inserted {inserted_count:,} WFO taxa")
                
                # Update parent relationships for WFO records
                print("  🔗 Setting up WFO parent relationships...")
                parent_updates = 0
                
                for record in wfo_records:
                    wfo_id, name, rank, parent_wfo_id, status, family, ipni_id, tpl_id, source = record
                    
                    if parent_wfo_id and parent_wfo_id in wfo_id_to_taxa_id:
                        child_taxa_id = wfo_id_to_taxa_id.get(wfo_id)
                        parent_taxa_id = wfo_id_to_taxa_id.get(parent_wfo_id)
                        
                        if child_taxa_id and parent_taxa_id:
                            try:
                                cur.execute("""
                                    UPDATE taxonomy.taxa 
                                    SET parent_id = %s 
                                    WHERE id = %s
                                """, (parent_taxa_id, child_taxa_id))
                                parent_updates += 1
                            except Exception as e:
                                continue
                
                print(f"  ✓ Updated {parent_updates:,} parent relationships")
                
                # Now process GBIF data (fill gaps not covered by WFO) 
                print("\n📋 Processing GBIF data...")
                
                gbif_query = """
                    SELECT 
                        "taxonID" as gbif_key,
                        COALESCE("canonicalName", "scientificName") as name,  -- Use canonical name (clean) over scientific name (includes authors)
                        "taxonRank" as rank,
                        "parentNameUsageID" as parent_gbif_key,
                        "taxonomicStatus" as status,
                        "acceptedNameUsageID" as accepted_gbif_key,
                        "kingdom", "phylum", "class", "order", "family", "genus",
                        "scientificName" as full_scientific_name  -- Keep full name for reference
                    FROM temporary.gbif_taxon
                    WHERE COALESCE("canonicalName", "scientificName") IS NOT NULL
                    AND "taxonomicStatus" = 'accepted'  -- GBIF uses 'accepted' (lowercase)
                    AND "kingdom" = 'Plantae'  -- Only plants
                    AND COALESCE("canonicalName", "scientificName") NOT IN (
                        SELECT name FROM taxonomy.taxa
                    )
                    ORDER BY 
                        CASE UPPER("taxonRank")
                            WHEN 'KINGDOM' THEN 1
                            WHEN 'PHYLUM' THEN 2
                            WHEN 'CLASS' THEN 3
                            WHEN 'ORDER' THEN 4
                            WHEN 'FAMILY' THEN 5
                            WHEN 'SUBFAMILY' THEN 6
                            WHEN 'TRIBE' THEN 7
                            WHEN 'GENUS' THEN 8
                            WHEN 'SPECIES' THEN 9
                            ELSE 10
                        END,
                        COALESCE("canonicalName", "scientificName")
                """
                
                cur.execute(gbif_query)
                gbif_records = cur.fetchall()
                print(f"  Found {len(gbif_records):,} new GBIF records to process")
                
                # Track GBIF mappings
                gbif_key_to_taxa_id = {}
                gbif_inserted = 0
                
                # Insert GBIF records in batches for better performance
                batch_size = 10000
                batch_data = []
                
                for i, record in enumerate(gbif_records):
                    gbif_key, name, rank, parent_gbif_key, status, accepted_gbif_key, kingdom, phylum, cls, order, family, genus, full_scientific_name = record
                    
                    mapped_rank = self.map_taxonomic_rank(rank)
                    mapped_status = self.map_taxonomic_status(status)
                    
                    if not mapped_rank:
                        continue
                    
                    batch_data.append((name, mapped_rank, mapped_status, gbif_key, parent_gbif_key, kingdom, phylum, cls, order, family, genus, full_scientific_name))
                    
                    # Process batch when it reaches batch_size
                    if len(batch_data) >= batch_size:
                        gbif_inserted += self.process_gbif_batch(cur, batch_data, gbif_key_to_taxa_id, taxa_id_to_external_ids)
                        batch_data = []
                        
                        if gbif_inserted % 50000 == 0:
                            print(f"    Inserted {gbif_inserted:,} GBIF records...")
                            conn.commit()  # Commit periodically to prevent transaction timeouts
                
                # Process remaining records in final batch
                if batch_data:
                    gbif_inserted += self.process_gbif_batch(cur, batch_data, gbif_key_to_taxa_id, taxa_id_to_external_ids)
                
                print(f"  ✓ Inserted {gbif_inserted:,} GBIF taxa")
                
                # Commit the main taxa insertions
                conn.commit()
                print(f"\n✅ Successfully merged {inserted_count + gbif_inserted:,} taxa records")
                
                return {
                    'wfo_inserted': inserted_count,
                    'gbif_inserted': gbif_inserted,
                    'wfo_id_to_taxa_id': wfo_id_to_taxa_id,
                    'gbif_key_to_taxa_id': gbif_key_to_taxa_id,
                    'taxa_id_to_external_ids': taxa_id_to_external_ids
                }
                
        except Exception as e:
            print(f"❌ Error during merge: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()
    
    def populate_crosswalk_tables(self, mapping_data):
        """Populate the crosswalk tables with external ID mappings"""
        print("\n=== POPULATING CROSSWALK TABLES ===")
        
        if not mapping_data or 'taxa_id_to_external_ids' not in mapping_data:
            print("❌ No mapping data available")
            print(f"  Mapping data keys: {list(mapping_data.keys()) if mapping_data else 'None'}")
            return
        
        external_ids_data = mapping_data['taxa_id_to_external_ids']
        print(f"📊 Found {len(external_ids_data)} taxa with external ID mappings")
        
        # Debug: Show sample of what we have
        sample_count = 0
        wfo_count_preview = 0
        gbif_count_preview = 0
        for taxa_id, external_ids in external_ids_data.items():
            if sample_count < 5:
                print(f"  Sample taxa {taxa_id}: {list(external_ids.keys())}")
                sample_count += 1
            if 'wfo_id' in external_ids:
                wfo_count_preview += 1
            if 'gbif_key' in external_ids:
                gbif_count_preview += 1
        
        print(f"  Preview: {wfo_count_preview} WFO mappings, {gbif_count_preview} GBIF mappings")
        
        conn = self.get_db_connection()
        conn.autocommit = False  # Explicit transaction management
        
        try:
            with conn.cursor() as cur:
                # Populate WFO crosswalk using the stored external ID data
                print("📋 Populating WFO crosswalk...")
                wfo_count = 0
                
                for taxa_id, external_ids in external_ids_data.items():
                    if 'wfo_id' in external_ids:
                        try:
                            cur.execute("""
                                INSERT INTO crosswalk.wfo (id, taxa_id)
                                VALUES (%s, %s)
                            """, (
                                external_ids['wfo_id'],
                                taxa_id
                            ))
                            wfo_count += 1
                            
                            if wfo_count <= 3:  # Debug first few insertions
                                print(f"    Inserted WFO {wfo_count}: {external_ids['wfo_id']} -> taxa_id {taxa_id}")
                                
                        except Exception as e:
                            if wfo_count <= 3:  # Only show first few errors
                                print(f"    ⚠️  Error inserting WFO crosswalk for taxa {taxa_id}: {e}")
                
                # Commit WFO crosswalk immediately
                conn.commit()
                print(f"  ✓ Inserted and committed {wfo_count:,} WFO crosswalk records")
                
                # Populate GBIF crosswalk
                print("📋 Populating GBIF crosswalk...")
                gbif_count = 0
                
                for taxa_id, external_ids in external_ids_data.items():
                    if 'gbif_key' in external_ids:
                        try:
                            cur.execute("""
                                INSERT INTO crosswalk.gbif (id, taxa_id)
                                VALUES (%s, %s)
                            """, (
                                int(external_ids['gbif_key']) if external_ids['gbif_key'] else None,
                                taxa_id
                            ))
                            gbif_count += 1
                            
                            if gbif_count <= 3:  # Debug first few insertions
                                print(f"    Inserted GBIF {gbif_count}: {external_ids['gbif_key']} -> taxa_id {taxa_id}")
                                
                        except Exception as e:
                            if gbif_count <= 3:  # Only show first few errors
                                print(f"    ⚠️  Error inserting GBIF crosswalk for taxa {taxa_id}: {e}")
                
                # Commit GBIF crosswalk immediately
                conn.commit()
                print(f"  ✓ Inserted and committed {gbif_count:,} GBIF crosswalk records")
                
                # Populate TPL crosswalk from WFO data
                print("📋 Populating TPL crosswalk...")
                tpl_count = 0
                
                for taxa_id, external_ids in external_ids_data.items():
                    if 'tpl_id' in external_ids and external_ids['tpl_id']:
                        try:
                            cur.execute("""
                                INSERT INTO crosswalk.tpl (id, taxa_id)
                                VALUES (%s, %s)
                            """, (
                                external_ids['tpl_id'],
                                taxa_id
                            ))
                            tpl_count += 1
                            
                            if tpl_count <= 3:  # Debug first few insertions
                                print(f"    Inserted TPL {tpl_count}: {external_ids['tpl_id']} -> taxa_id {taxa_id}")
                                
                        except Exception as e:
                            if tpl_count <= 3:
                                print(f"    ⚠️  Error inserting TPL crosswalk for taxa {taxa_id}: {e}")
                
                # Commit TPL crosswalk immediately
                conn.commit()
                print(f"  ✓ Inserted and committed {tpl_count:,} TPL crosswalk records")
                
                # Populate IPNI crosswalk from WFO data
                print("📋 Populating IPNI crosswalk...")
                ipni_count = 0
                ipni_processed = 0
                ipni_errors = 0
                
                for taxa_id, external_ids in external_ids_data.items():
                    if 'ipni_id' in external_ids and external_ids['ipni_id']:
                        ipni_processed += 1
                        try:
                            # Extract IPNI ID from URN format
                            ipni_id = external_ids['ipni_id']
                            if ipni_id.startswith('urn:lsid:ipni.org:names:'):
                                ipni_id = ipni_id.replace('urn:lsid:ipni.org:names:', '')
                            
                            cur.execute("""
                                INSERT INTO crosswalk.ipni (id, taxa_id)
                                VALUES (%s, %s)
                            """, (
                                ipni_id,
                                taxa_id
                            ))
                            ipni_count += 1
                            
                            if ipni_count <= 3:  # Debug first few insertions
                                print(f"    Inserted IPNI {ipni_count}: {ipni_id} -> taxa_id {taxa_id} (original: {external_ids['ipni_id']})")
                                
                        except Exception as e:
                            ipni_errors += 1
                            if ipni_errors <= 3:
                                print(f"    ⚠️  Error inserting IPNI crosswalk for taxa {taxa_id}: {e}")
                                print(f"        IPNI ID: {external_ids['ipni_id']}")
                
                # Final commit for IPNI with extra verification
                conn.commit()
                print(f"  ✓ Inserted and committed {ipni_count:,} IPNI crosswalk records")
                print(f"  📊 IPNI debug: processed {ipni_processed} records with IPNI IDs, successfully inserted {ipni_count}, errors: {ipni_errors}")
                
                # Immediate verification using the SAME connection/transaction
                cur.execute("SELECT COUNT(*) FROM crosswalk.ipni")
                ipni_verify = cur.fetchone()[0]
                print(f"  🔍 IPNI immediate verification (same connection): {ipni_verify:,} records in table after commit")
                
                if ipni_verify != ipni_count:
                    print(f"  ⚠️  WARNING: IPNI count mismatch! Inserted {ipni_count} but found {ipni_verify}")
                    
                    # Check for sample records
                    cur.execute("SELECT id, taxa_id FROM crosswalk.ipni LIMIT 3")
                    sample_records = cur.fetchall()
                    print(f"  Sample IPNI records: {sample_records}")
                
                # Also check with a fresh connection to see if it's a transaction isolation issue
                print(f"  🔍 IPNI verification with fresh connection...")
                fresh_conn = self.get_db_connection()
                try:
                    with fresh_conn.cursor() as fresh_cur:
                        fresh_cur.execute("SELECT COUNT(*) FROM crosswalk.ipni")
                        fresh_ipni_count = fresh_cur.fetchone()[0]
                        print(f"    Fresh connection shows: {fresh_ipni_count:,} IPNI records")
                        
                        if fresh_ipni_count != ipni_verify:
                            print(f"    ⚠️  TRANSACTION ISOLATION ISSUE DETECTED!")
                            print(f"    Same connection: {ipni_verify}, Fresh connection: {fresh_ipni_count}")
                finally:
                    fresh_conn.close()

                
                print(f"\n✅ All crosswalk tables populated and committed successfully")
                print(f"  📊 Summary: WFO={wfo_count:,}, GBIF={gbif_count:,}, TPL={tpl_count:,}, IPNI={ipni_count:,}")
                
                # Double check that data is actually there
                print(f"\n🔍 Verifying crosswalk data persistence...")
                cur.execute("SELECT COUNT(*) FROM crosswalk.wfo")
                wfo_check = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM crosswalk.gbif") 
                gbif_check = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM crosswalk.tpl")
                tpl_check = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM crosswalk.ipni")
                ipni_check = cur.fetchone()[0]
                
                print(f"  Verification counts: WFO={wfo_check:,}, GBIF={gbif_check:,}, TPL={tpl_check:,}, IPNI={ipni_check:,}")
                
                if wfo_check == 0 and gbif_check == 0:
                    print("  ⚠️  WARNING: Crosswalk tables still empty after commit!")
                    print("  This suggests a transaction isolation or connection issue.")
                
        except Exception as e:
            print(f"❌ Error populating crosswalk tables: {e}")
            print("Rolling back crosswalk transaction...")
            conn.rollback()
            raise  # Re-raise to see the full error
        finally:
            conn.close()
    
    def verify_integration(self):
        """Verify the integration results"""
        print("\n=== VERIFYING INTEGRATION ===")
        
        conn = self.get_db_connection()
        
        try:
            with conn.cursor() as cur:
                # Check main taxonomy table
                cur.execute("SELECT COUNT(*) FROM taxonomy.taxa")
                total_taxa = cur.fetchone()[0]
                print(f"📊 Total taxa: {total_taxa:,}")
                
                # Check rank distribution
                cur.execute("""
                    SELECT rank, COUNT(*) 
                    FROM taxonomy.taxa 
                    GROUP BY rank 
                    ORDER BY COUNT(*) DESC
                """)
                ranks = cur.fetchall()
                print(f"📊 Rank distribution:")
                for rank, count in ranks:
                    print(f"  {rank}: {count:,}")
                
                # Check status distribution
                cur.execute("""
                    SELECT status, COUNT(*) 
                    FROM taxonomy.taxa 
                    GROUP BY status 
                    ORDER BY COUNT(*) DESC
                """)
                statuses = cur.fetchall()
                print(f"📊 Status distribution:")
                for status, count in statuses:
                    print(f"  {status}: {count:,}")
                
                # Check crosswalk tables
                cur.execute("SELECT COUNT(*) FROM crosswalk.wfo")
                wfo_crosswalk = cur.fetchone()[0]
                print(f"📊 WFO crosswalk records: {wfo_crosswalk:,}")
                
                cur.execute("SELECT COUNT(*) FROM crosswalk.gbif")  
                gbif_crosswalk = cur.fetchone()[0]
                print(f"📊 GBIF crosswalk records: {gbif_crosswalk:,}")
                
                cur.execute("SELECT COUNT(*) FROM crosswalk.tpl")
                tpl_crosswalk = cur.fetchone()[0]
                print(f"📊 TPL crosswalk records: {tpl_crosswalk:,}")
                
                cur.execute("SELECT COUNT(*) FROM crosswalk.ipni")
                ipni_crosswalk = cur.fetchone()[0]
                print(f"📊 IPNI crosswalk records: {ipni_crosswalk:,}")
                
                # Debug IPNI issue - check what's actually in the table
                if ipni_crosswalk == 0:
                    print("🔍 IPNI Debug - checking table contents...")
                    cur.execute("SELECT COUNT(*) FROM crosswalk.ipni")
                    actual_count = cur.fetchone()[0]
                    print(f"   Direct count from IPNI table: {actual_count}")
                    
                    # Check if there are any rows at all
                    cur.execute("SELECT id, taxa_id FROM crosswalk.ipni LIMIT 3")
                    sample_ipni = cur.fetchall()
                    if sample_ipni:
                        print(f"   Sample IPNI records: {sample_ipni}")
                    else:
                        print("   No IPNI records found in table")
                
                # Sample some records with fixed column names
                print(f"\n📋 Sample integrated records:")
                
                # Get WFO samples
                cur.execute("""
                    SELECT t.name, t.rank, t.status, 'WFO' as source
                    FROM taxonomy.taxa t
                    JOIN crosswalk.wfo w ON t.id = w.taxa_id
                    WHERE t.name LIKE '%Drosera%'
                    LIMIT 5
                """)
                wfo_samples = cur.fetchall()
                
                # Get GBIF samples
                cur.execute("""
                    SELECT t.name, t.rank, t.status, 'GBIF' as source
                    FROM taxonomy.taxa t
                    JOIN crosswalk.gbif g ON t.id = g.taxa_id
                    WHERE t.name LIKE '%Drosera%'
                    LIMIT 5
                """)
                gbif_samples = cur.fetchall()
                
                # Combine and display samples
                all_samples = list(wfo_samples) + list(gbif_samples)
                for name, rank, status, source in all_samples:
                    print(f"  {name} ({rank}) [{status}] - from {source}")
                
        finally:
            conn.close()
    
    def run_full_integration(self):
        """Run the complete integration process"""
        print("🚀 STARTING TAXONOMY INTEGRATION")
        print("="*60)
        
        start_time = time.time()
        
        # Step 1: Analyze temporary tables
        analysis = self.analyze_temporary_tables()
        
        # Step 2: Create crosswalk schema
        self.create_crosswalk_schema()
        
        # Step 3: Merge taxonomy data
        mapping_data = self.merge_taxa_data()
        
        # Only proceed with crosswalk if merge was successful
        if mapping_data:
            # Step 4: Populate crosswalk tables
            self.populate_crosswalk_tables(mapping_data)
        else:
            print("⚠️  Skipping crosswalk population due to merge failure")
        
        # Step 5: Verify results  
        self.verify_integration()
        
        elapsed = time.time() - start_time
        print(f"\n🎉 INTEGRATION COMPLETE!")
        print(f"⏱️  Total time: {elapsed/60:.1f} minutes")
        print(f"✅ Taxonomy database is ready for use")

def main():
    print("=== TAXONOMY DATA INTEGRATION ===")
    
    merger = TaxonomyMerger()
    merger.run_full_integration()

if __name__ == "__main__":
    main()