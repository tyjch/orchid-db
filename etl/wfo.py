import os
import duckdb
import psycopg
from dotenv import load_dotenv
import time
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import threading
import multiprocessing as mp
import yaml
from dataclasses import dataclass
from typing import Optional, Dict, List, Any
import logging
from datetime import datetime
import zipfile
import csv
import glob

# Load environment
load_dotenv()

@dataclass
class WFODatasetConfig:
    """Configuration for WFO dataset processing"""
    name: str
    families_dir: str
    duckdb_file: str
    split_dir: str
    table_name: str
    schema_name: str = "temporary"
    progress_file: Optional[str] = None
    
    # Processing settings
    chunk_size: int = 2500
    batch_size: int = 25
    max_workers: int = 4
    
    # File splitting settings
    split_chunk_size: int = 500000
    
    # Plant families configuration
    plant_families_yaml: str = "plant_families.yaml"

class WFODataProcessor:
    def __init__(self, config: WFODatasetConfig):
        self.config = config
        self.progress_lock = threading.Lock()
        self.completed_chunks = set()
        self.records_found = 0
        self.start_time = time.time()
        
        # Create directories
        os.makedirs(os.path.dirname(config.duckdb_file), exist_ok=True)
        os.makedirs(config.split_dir, exist_ok=True)
        
        # Load plant families configuration
        self.target_families = self.load_target_families()
        
        # Set up logging
        self.setup_logging()
        
    def setup_logging(self):
        """Set up detailed logging"""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'{self.config.table_name}_processing.log')
            ]
        )
        self.logger = logging.getLogger(self.config.table_name)
        
    def load_target_families(self):
        """Load target plant families from YAML configuration"""
        try:
            with open(self.config.plant_families_yaml, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            
            # Extract all families from relevant categories
            all_families = []
            family_categories = ['carnivorous_plants', 'ferns_and_allies', 'geophytes', 'aquatic_and_wetland']
            
            for category in family_categories:
                if category in config and 'families' in config[category]:
                    families = config[category]['families']
                    all_families.extend(families)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_families = []
            for family in all_families:
                if family not in seen:
                    seen.add(family)
                    unique_families.append(family)
            
            print(f"✓ Loaded {len(unique_families)} target plant families from YAML")
            print(f"  Sample families: {unique_families[:5]}...")
            return unique_families
            
        except Exception as e:
            print(f"❌ Error loading plant families config: {e}")
            return []
    
    def find_wfo_files(self):
        """Find WFO zip files for target families"""
        print(f"🔍 Scanning for WFO files in: {self.config.families_dir}")
        
        found_files = []
        missing_families = []
        
        for family in self.target_families:
            # Look for zip files containing the family name
            pattern = os.path.join(self.config.families_dir, f"*{family}*.zip")
            files = glob.glob(pattern)
            
            if files:
                # Take the first match
                found_files.append((family, files[0]))
                print(f"  ✓ {family}: {os.path.basename(files[0])}")
            else:
                missing_families.append(family)
                print(f"  ✗ {family}: No file found")
        
        print(f"\n📊 WFO File Summary:")
        print(f"  Found: {len(found_files)} families")
        print(f"  Missing: {len(missing_families)} families")
        
        if missing_families:
            print(f"  Missing families: {missing_families[:10]}...")
            if len(missing_families) > 10:
                print(f"    ... and {len(missing_families) - 10} more")
        
        return found_files, missing_families
    
    def extract_wfo_data(self, family_zip_pairs):
        """Extract taxonomy data from WFO zip files"""
        print(f"\n🗜️  EXTRACTING WFO TAXONOMY DATA")
        
        combined_csv_path = os.path.join(self.config.split_dir, "wfo_combined.csv")
        
        # Check if combined file already exists
        if os.path.exists(combined_csv_path):
            print(f"✓ Found existing combined WFO file: {combined_csv_path}")
            return combined_csv_path
        
        print(f"Creating combined WFO taxonomy file...")
        
        total_records = 0
        header_written = False
        
        with open(combined_csv_path, 'w', encoding='utf-8', newline='') as outfile:
            csv_writer = None
            
            for i, (family_name, zip_path) in enumerate(family_zip_pairs):
                print(f"  Processing {family_name} ({i+1}/{len(family_zip_pairs)})...")
                
                try:
                    with zipfile.ZipFile(zip_path, 'r') as z:
                        # Look for classification.csv in the zip
                        csv_files = [f for f in z.namelist() if f.endswith('classification.csv')]
                        
                        if not csv_files:
                            print(f"    ⚠️  No classification.csv found in {family_name}")
                            continue
                        
                        csv_file = csv_files[0]
                        
                        with z.open(csv_file) as f:
                            content = f.read().decode('utf-8')
                            csv_reader = csv.DictReader(content.splitlines())
                            
                            family_records = 0
                            
                            for row in csv_reader:
                                # Keep all records - accepted, synonyms, invalid, unresolved
                                # We'll separate them later in the database
                                
                                # Filter to only records from target family (if specified)
                                if row.get('family', '') and row.get('family', '') != family_name:
                                    continue
                                
                                # Initialize CSV writer with first valid row
                                if not header_written:
                                    csv_writer = csv.DictWriter(outfile, fieldnames=row.keys())
                                    csv_writer.writeheader()
                                    header_written = True
                                
                                # Add source family column for tracking (but use existing 'source' column)
                                # Most WFO files already have a 'source' column we can use
                                csv_writer.writerow(row)
                                
                                family_records += 1
                                total_records += 1
                            
                            print(f"    ✓ {family_name}: {family_records:,} records")
                            
                except Exception as e:
                    print(f"    ❌ Error processing {family_name}: {e}")
                    continue
        
        print(f"✓ Combined WFO file created: {total_records:,} total records")
        print(f"  File: {combined_csv_path}")
        
        # Show file size
        file_size_mb = os.path.getsize(combined_csv_path) / (1024**2)
        print(f"  Size: {file_size_mb:.1f} MB")
        
        return combined_csv_path
    
    def log_worker_activity(self, worker_id, message, level="INFO"):
        """Thread-safe worker logging"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        if level == "INFO":
            print(f"[{timestamp}] 🔧 Worker-{worker_id}: {message}")
        elif level == "SUCCESS":
            print(f"[{timestamp}] ✅ Worker-{worker_id}: {message}")
        elif level == "ERROR":
            print(f"[{timestamp}] ❌ Worker-{worker_id}: {message}")
        elif level == "PROGRESS":
            print(f"[{timestamp}] 📊 Worker-{worker_id}: {message}")
            
    def get_elapsed_time(self):
        """Get formatted elapsed time"""
        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        return f"{minutes:02d}:{seconds:02d}"
        
    def load_progress(self):
        """Load progress from file"""
        if not self.config.progress_file or not os.path.exists(self.config.progress_file):
            return set()
            
        try:
            with open(self.config.progress_file, 'r') as f:
                completed_chunks = set()
                for line in f:
                    line = line.strip()
                    if line and line.isdigit():
                        completed_chunks.add(int(line))
                return completed_chunks
        except Exception as e:
            print(f"⚠️  Error reading progress file: {e}")
            return set()
    
    def save_progress_unsafe(self, completed_chunks):
        """Save progress without acquiring lock (for use when lock is already held)"""
        if not self.config.progress_file:
            return
            
        try:
            with open(self.config.progress_file, 'w') as f:
                for chunk_num in sorted(completed_chunks):
                    f.write(f"{chunk_num}\n")
        except Exception as e:
            print(f"⚠️  Error saving progress: {e}")
    
    def save_progress(self, completed_chunks):
        """Thread-safe progress saving"""
        if not self.config.progress_file:
            return
            
        with self.progress_lock:
            self.save_progress_unsafe(completed_chunks)
    
    def clear_progress(self):
        """Clear progress file"""
        if self.config.progress_file and os.path.exists(self.config.progress_file):
            os.remove(self.config.progress_file)
            print("✓ Cleared previous progress")
    
    def split_large_csv(self, csv_file_path):
        """Split the combined CSV into smaller, manageable files"""
        print(f"=== SPLITTING WFO COMBINED FILE ===")
        print(f"Source: {csv_file_path}")
        print(f"Target directory: {self.config.split_dir}")
        
        # Check if splits already exist
        existing_splits = [f for f in os.listdir(self.config.split_dir) 
                          if f.startswith("wfo_chunk_") and f.endswith(".csv")]
        if existing_splits:
            print(f"✓ Found {len(existing_splits)} existing split files")
            return sorted(existing_splits)
        
        # Split the file
        chunk_size = self.config.split_chunk_size
        chunk_num = 0
        current_chunk_rows = 0
        header_line = None
        created_files = []
        
        print(f"Splitting into chunks of {chunk_size:,} rows each...")
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as infile:
                # Read and save header
                header_line = infile.readline()
                print(f"✓ Header: {header_line.strip()[:100]}...")
                
                current_chunk_file = None
                
                for line_num, line in enumerate(infile, 1):
                    # Start new chunk if needed
                    if current_chunk_rows == 0:
                        chunk_num += 1
                        chunk_filename = f"wfo_chunk_{chunk_num:03d}.csv"
                        chunk_path = os.path.join(self.config.split_dir, chunk_filename)
                        
                        if current_chunk_file:
                            current_chunk_file.close()
                        
                        current_chunk_file = open(chunk_path, 'w', encoding='utf-8')
                        current_chunk_file.write(header_line)
                        created_files.append(chunk_filename)
                        print(f"  Creating {chunk_filename}...")
                    
                    # Write line to current chunk
                    current_chunk_file.write(line)
                    current_chunk_rows += 1
                    
                    # Show progress every 50k lines
                    if line_num % 50000 == 0:
                        print(f"    Processed {line_num:,} lines...")
                    
                    # Close chunk when it reaches target size
                    if current_chunk_rows >= chunk_size:
                        current_chunk_rows = 0
                
                # Close final chunk
                if current_chunk_file:
                    current_chunk_file.close()
            
            print(f"✓ Split complete: {len(created_files)} chunks created")
            return created_files
            
        except Exception as e:
            print(f"✗ Error splitting CSV: {e}")
            return None
    
    def load_chunk_to_duckdb(self, duck_conn, chunk_file, is_first_chunk=False):
        """Load a single chunk into DuckDB"""
        chunk_path = os.path.join(self.config.split_dir, chunk_file)
        
        try:
            if is_first_chunk:
                # First chunk: create table
                duck_conn.execute(f"""
                    CREATE TABLE {self.config.table_name} AS 
                    SELECT * FROM read_csv_auto(
                        '{chunk_path}',
                        ignore_errors=true,
                        strict_mode=false
                    )
                """)
            else:
                # Subsequent chunks: insert into existing table
                duck_conn.execute(f"""
                    INSERT INTO {self.config.table_name}
                    SELECT * FROM read_csv_auto(
                        '{chunk_path}',
                        ignore_errors=true,
                        strict_mode=false
                    )
                """)
            
            # Get row count for this chunk
            result = duck_conn.execute(f"SELECT COUNT(*) FROM {self.config.table_name}").fetchone()
            return result[0]
            
        except Exception as e:
            print(f"  ✗ Error loading {chunk_file}: {e}")
            return None
    
    def process_all_chunks(self, chunk_files):
        """Process all CSV chunks into DuckDB"""
        print(f"\n=== LOADING WFO DATA INTO DUCKDB ===")
        
        # Check if DuckDB file already exists
        if os.path.exists(self.config.duckdb_file):
            print(f"✓ DuckDB file already exists: {self.config.duckdb_file}")
            duck_conn = duckdb.connect(self.config.duckdb_file)
            try:
                result = duck_conn.execute(f"SELECT COUNT(*) FROM {self.config.table_name}").fetchone()
                print(f"✓ Found {result[0]:,} rows in existing DuckDB file")
                return duck_conn
            except Exception as e:
                print(f"⚠️  Error reading existing DuckDB file: {e}")
                print("Will recreate the file...")
                duck_conn.close()
                os.remove(self.config.duckdb_file)
        
        # Create new DuckDB file and process chunks
        print("Creating new DuckDB file...")
        duck_conn = duckdb.connect(self.config.duckdb_file)
        
        if not chunk_files:
            print("✗ No chunk files found!")
            return None
        
        print(f"Processing {len(chunk_files)} chunks...")
        start_time = time.time()
        previous_total = 0
        
        for i, chunk_file in enumerate(chunk_files):
            print(f"Processing {chunk_file} ({i+1}/{len(chunk_files)})...")
            
            current_total = self.load_chunk_to_duckdb(duck_conn, chunk_file, is_first_chunk=(i == 0))
            
            if current_total is not None:
                chunk_rows = current_total - previous_total
                print(f"  ✓ Added {chunk_rows:,} rows (total: {current_total:,})")
                previous_total = current_total
            else:
                print(f"  ✗ Failed to process {chunk_file}")
        
        elapsed = time.time() - start_time
        final_count = duck_conn.execute(f"SELECT COUNT(*) FROM {self.config.table_name}").fetchone()[0]
        print(f"✓ Loaded {final_count:,} rows into DuckDB in {elapsed:.1f} seconds")
        
        # Show file size
        file_size_mb = os.path.getsize(self.config.duckdb_file) / (1024**2)
        print(f"✓ DuckDB file size: {file_size_mb:.1f} MB")
        
        # Show sample data
        print(f"\n📊 Sample WFO data:")
        try:
            sample = duck_conn.execute(f"""
                SELECT scientificName, taxonRank, family, taxonomicStatus 
                FROM {self.config.table_name} 
                LIMIT 5
            """).fetchall()
            
            for row in sample:
                print(f"  {row[0]} ({row[1]}) - {row[2]} [{row[3]}]")
        except Exception as e:
            print(f"  Could not show sample: {e}")
        
        return duck_conn
    
    def get_db_connection(self):
        """Create a new PostgreSQL connection"""
        return psycopg.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            connect_timeout=30,
            options="-c statement_timeout=0"
        )
    
    def setup_database(self, duck_conn):
        """Set up PostgreSQL schema and table"""
        print("Setting up PostgreSQL schema and table...")
        
        pg_conn = self.get_db_connection()
        pg_conn.autocommit = False
        
        try:
            # Create schema
            with pg_conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config.schema_name}")
                pg_conn.commit()
            
            # Get column info from DuckDB
            columns_info = duck_conn.execute(f"DESCRIBE {self.config.table_name}").fetchall()
            column_names = [col[0] for col in columns_info]
            
            # Check if table exists
            table_exists = False
            with pg_conn.cursor() as cur:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {self.config.schema_name}.{self.config.table_name}")
                    current_rows = cur.fetchone()[0]
                    table_exists = True
                    print(f"✓ Found existing table with {current_rows:,} rows")
                except:
                    # Create table
                    pg_conn.rollback()
                    cur.execute(f"DROP TABLE IF EXISTS {self.config.schema_name}.{self.config.table_name}")
                    columns_def = ", ".join([f'"{col}" TEXT' for col in column_names])
                    cur.execute(f"CREATE TABLE {self.config.schema_name}.{self.config.table_name} ({columns_def})")
                    pg_conn.commit()
                    print(f"✓ Created new table with {len(column_names)} columns")
            
            pg_conn.close()
            return table_exists
            
        except Exception as e:
            print(f"✗ Setup error: {e}")
            pg_conn.close()
            return False
    
    def process_chunk_batch(self, batch_info):
        """Process a batch of chunks in a single connection"""
        worker_id, chunk_batch, table_exists = batch_info
        
        self.log_worker_activity(worker_id, f"Starting batch with {len(chunk_batch)} chunks")
        
        # Create fresh connections for this worker
        duck_conn = duckdb.connect(self.config.duckdb_file)
        pg_conn = None
        
        try:
            self.log_worker_activity(worker_id, "Connecting to PostgreSQL...")
            pg_conn = self.get_db_connection()
            pg_conn.autocommit = False
            self.log_worker_activity(worker_id, "✓ PostgreSQL connection established")
            
            processed_chunks = []
            batch_record_count = 0
            temp_csv = f"temp_worker_{worker_id}_{self.config.table_name}.csv"
            
            with pg_conn.cursor() as cur:
                for i, chunk_num in enumerate(chunk_batch):
                    # Skip if already completed
                    if chunk_num in self.completed_chunks:
                        self.log_worker_activity(worker_id, f"Skipping completed chunk {chunk_num + 1}")
                        continue
                    
                    chunk_progress = f"({i+1}/{len(chunk_batch)})"
                    self.log_worker_activity(worker_id, f"Processing chunk {chunk_num + 1} {chunk_progress}")
                    
                    offset = chunk_num * self.config.chunk_size
                    
                    try:
                        # Export chunk from DuckDB
                        query = f"""
                            COPY (
                                SELECT * FROM {self.config.table_name} 
                                LIMIT {self.config.chunk_size} OFFSET {offset}
                            ) TO '{temp_csv}' (FORMAT CSV, HEADER)
                        """
                        
                        self.log_worker_activity(worker_id, f"Exporting chunk {chunk_num + 1} from DuckDB...")
                        duck_conn.execute(query)
                        
                        if not os.path.exists(temp_csv):
                            self.log_worker_activity(worker_id, f"No data file created for chunk {chunk_num + 1}", "ERROR")
                            continue
                        
                        # Check if chunk has data
                        try:
                            with open(temp_csv, 'r', encoding='utf-8', errors='replace') as f:
                                lines = f.readlines()
                                if len(lines) <= 1:  # Only header
                                    self.log_worker_activity(worker_id, f"Chunk {chunk_num + 1} is empty")
                                    os.remove(temp_csv)
                                    continue
                                chunk_records = len(lines) - 1
                        except Exception as e:
                            self.log_worker_activity(worker_id, f"Error reading chunk {chunk_num + 1}: {e}", "ERROR")
                            if os.path.exists(temp_csv):
                                os.remove(temp_csv)
                            continue
                        
                        self.log_worker_activity(worker_id, f"Importing {chunk_records:,} records from chunk {chunk_num + 1}...")
                        
                        # Import chunk
                        import_start = time.time()
                        
                        with open(temp_csv, 'r', encoding='utf-8', errors='replace') as f:
                            if chunk_num == 0 and not table_exists:
                                # First chunk globally: include header
                                with cur.copy(f"COPY {self.config.schema_name}.{self.config.table_name} FROM STDIN WITH CSV HEADER") as copy:
                                    while True:
                                        chunk_data = f.read(4096)
                                        if not chunk_data:
                                            break
                                        copy.write(chunk_data)
                            else:
                                # Skip header for all other chunks
                                next(f)
                                with cur.copy(f"COPY {self.config.schema_name}.{self.config.table_name} FROM STDIN WITH CSV") as copy:
                                    while True:
                                        chunk_data = f.read(4096)
                                        if not chunk_data:
                                            break
                                        copy.write(chunk_data)
                        
                        import_time = time.time() - import_start
                        processed_chunks.append(chunk_num)
                        batch_record_count += chunk_records
                        
                        self.log_worker_activity(worker_id, 
                            f"✓ Chunk {chunk_num + 1} completed in {import_time:.1f}s ({chunk_records:,} records)", 
                            "SUCCESS")
                        
                        # Clean up temp file
                        if os.path.exists(temp_csv):
                            os.remove(temp_csv)
                        
                    except Exception as e:
                        self.log_worker_activity(worker_id, f"Error processing chunk {chunk_num + 1}: {e}", "ERROR")
                        if os.path.exists(temp_csv):
                            os.remove(temp_csv)
                        pg_conn.rollback()
                        continue
                
                # Commit all chunks in this batch
                if processed_chunks:
                    self.log_worker_activity(worker_id, f"Committing {len(processed_chunks)} chunks...")
                    commit_start = time.time()
                    pg_conn.commit()
                    commit_time = time.time() - commit_start
                    self.log_worker_activity(worker_id, f"✓ Committed in {commit_time:.1f}s", "SUCCESS")
            
            # Update global progress
            with self.progress_lock:
                self.completed_chunks.update(processed_chunks)
                self.records_found += batch_record_count
                if self.config.progress_file:
                    self.save_progress_unsafe(self.completed_chunks)
            
            self.log_worker_activity(worker_id, 
                f"BATCH COMPLETE: {len(processed_chunks)} chunks, {batch_record_count:,} records",
                "SUCCESS")
            
            return {
                'worker_id': worker_id,
                'processed_chunks': len(processed_chunks),
                'record_count': batch_record_count,
                'success': True
            }
            
        except Exception as e:
            self.log_worker_activity(worker_id, f"FATAL ERROR: {e}", "ERROR")
            return {
                'worker_id': worker_id,
                'processed_chunks': 0,
                'record_count': 0,
                'success': False,
                'error': str(e)
            }
            
        finally:
            # Cleanup
            if duck_conn:
                duck_conn.close()
            if pg_conn:
                pg_conn.close()
            gc.collect()
    
    def parallel_transfer(self, duck_conn):
        """Main parallel transfer function"""
        print(f"\n=== PARALLEL WFO TRANSFER ===")
        print(f"Using {self.config.max_workers} parallel workers")
        
        # Load progress
        self.completed_chunks = self.load_progress()
        
        # Setup database
        table_exists = self.setup_database(duck_conn)
        if not table_exists and self.completed_chunks:
            print("⚠️  Table was recreated - clearing progress")
            self.completed_chunks = set()
            self.clear_progress()
        
        # Get data statistics
        total_count = duck_conn.execute(f"SELECT COUNT(*) FROM {self.config.table_name}").fetchone()[0]
        print(f"📊 Total records to transfer: {total_count:,}")
        
        # Calculate chunks
        max_chunks = (total_count + self.config.chunk_size - 1) // self.config.chunk_size
        remaining_chunks = [i for i in range(max_chunks) if i not in self.completed_chunks]
        
        print(f"   Total chunks: {max_chunks:,}")
        print(f"   Already completed: {len(self.completed_chunks):,}")
        print(f"   Remaining: {len(remaining_chunks):,}")
        
        if not remaining_chunks:
            print("✓ All chunks already completed!")
            return
        
        # Create chunk batches for workers
        chunk_batches = []
        for i in range(0, len(remaining_chunks), self.config.batch_size):
            batch = remaining_chunks[i:i + self.config.batch_size]
            chunk_batches.append(batch)
        
        print(f"   Processing {len(chunk_batches)} batches with {self.config.max_workers} workers")
        
        # Create work items
        work_items = []
        for i, chunk_batch in enumerate(chunk_batches):
            work_items.append((i, chunk_batch, table_exists))
        
        # Process in parallel
        start_time = time.time()
        completed_batches = 0
        
        print(f"\n🚀 Starting parallel transfer...")
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_batch = {executor.submit(self.process_chunk_batch, item): item for item in work_items}
            
            for future in as_completed(future_to_batch):
                try:
                    result = future.result(timeout=30)
                except Exception as e:
                    result = {
                        'worker_id': 'unknown',
                        'processed_chunks': 0,
                        'record_count': 0,
                        'success': False,
                        'error': str(e)
                    }
                
                completed_batches += 1
                elapsed = time.time() - start_time
                
                if result['success']:
                    rate = completed_batches / elapsed if elapsed > 0 else 0
                    remaining_batches = len(work_items) - completed_batches
                    eta_seconds = remaining_batches / rate if rate > 0 else 0
                    eta_minutes = int(eta_seconds // 60)
                    eta_secs = int(eta_seconds % 60)
                    
                    print(f"\n📈 PROGRESS [{self.get_elapsed_time()}]:")
                    print(f"   ✅ Completed: {completed_batches}/{len(work_items)} batches")
                    print(f"   📊 Records: {self.records_found:,}")
                    print(f"   ⏳ ETA: {eta_minutes:02d}:{eta_secs:02d}")
                else:
                    print(f"\n❌ BATCH FAILED: {result.get('error', 'Unknown error')}")
        
        elapsed = time.time() - start_time
        print(f"\n🎉 PARALLEL TRANSFER COMPLETE!")
        print(f"   ⏱️  Total time: {elapsed/60:.1f} minutes")
        print(f"   📊 Total records: {self.records_found:,}")
        
        # Final verification
        self.verify_transfer()
    
    def verify_transfer(self):
        """Verify the final transfer"""
        print("\n🔍 VERIFYING TRANSFER...")
        
        try:
            pg_conn = self.get_db_connection()
            
            with pg_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.config.schema_name}.{self.config.table_name}")
                pg_count = cur.fetchone()[0]
                print(f"✓ PostgreSQL table now has {pg_count:,} records")
                
                # Sample some records
                cur.execute(f"""
                    SELECT "scientificName", "taxonRank", "family" 
                    FROM {self.config.schema_name}.{self.config.table_name} 
                    LIMIT 3
                """)
                samples = cur.fetchall()
                
                print("📋 Sample records:")
                for name, rank, family in samples:
                    print(f"  {name} ({rank}) - {family}")
            
            pg_conn.close()
            
            # Clean up progress file
            if self.config.progress_file and os.path.exists(self.config.progress_file):
                os.remove(self.config.progress_file)
                print("🧹 Progress file cleaned up")
            
        except Exception as e:
            print(f"⚠️  Could not verify final transfer: {e}")
    
    def run_full_pipeline(self):
        """Run the complete WFO pipeline"""
        print(f"=== PROCESSING WFO PLANT TAXONOMY DATA ===")
        print(f"Source directory: {self.config.families_dir}")
        print(f"Target: {self.config.schema_name}.{self.config.table_name}")
        print(f"Target families: {len(self.target_families)}")
        
        # Step 1: Find WFO files for target families
        family_zip_pairs, missing_families = self.find_wfo_files()
        if not family_zip_pairs:
            print("❌ No WFO files found for target families!")
            return False
        
        # Step 2: Extract and combine WFO data
        combined_csv = self.extract_wfo_data(family_zip_pairs)
        if not combined_csv:
            print("❌ Failed to create combined WFO file!")
            return False
        
        # Step 3: Split the combined CSV
        chunk_files = self.split_large_csv(combined_csv)
        if not chunk_files:
            print("❌ Failed to split combined CSV!")
            return False
        
        # Step 4: Process chunks into DuckDB
        duck_conn = self.process_all_chunks(chunk_files)
        if not duck_conn:
            print("❌ Failed to load data into DuckDB!")
            return False
        
        # Step 5: Transfer to PostgreSQL in parallel
        self.parallel_transfer(duck_conn)
        
        # Step 6: Show final summary
        print(f"\n🎉 WFO DATA PROCESSING COMPLETE!")
        
        try:
            total_count = duck_conn.execute(f"SELECT COUNT(*) FROM {self.config.table_name}").fetchone()[0]
            family_count = duck_conn.execute(f"SELECT COUNT(DISTINCT family) FROM {self.config.table_name}").fetchone()[0]
            
            print(f"  📊 Total records: {total_count:,}")
            print(f"  📊 Families in data: {family_count}")
            print(f"  📊 Families processed: {len([f for f, _ in family_zip_pairs])}")
            print(f"  📊 Missing families: {len(missing_families)}")
            
            # Show taxonomic status distribution
            print(f"\n  📊 Taxonomic status distribution:")
            statuses = duck_conn.execute(f"""
                SELECT taxonomicStatus, COUNT(*) as count 
                FROM {self.config.table_name} 
                WHERE taxonomicStatus IS NOT NULL
                GROUP BY taxonomicStatus 
                ORDER BY count DESC
            """).fetchall()
            
            for status, count in statuses:
                print(f"    {status}: {count:,}")
            
            # Show rank distribution
            print(f"\n  📊 Rank distribution:")
            ranks = duck_conn.execute(f"""
                SELECT taxonRank, COUNT(*) as count 
                FROM {self.config.table_name} 
                WHERE taxonRank IS NOT NULL
                GROUP BY taxonRank 
                ORDER BY count DESC
                LIMIT 10
            """).fetchall()
            
            for rank, count in ranks:
                print(f"    {rank}: {count:,}")
            
        except Exception as e:
            print(f"  ⚠️  Could not show summary statistics: {e}")
        
        # Clean up
        duck_conn.close()
        
        print(f"\n✅ WFO data loaded to PostgreSQL: {self.config.schema_name}.{self.config.table_name}")
        print(f"✅ Includes accepted names, synonyms, and invalid names for comprehensive coverage")
        print(f"✅ Ready to merge with GBIF data for complete taxonomy coverage")
        
        return True

def main():
    print("=== WFO PLANT TAXONOMY PROCESSOR ===")
    
    # Configuration for WFO processing
    wfo_config = WFODatasetConfig(
        name="WFO Plant Taxonomy",
        families_dir=r"datasets\World Flora Online\families",
        duckdb_file="datasets/wfo_taxa.duckdb",
        split_dir="datasets/split_csvs_wfo",
        table_name="wfo_taxa",
        schema_name="temporary",
        progress_file="datasets/transfer_progress_wfo.txt",
        plant_families_yaml="plant_families.yaml"
    )
    
    # Create and run processor
    processor = WFODataProcessor(wfo_config)
    success = processor.run_full_pipeline()
    
    if success:
        print(f"\n🎉 WFO processing completed successfully!")
        print(f"📊 Data available in:")
        print(f"   DuckDB: {wfo_config.duckdb_file}")
        print(f"   PostgreSQL: {wfo_config.schema_name}.{wfo_config.table_name}")
        print(f"\n🔄 Next steps:")
        print(f"   1. Merge WFO + GBIF data into taxonomy.taxa")
        print(f"   2. Set up crosswalk schema for external ID mappings")
        print(f"   3. Create synonym relationships later")
    else:
        print(f"\n❌ WFO processing failed!")

if __name__ == "__main__":
    main()