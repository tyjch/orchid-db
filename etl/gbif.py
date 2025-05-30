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

# Load environment
load_dotenv()

@dataclass
class DatasetConfig:
    """Configuration for a dataset"""
    name: str
    csv_file: str
    duckdb_file: str
    split_dir: str
    table_name: str
    schema_name: str = "temporary"
    progress_file: Optional[str] = None
    
    # Processing settings
    chunk_size: int = 2500
    batch_size: int = 25
    max_workers: int = 4
    
    # Data filtering
    filter_condition: Optional[str] = None
    
    # File splitting settings
    split_chunk_size: int = 500000
    
    # Type overrides for problematic columns
    column_types: Optional[Dict[str, str]] = None

# Dataset configurations
DATASET_CONFIGS = {
    "gbif_occurrences": DatasetConfig(
        name="GBIF Occurrences",
        csv_file=r"datasets\Global Biodiversity Information Facility\occurences\0016315-250515123054153.csv",
        duckdb_file="datasets/gbif_occurrences.duckdb",
        split_dir="datasets/split_csvs_occurrences",
        table_name="gbif_occurrences",
        schema_name="temporary",
        progress_file="datasets/transfer_progress_occurrences.txt",
        filter_condition="kingdom = 'Plantae'",
        column_types={"eventDate": "VARCHAR"}
    ),
    
    "gbif_taxon": DatasetConfig(
        name="GBIF Taxonomy Backbone",
        csv_file=r"datasets\Global Biodiversity Information Facility\backbone\Taxon.tsv",
        duckdb_file="datasets/gbif_taxon.duckdb",
        split_dir="datasets/split_csvs_taxon",
        table_name="gbif_taxon",
        schema_name="temporary",
        progress_file="datasets/transfer_progress_taxon.txt",
        chunk_size=5000,  # Larger chunks for taxonomy data
        batch_size=50,
        filter_condition=None,  # No filtering for taxonomy
        split_chunk_size=1000000,  # 1M rows per split
        column_types=None
    ),
    
    "custom_dataset": DatasetConfig(
        name="Custom Dataset Template",
        csv_file="datasets/your_file.csv",
        duckdb_file="datasets/your_data.duckdb", 
        split_dir="datasets/split_csvs_custom",
        table_name="your_table",
        schema_name="temporary",
        progress_file="datasets/transfer_progress_custom.txt"
    )
}

class FlexibleDataProcessor:
    def __init__(self, config: DatasetConfig):
        self.config = config
        self.progress_lock = threading.Lock()
        self.completed_chunks = set()
        self.records_found = 0
        self.start_time = time.time()
        
        # Create directories
        os.makedirs(os.path.dirname(config.duckdb_file), exist_ok=True)
        os.makedirs(config.split_dir, exist_ok=True)
        
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
    
    def split_large_csv(self):
        """Split the large CSV into smaller, manageable files"""
        print(f"=== SPLITTING {self.config.name.upper()} ===")
        print(f"Source: {self.config.csv_file}")
        print(f"Target directory: {self.config.split_dir}")
        
        # Check if splits already exist
        existing_splits = [f for f in os.listdir(self.config.split_dir) 
                          if f.startswith("chunk_") and f.endswith(".csv")]
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
            with open(self.config.csv_file, 'r', encoding='utf-8') as infile:
                # Read and save header
                header_line = infile.readline()
                print(f"✓ Header: {header_line.strip()[:100]}...")
                
                current_chunk_file = None
                
                for line_num, line in enumerate(infile, 1):
                    # Start new chunk if needed
                    if current_chunk_rows == 0:
                        chunk_num += 1
                        chunk_filename = f"chunk_{chunk_num:03d}.csv"
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
                    
                    # Show progress every 100k lines
                    if line_num % 100000 == 0:
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
    
    def load_chunk_to_duckdb(self, chunk_file, duck_conn, is_first_chunk=False):
        """Load a single chunk into DuckDB"""
        chunk_path = os.path.join(self.config.split_dir, chunk_file)
        
        try:
            # Build the SQL with type overrides if specified
            if self.config.column_types:
                types_dict = self.config.column_types
            else:
                types_dict = {}
            
            if is_first_chunk:
                # First chunk: create table
                if types_dict:
                    duck_conn.execute(f"""
                        CREATE TABLE {self.config.table_name} AS 
                        SELECT * FROM read_csv_auto(
                            '{chunk_path}', 
                            types={types_dict},
                            ignore_errors=true,
                            strict_mode=false
                        )
                    """)
                else:
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
                if types_dict:
                    duck_conn.execute(f"""
                        INSERT INTO {self.config.table_name}
                        SELECT * FROM read_csv_auto(
                            '{chunk_path}', 
                            types={types_dict},
                            ignore_errors=true,
                            strict_mode=false
                        )
                    """)
                else:
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
    
    def process_all_chunks(self):
        """Process all CSV chunks into DuckDB"""
        print(f"\n=== LOADING {self.config.name.upper()} INTO DUCKDB ===")
        
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
        
        # Get list of chunk files
        chunk_files = [f for f in os.listdir(self.config.split_dir) 
                      if f.startswith("chunk_") and f.endswith(".csv")]
        chunk_files.sort()
        
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
            
            # Get column info
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
                        # Build query with optional filter
                        if self.config.filter_condition:
                            query = f"""
                                COPY (
                                    SELECT * FROM {self.config.table_name} 
                                    WHERE {self.config.filter_condition}
                                    LIMIT {self.config.chunk_size} OFFSET {offset}
                                ) TO '{temp_csv}' (FORMAT CSV, HEADER)
                            """
                        else:
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
                            # Use UTF-8 encoding to handle international characters
                            with open(temp_csv, 'r', encoding='utf-8', errors='replace') as f:
                                lines = f.readlines()
                                if len(lines) <= 1:  # Only header
                                    self.log_worker_activity(worker_id, f"Chunk {chunk_num + 1} is empty (filtered out)")
                                    os.remove(temp_csv)
                                    continue
                                chunk_records = len(lines) - 1
                        except Exception as e:
                            self.log_worker_activity(worker_id, f"Error reading chunk {chunk_num + 1}: {e}", "ERROR")
                            # Try with different encodings
                            try:
                                with open(temp_csv, 'r', encoding='latin-1') as f:
                                    lines = f.readlines()
                                    if len(lines) <= 1:
                                        os.remove(temp_csv)
                                        continue
                                    chunk_records = len(lines) - 1
                                self.log_worker_activity(worker_id, f"Successfully read chunk {chunk_num + 1} with latin-1 encoding")
                            except Exception as e2:
                                self.log_worker_activity(worker_id, f"Failed to read chunk {chunk_num + 1} with any encoding: {e2}", "ERROR")
                                if os.path.exists(temp_csv):
                                    os.remove(temp_csv)
                                continue
                        
                        self.log_worker_activity(worker_id, f"Importing {chunk_records:,} records from chunk {chunk_num + 1}...")
                        
                        # Import chunk with proper encoding handling
                        import_start = time.time()
                        
                        # Try UTF-8 first, then fall back to latin-1
                        for encoding in ['utf-8', 'latin-1']:
                            try:
                                with open(temp_csv, 'r', encoding=encoding, errors='replace') as f:
                                    if chunk_num == 0 and not table_exists:
                                        # First chunk globally: include header
                                        with cur.copy(f"COPY {self.config.schema_name}.{self.config.table_name} FROM STDIN WITH CSV HEADER") as copy:
                                            bytes_written = 0
                                            while True:
                                                chunk_data = f.read(4096)
                                                if not chunk_data:
                                                    break
                                                copy.write(chunk_data)
                                                bytes_written += len(chunk_data)
                                                del chunk_data
                                            self.log_worker_activity(worker_id, f"Wrote {bytes_written:,} bytes for chunk {chunk_num + 1} ({encoding})")
                                    else:
                                        # Skip header for all other chunks
                                        next(f)
                                        with cur.copy(f"COPY {self.config.schema_name}.{self.config.table_name} FROM STDIN WITH CSV") as copy:
                                            bytes_written = 0
                                            while True:
                                                chunk_data = f.read(4096)
                                                if not chunk_data:
                                                    break
                                                copy.write(chunk_data)
                                                bytes_written += len(chunk_data)
                                                del chunk_data
                                            self.log_worker_activity(worker_id, f"Wrote {bytes_written:,} bytes for chunk {chunk_num + 1} ({encoding})")
                                break  # Success, exit encoding loop
                            except UnicodeDecodeError:
                                if encoding == 'utf-8':
                                    self.log_worker_activity(worker_id, f"UTF-8 failed for chunk {chunk_num + 1}, trying latin-1...")
                                    continue
                                else:
                                    raise  # Both encodings failed
                            except Exception as e:
                                if encoding == 'latin-1':
                                    raise  # Last encoding failed
                                continue
                        
                        import_time = time.time() - import_start
                        processed_chunks.append(chunk_num)
                        batch_record_count += chunk_records
                        
                        self.log_worker_activity(worker_id, 
                            f"✓ Chunk {chunk_num + 1} completed in {import_time:.1f}s ({chunk_records:,} records)", 
                            "SUCCESS")
                        
                        # Clean up temp file
                        if os.path.exists(temp_csv):
                            os.remove(temp_csv)
                        
                        # Show progress every few chunks
                        if (i + 1) % 5 == 0 or i == len(chunk_batch) - 1:
                            self.log_worker_activity(worker_id, 
                                f"Batch progress: {i+1}/{len(chunk_batch)} chunks, {batch_record_count:,} records so far",
                                "PROGRESS")
                        
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
            
            # Update global progress with timeout protection and debugging
            self.log_worker_activity(worker_id, f"Updating global progress... (processed: {len(processed_chunks)}, records: {batch_record_count})")
            progress_start = time.time()
            
            try:
                # Try to acquire lock with timeout to detect deadlocks
                lock_acquired = self.progress_lock.acquire(timeout=10)  # 10 second timeout
                
                if not lock_acquired:
                    self.log_worker_activity(worker_id, "ERROR: Could not acquire progress lock within 10 seconds!", "ERROR")
                    # Continue without updating progress to avoid deadlock
                else:
                    try:
                        self.log_worker_activity(worker_id, "✓ Progress lock acquired")
                        
                        # Update counts
                        old_completed_count = len(self.completed_chunks)
                        old_records_count = self.records_found
                        
                        self.completed_chunks.update(processed_chunks)
                        self.records_found += batch_record_count
                        
                        new_completed_count = len(self.completed_chunks)
                        new_records_count = self.records_found
                        
                        self.log_worker_activity(worker_id, 
                            f"Progress updated: chunks {old_completed_count}→{new_completed_count}, "
                            f"records {old_records_count:,}→{new_records_count:,}")
                        
                        # Save progress with error handling
                        try:
                            if self.config.progress_file:
                                self.save_progress_unsafe(self.completed_chunks)  # Don't acquire lock again
                                self.log_worker_activity(worker_id, "✓ Progress file saved")
                        except Exception as save_error:
                            self.log_worker_activity(worker_id, f"Warning: Progress save failed: {save_error}", "ERROR")
                            
                    finally:
                        self.progress_lock.release()
                        progress_time = time.time() - progress_start
                        self.log_worker_activity(worker_id, f"✓ Progress lock released after {progress_time:.1f}s")
                        
            except Exception as progress_error:
                self.log_worker_activity(worker_id, f"CRITICAL: Progress update error: {progress_error}", "ERROR")
                # Try to release lock if we have it
                try:
                    self.progress_lock.release()
                except:
                    pass
            
            filter_msg = f" (filtered: {self.config.filter_condition})" if self.config.filter_condition else ""
            self.log_worker_activity(worker_id, 
                f"BATCH COMPLETE: {len(processed_chunks)} chunks, {batch_record_count:,} records{filter_msg}",
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
            # Cleanup with detailed logging
            cleanup_start = time.time()
            self.log_worker_activity(worker_id, "Starting cleanup...")
            
            try:
                if duck_conn:
                    duck_conn.close()
                    self.log_worker_activity(worker_id, "✓ DuckDB connection closed")
            except Exception as e:
                self.log_worker_activity(worker_id, f"Error closing DuckDB: {e}", "ERROR")
                
            try:
                if pg_conn:
                    pg_conn.close()
                    self.log_worker_activity(worker_id, "✓ PostgreSQL connection closed")
            except Exception as e:
                self.log_worker_activity(worker_id, f"Error closing PostgreSQL: {e}", "ERROR")
                
            try:
                gc.collect()
                cleanup_time = time.time() - cleanup_start
                self.log_worker_activity(worker_id, f"✓ Cleanup completed in {cleanup_time:.1f}s")
            except Exception as e:
                self.log_worker_activity(worker_id, f"Error during garbage collection: {e}", "ERROR")
    
    def parallel_transfer(self, duck_conn):
        """Main parallel transfer function"""
        print(f"\n=== PARALLEL {self.config.name.upper()} TRANSFER ===")
        print(f"Using {self.config.max_workers} parallel workers")
        
        # Check current state first
        completed_chunks_count = self.check_current_progress()
        
        # Load progress
        self.completed_chunks = self.load_progress()
        
        # Setup database
        table_exists = self.setup_database(duck_conn)
        if not table_exists and self.completed_chunks:
            # Clear progress if table was recreated
            print("⚠️  Table was recreated - clearing progress")
            self.completed_chunks = set()
            self.clear_progress()
        
        # Get data statistics
        if self.config.filter_condition:
            filtered_count = duck_conn.execute(
                f"SELECT COUNT(*) FROM {self.config.table_name} WHERE {self.config.filter_condition}"
            ).fetchone()[0]
            total_count = duck_conn.execute(f"SELECT COUNT(*) FROM {self.config.table_name}").fetchone()[0]
            
            print(f"📊 Dataset summary:")
            print(f"   Total records in DuckDB: {total_count:,}")
            print(f"   Filtered records to transfer: {filtered_count:,}")
            print(f"   Filter: {self.config.filter_condition}")
        else:
            total_count = duck_conn.execute(f"SELECT COUNT(*) FROM {self.config.table_name}").fetchone()[0]
            print(f"📊 Dataset summary:")
            print(f"   Total records to transfer: {total_count:,}")
            print(f"   No filtering applied")
        
        # Calculate chunks
        max_chunks = (total_count + self.config.chunk_size - 1) // self.config.chunk_size
        remaining_chunks = [i for i in range(max_chunks) if i not in self.completed_chunks]
        
        print(f"   Total possible chunks: {max_chunks:,}")
        print(f"   Already completed chunks: {len(self.completed_chunks):,}")
        print(f"   Remaining chunks to process: {len(remaining_chunks):,}")
        
        # Check for the suspicious 1M row limit
        if len(self.completed_chunks) * self.config.chunk_size >= 1000000:
            print("⚠️  WARNING: Approaching or exceeded 1M records!")
            print("   This might explain the stalling issue.")
        
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
        
        print(f"\n🚀 Starting {len(work_items)} batches across {self.config.max_workers} workers...")
        print(f"📊 Each worker will process ~{len(work_items) // self.config.max_workers} batches")
        print(f"⏱️  Started at: {datetime.now().strftime('%H:%M:%S')}")
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all work with logging
            print(f"🔄 Submitting {len(work_items)} batches to executor...")
            future_to_batch = {}
            
            for i, item in enumerate(work_items):
                future = executor.submit(self.process_chunk_batch, item)
                future_to_batch[future] = item
                if (i + 1) % 10 == 0:
                    print(f"   📤 Submitted {i + 1}/{len(work_items)} batches...")
            
            print(f"✅ All {len(future_to_batch)} batches submitted to worker pool")
            
            # Process results as they complete with timeout monitoring
            last_completion_time = time.time()
            stall_warning_threshold = 60  # Warn if no completion for 60 seconds
            
            for future in as_completed(future_to_batch):
                current_time = time.time()
                time_since_last = current_time - last_completion_time
                last_completion_time = current_time
                
                if time_since_last > stall_warning_threshold:
                    print(f"\n⚠️  WARNING: {time_since_last:.0f}s since last completion - checking worker status...")
                    active_futures = [f for f in future_to_batch.keys() if not f.done()]
                    print(f"   🔍 Active workers: {len(active_futures)}")
                
                try:
                    result = future.result(timeout=30)  # 30 second timeout per result
                except Exception as e:
                    print(f"\n❌ WORKER TIMEOUT/ERROR:")
                    print(f"   Error: {e}")
                    result = {
                        'worker_id': 'unknown',
                        'processed_chunks': 0,
                        'record_count': 0,
                        'success': False,
                        'error': str(e)
                    }
                
                completed_batches += 1
                
                elapsed = time.time() - start_time
                remaining_batches = len(work_items) - completed_batches
                rate = completed_batches / elapsed if elapsed > 0 else 0
                
                if result['success']:
                    eta_seconds = remaining_batches / rate if rate > 0 else 0
                    eta_minutes = int(eta_seconds // 60)
                    eta_secs = int(eta_seconds % 60)
                    
                    print(f"\n📈 OVERALL PROGRESS [{self.get_elapsed_time()}]:")
                    print(f"   ✅ Completed: {completed_batches}/{len(work_items)} batches")
                    print(f"   📊 Records processed: {self.records_found:,}")
                    print(f"   ⚡ Rate: {rate:.1f} batches/sec")
                    print(f"   ⏳ ETA: {eta_minutes:02d}:{eta_secs:02d}")
                    print(f"   🔄 Active workers: {len([f for f in future_to_batch.keys() if not f.done()])}")
                    print(f"   ⏱️  Time since last completion: {time_since_last:.1f}s")
                else:
                    print(f"\n❌ BATCH FAILED [{self.get_elapsed_time()}]:")
                    print(f"   Error: {result.get('error', 'Unknown error')}")
                    print(f"   Completed: {completed_batches}/{len(work_items)} batches")
                
                # Show detailed progress every 5 batches or if stalled
                if completed_batches % 5 == 0 or not result['success'] or time_since_last > 30:
                    active_futures = [f for f in future_to_batch.keys() if not f.done()]
                    print(f"   🏃 Workers still active: {len(active_futures)}")
                    print(f"   💾 Progress saved to: {self.config.progress_file}")
                    
                    # If we have many workers stuck, show warning
                    if len(active_futures) > 0 and time_since_last > 30:
                        print(f"   ⚠️  Some workers may be stalled - will continue monitoring...")
        
        elapsed = time.time() - start_time
        print(f"\n🎉 PARALLEL TRANSFER COMPLETE!")
        print(f"   ⏱️  Total time: {elapsed/60:.1f} minutes")
        print(f"   📊 Total records: {self.records_found:,}")
        print(f"   ⚡ Average rate: {completed_batches / elapsed:.1f} batches/sec")
        print(f"   📈 Record rate: {self.records_found / elapsed:.0f} records/sec")
        
        # Final verification
        self.verify_transfer()
    
    def verify_transfer(self):
        """Verify the final transfer with detailed logging"""
        print("\n🔍 VERIFYING TRANSFER...")
        
        try:
            pg_conn = self.get_db_connection()
            
            with pg_conn.cursor() as cur:
                # Get total count
                cur.execute(f"SELECT COUNT(*) FROM {self.config.schema_name}.{self.config.table_name}")
                pg_count = cur.fetchone()[0]
                print(f"✓ PostgreSQL table now has {pg_count:,} records")
                
                # Check if we hit exactly 1 million (suspicious)
                if pg_count == 1000000:
                    print("⚠️  WARNING: Exactly 1,000,000 records detected!")
                    print("   This suggests a potential limit or truncation issue.")
                    
                    # Check for any LIMIT clauses or constraints
                    cur.execute(f"""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_schema = '{self.config.schema_name}' 
                        AND table_name = '{self.config.table_name}'
                        LIMIT 5
                    """)
                    columns = cur.fetchall()
                    print(f"   Table structure (first 5 cols): {columns}")
                
                # Get some sample data to verify it looks correct
                cur.execute(f"""
                    SELECT COUNT(*) as cnt, MIN(id) as min_id, MAX(id) as max_id
                    FROM {self.config.schema_name}.{self.config.table_name}
                    WHERE id IS NOT NULL
                """)
                id_stats = cur.fetchone()
                if id_stats and id_stats[0] > 0:
                    print(f"   ID range: {id_stats[1]} to {id_stats[2]} ({id_stats[0]:,} records with IDs)")
            
            pg_conn.close()
            
            # Clean up progress file
            if self.config.progress_file and os.path.exists(self.config.progress_file):
                os.remove(self.config.progress_file)
                print("🧹 Progress file cleaned up")
            
        except Exception as e:
            print(f"⚠️  Could not verify final transfer: {e}")
    
    def check_current_progress(self):
        """Check current progress and detect potential issues"""
        print(f"\n🔍 CHECKING CURRENT STATE...")
        
        # Check DuckDB
        try:
            duck_conn = duckdb.connect(self.config.duckdb_file)
            total_in_duck = duck_conn.execute(f"SELECT COUNT(*) FROM {self.config.table_name}").fetchone()[0]
            print(f"📊 DuckDB has {total_in_duck:,} total records")
            
            if self.config.filter_condition:
                filtered_in_duck = duck_conn.execute(
                    f"SELECT COUNT(*) FROM {self.config.table_name} WHERE {self.config.filter_condition}"
                ).fetchone()[0]
                print(f"📊 DuckDB has {filtered_in_duck:,} filtered records")
            
            duck_conn.close()
        except Exception as e:
            print(f"❌ Error checking DuckDB: {e}")
        
        # Check PostgreSQL
        try:
            pg_conn = self.get_db_connection()
            with pg_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.config.schema_name}.{self.config.table_name}")
                pg_count = cur.fetchone()[0]
                print(f"📊 PostgreSQL currently has {pg_count:,} records")
            pg_conn.close()
        except Exception as e:
            print(f"❌ Error checking PostgreSQL: {e}")
        
        # Check progress file
        completed = self.load_progress()
        print(f"📊 Progress file shows {len(completed):,} completed chunks")
        
        if completed:
            print(f"   Completed chunk range: {min(completed)} to {max(completed)}")
        
        return len(completed)
    
    def run_full_pipeline(self):
        """Run the complete pipeline for this dataset"""
        print(f"=== PROCESSING {self.config.name.upper()} ===")
        print(f"Source: {self.config.csv_file}")
        print(f"Target: {self.config.schema_name}.{self.config.table_name}")
        
        # Step 1: Split the large CSV
        chunk_files = self.split_large_csv()
        if not chunk_files:
            return False
        
        # Step 2: Process chunks into DuckDB
        duck_conn = self.process_all_chunks()
        if not duck_conn:
            return False
        
        # Step 3: Transfer to PostgreSQL
        self.parallel_transfer(duck_conn)
        
        # Clean up
        duck_conn.close()
        
        print(f"\n🎉 {self.config.name} pipeline completed successfully!")
        return True

def main():
    print("=== FLEXIBLE MULTI-DATASET PROCESSOR ===")
    print("Available datasets:")
    for key, config in DATASET_CONFIGS.items():
        print(f"  {key}: {config.name}")
    
    # Choose dataset
    dataset_key = input("\nEnter dataset key (or 'all' for all datasets): ").strip()
    
    if dataset_key == 'all':
        # Process all datasets
        for key, config in DATASET_CONFIGS.items():
            if key == 'custom_dataset':  # Skip template
                continue
                
            print(f"\n{'='*80}")
            processor = FlexibleDataProcessor(config)
            processor.run_full_pipeline()
    
    elif dataset_key in DATASET_CONFIGS:
        # Process single dataset
        config = DATASET_CONFIGS[dataset_key]
        processor = FlexibleDataProcessor(config)
        processor.run_full_pipeline()
    
    else:
        print(f"Unknown dataset key: {dataset_key}")
        print("Available keys:", list(DATASET_CONFIGS.keys()))

if __name__ == "__main__":
    main()