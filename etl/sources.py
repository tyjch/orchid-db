import os
import gc
import csv
import duckdb
import psycopg
import tempfile
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
from typing import List, Tuple

load_dotenv()
log = logger.bind(tags=['sources'])

def get_file_size_gb(filepath: str) -> float:
    try:
        return os.path.getsize(filepath) / (1024**3)
    except (OSError, FileNotFoundError) as e:
        log.error(f"Could not get size for file {filepath}: {e}")
        return 0.0

def load_csv_with_duckdb_autodetect(filepath: str, conn: duckdb.DuckDBPyConnection, table_name: str = "source_data") -> int:
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            log.debug(f"Trying DuckDB auto-detect with {encoding} encoding")
            
            conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM read_csv(
                    '{filepath}',
                    auto_detect=true,
                    ignore_errors=true,
                    all_varchar=true,
                    encoding='{encoding}'
                )
            """)
            
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if count > 0:
                columns_info = conn.execute(f"DESCRIBE {table_name}").fetchall()
                column_count = len(columns_info)
                
                log.success(f"DuckDB auto-detect successful: {count:,} rows, {column_count} columns with {encoding}")
                return count
            else:
                log.debug(f"Auto-detect with {encoding} loaded 0 rows")
                
        except Exception as e:
            log.debug(f"Auto-detect failed with {encoding}: {e}")
            conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            continue
    
    raise Exception(f"Could not load {filepath} with DuckDB auto-detect using any encoding")
    return os.path.getsize(filepath) / (1024**3)

def get_files_from_path(path: str) -> List[Tuple[str, str]]:
    path_obj = Path(path)
    
    if not path_obj.exists():
        log.error(f"Path not found: {path}")
        return []
    
    if path_obj.is_file():
        return [(path_obj.stem, str(path_obj))]

    files = []
    for file_path in path_obj.rglob('*'):
        if file_path.is_file() and file_path.suffix.lower() in ['.csv', '.tsv', '.txt']:
            key = file_path.stem
            files.append((key, str(file_path)))
    
    log.info(f"Found {len(files)} files in directory: {path}")
    return files

def create_smart_filter(filter_condition: str, available_columns: List[str]) -> str:
    if not filter_condition:
        return None
    
    column_mapping = {}
    
    expected_columns = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species', 
                       'taxonomicStatus', 'taxonRank', 'scientificName']
    
    for expected in expected_columns:
        for actual in available_columns:
            if actual.lower() == expected.lower():
                quoted_name = f'"{actual}"' if ' ' in actual or actual != actual.lower() else actual
                column_mapping[expected] = quoted_name
                break

    adjusted_filter = filter_condition
    for expected, actual in column_mapping.items():
        adjusted_filter = adjusted_filter.replace(expected, actual)
    
    return adjusted_filter

def get_postgres_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

def create_postgres_table(table_name: str, columns_info: List, pg_conn: psycopg.Connection, schema_name:str = 'raw'):
    
    with pg_conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        
        clean_name = table_name.lower().replace('-', '_').replace(' ', '_')
        full_table = f"{schema_name}.{clean_name}"

        columns_def = ", ".join([f'"{col[0]}" TEXT' for col in columns_info])
        
        cur.execute(f"DROP TABLE IF EXISTS {full_table} CASCADE")
        cur.execute(f"CREATE TABLE {full_table} ({columns_def})")
        
        log.debug(f"Created table: {full_table}")
        return full_table

def repair_and_load_csv(filepath: str, conn: duckdb.DuckDBPyConnection) -> int:
    log.debug(f"Attempting to repair CSV file: {filepath}")
    
    temp_dir = tempfile.gettempdir()
    repaired_path = os.path.join(temp_dir, f"repaired_{Path(filepath).name}")
    
    try:
        with open(filepath, 'rb') as infile:
            raw_data = infile.read()

        text_data = None
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                text_data = raw_data.decode(encoding, errors='replace')
                log.debug(f"Successfully decoded with {encoding} for repair")
                break
            except:
                continue
        
        if not text_data:
            raise Exception("Could not decode file for repair")
        

        lines = text_data.split('\n')
        cleaned_lines = []
        
        for line in lines:
            cleaned = line.replace('\x00', '').replace('\r', '').strip()
            if cleaned:
                cleaned_lines.append(cleaned)
        

        with open(repaired_path, 'w', encoding='utf-8', newline='') as outfile:
            for line in cleaned_lines:
                outfile.write(line + '\n')
        
        log.debug(f"Created repaired file: {repaired_path}")

        conn.execute("DROP TABLE IF EXISTS source_data")
        conn.execute(f"""
            CREATE TABLE source_data AS 
            SELECT * FROM read_csv(
                '{repaired_path}',
                auto_detect=true,
                ignore_errors=true,
                all_varchar=true
            )
        """)
        
        count = conn.execute("SELECT COUNT(*) FROM source_data").fetchone()[0]
        if count > 0:
            log.success(f"Repaired and loaded {count:,} rows")
            return count
        else:
            raise Exception("Repaired file still has 0 rows")
            
    finally:
        if os.path.exists(repaired_path):
            os.remove(repaired_path)

def load_small_csv_to_duckdb(filepath: str, conn: duckdb.DuckDBPyConnection) -> int:
    try:
        return load_csv_with_duckdb_autodetect(filepath, conn)
    except Exception as e:
        log.warning(f"DuckDB auto-detect failed: {e}")
        log.info("Falling back to manual options...")
    
    delimiter = '\t' if Path(filepath).suffix.lower() == '.tsv' else ','
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16', 'utf-8-sig']
    
    log.debug(f"Inspecting problematic file: {filepath}")
    try:
        file_size = os.path.getsize(filepath)
        log.debug(f"File size: {file_size} bytes")
        
        with open(filepath, 'rb') as f:
            first_bytes = f.read(100)
            log.debug(f"First 100 bytes: {first_bytes}")
            
        for enc in encodings:
            try:
                with open(filepath, 'r', encoding=enc, errors='replace') as f:
                    first_lines = [f.readline() for _ in range(3)]
                    log.debug(f"First 3 lines with {enc}: {first_lines}")
                break
            except Exception as e:
                log.debug(f"Could not read with {enc}: {e}")
                
    except Exception as e:
        log.debug(f"File inspection failed: {e}")
    
    if file_size < 10:
        log.warning(f"File {filepath} is too small ({file_size} bytes), skipping")
        return 0
    
    for encoding in encodings:
        try:
            log.debug(f"Trying encoding: {encoding}")
            conn.execute("DROP TABLE IF EXISTS source_data")
            
            duckdb_options = [
                f"""
                    CREATE TABLE source_data AS 
                    SELECT * FROM read_csv(
                        '{filepath}',
                        delim='{delimiter}',
                        header=true,
                        ignore_errors=true,
                        all_varchar=true,
                        encoding='{encoding}'
                    )
                """,
                f"""
                    CREATE TABLE source_data AS 
                    SELECT * FROM read_csv(
                        '{filepath}',
                        delim='{delimiter}',
                        header=true, 
                        ignore_errors=true,
                        all_varchar=true,
                        encoding='{encoding}',
                        skip_blank_lines=true,
                        null_padding=true
                    )
                """,
                f"""
                    CREATE TABLE source_data AS 
                    SELECT * FROM read_csv(
                        '{filepath}',
                        auto_detect=true,
                        ignore_errors=true,
                        all_varchar=true,
                        encoding='{encoding}'
                    )
                """
            ]
            
            for i, sql_query in enumerate(duckdb_options):
                try:
                    log.debug(f"Trying option set {i+1}")
                    
                    conn.execute(sql_query)
                    
                    count = conn.execute("SELECT COUNT(*) FROM source_data").fetchone()[0]
                    if count > 0:
                        log.success(f"Loaded {count:,} rows with {encoding} encoding (option set {i+1})")
                        return count
                    else:
                        log.debug(f"Option set {i+1} loaded 0 rows")
                        conn.execute("DROP TABLE IF EXISTS source_data")
                        
                except Exception as e:
                    log.debug(f"Option set {i+1} failed: {e}")
                    conn.execute("DROP TABLE IF EXISTS source_data")
                    continue
                    
        except Exception as e:
            log.debug(f"Failed with {encoding}: {e}")
            continue
    
    log.warning(f"All encoding attempts failed, trying manual file repair...")
    try:
        return repair_and_load_csv(filepath, conn)
    except Exception as e:
        log.error(f"Manual repair also failed: {e}")
        raise Exception(f"Could not load {filepath} with any encoding or repair method")

def load_csv_to_duckdb(filepath: str, conn: duckdb.DuckDBPyConnection) -> int:
    file_size_gb = get_file_size_gb(filepath)
    if file_size_gb < 1.0:
        return load_small_csv_to_duckdb(filepath, conn)
    else:
        raise Exception(f"File {filepath} is too large ({file_size_gb:.1f}GB) for standard loading. Use streaming approach.")

def transfer_data_via_csv(
        duck_conn  : duckdb.DuckDBPyConnection, 
        pg_conn    : psycopg.Connection, 
        table_name : str
    ) -> int:

    temp_dir = tempfile.gettempdir()
    safe_name = table_name.replace('.', '_').replace('/', '_').replace('\\', '_')
    temp_csv = os.path.join(temp_dir, f"{safe_name}_temp.csv")
    
    try:
        duck_conn.execute(f"COPY source_data TO '{temp_csv}' (FORMAT CSV, HEADER)")
        
        with pg_conn.cursor() as cur:
            with open(temp_csv, 'r', encoding='utf-8') as f:
                with cur.copy(f"COPY {table_name} FROM STDIN WITH CSV HEADER") as copy:
                    while True:
                        data = f.read(8192)
                        if not data:
                            break
                        copy.write(data)

            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            
        pg_conn.commit()
        log.success(f"Transferred and committed {count:,} rows to {table_name}")
        return count
        
    finally:
        if os.path.exists(temp_csv):
            os.remove(temp_csv)

def insert_small_file_standard(key: str, filepath: str, filter_condition: str = None):
    duck_conn = duckdb.connect(':memory:')
    
    try:
        row_count = load_csv_to_duckdb(filepath, duck_conn)
        if filter_condition:
            original_count = duck_conn.execute("SELECT COUNT(*) FROM source_data").fetchone()[0]
            columns_info = duck_conn.execute("DESCRIBE source_data").fetchall()
            column_names = [col[0] for col in columns_info]
            
            adjusted_filter = create_smart_filter(filter_condition, column_names)
            
            if adjusted_filter:
                duck_conn.execute(f"""
                    CREATE TABLE filtered_data AS 
                    SELECT * FROM source_data 
                    WHERE {adjusted_filter}
                """)
                
                filtered_count = duck_conn.execute("SELECT COUNT(*) FROM filtered_data").fetchone()[0]
                duck_conn.execute("DROP TABLE source_data")
                duck_conn.execute("ALTER TABLE filtered_data RENAME TO source_data")
                
                log.info(f"Filter applied: {original_count:,} → {filtered_count:,} rows ({filtered_count/original_count*100:.1f}%)")
            else:
                log.warning(f"Could not create valid filter, using unfiltered data")
        
        columns_info = duck_conn.execute("DESCRIBE source_data").fetchall()
        pg_conn = get_postgres_connection()
        
        try:
            full_table = create_postgres_table(key, columns_info, pg_conn)
            final_count = transfer_data_via_csv(duck_conn, pg_conn, full_table)
            log.success(f"Successfully loaded {key}: {final_count:,} rows")
            
        finally:
            pg_conn.close()
            
    except Exception as e:
        log.error(f"Error processing {key}: {e}")
        raise
    finally:
        duck_conn.close()

def process_chunk_to_postgres(
        chunk_lines      : List[str], 
        chunk_num        : int, 
        base_name        : str,                       
        temp_dir         : str, 
        delimiter        : str, 
        pg_conn          : psycopg.Connection,
        key              : str, 
        is_first_chunk   : bool, 
        filter_condition : str = None,
        schema_name      : str = 'raw'
    ) -> dict:
 
    duck_conn = duckdb.connect(':memory:')
    chunk_path = os.path.join(temp_dir, f"{base_name}_chunk_{chunk_num:03d}.csv")
    
    try:
        with open(chunk_path, 'w', encoding='utf-8') as chunk_file:
            chunk_file.writelines(chunk_lines)
        
        log.debug(f"Processing chunk {chunk_num}: {len(chunk_lines)-1:,} lines")
        
        chunk_lines.clear()
        gc.collect()
        
        try:
            original_count = load_csv_with_duckdb_autodetect(chunk_path, duck_conn, "chunk_data")
            log.debug(f"Chunk {chunk_num} loaded via auto-detect: {original_count:,} rows")
        except Exception as e:
            log.debug(f"Auto-detect failed for chunk {chunk_num}: {e}")
            
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            
            for encoding in encodings:
                try:
                    duck_conn.execute(f"""
                        CREATE TABLE chunk_data AS 
                        SELECT * FROM read_csv(
                            '{chunk_path}',
                            delim='{delimiter}',
                            header=true,
                            ignore_errors=true,
                            all_varchar=true,
                            encoding='{encoding}'
                        )
                    """)
                    
                    original_count = duck_conn.execute("SELECT COUNT(*) FROM chunk_data").fetchone()[0]
                    if original_count > 0:
                        log.debug(f"Chunk {chunk_num} loaded with manual method: {original_count:,} rows")
                        break
                    else:
                        duck_conn.execute("DROP TABLE IF EXISTS chunk_data")
                        
                except Exception as enc_e:
                    log.debug(f"Encoding {encoding} failed for chunk {chunk_num}: {enc_e}")
                    duck_conn.execute("DROP TABLE IF EXISTS chunk_data")
                    continue
            else:
                log.warning(f"Could not load chunk {chunk_num} with any method")
                return {'rows': 0, 'original_rows': 0}
        
        filtered_count = original_count
        if filter_condition:
            try:
                columns_info = duck_conn.execute("DESCRIBE chunk_data").fetchall()
                column_names = [col[0] for col in columns_info]
                
                adjusted_filter = create_smart_filter(filter_condition, column_names)
                
                if adjusted_filter and adjusted_filter != filter_condition:
                    log.debug(f"Adjusted filter for chunk {chunk_num}: {filter_condition} → {adjusted_filter}")
                
                if adjusted_filter:
                    duck_conn.execute(f"""
                        CREATE TABLE filtered_chunk AS 
                        SELECT * FROM chunk_data 
                        WHERE {adjusted_filter}
                    """)
                    
                    filtered_count = duck_conn.execute("SELECT COUNT(*) FROM filtered_chunk").fetchone()[0]
                    duck_conn.execute("DROP TABLE chunk_data")
                    duck_conn.execute("ALTER TABLE filtered_chunk RENAME TO chunk_data")
                    
                    log.debug(f"Chunk {chunk_num} filter: {original_count:,} → {filtered_count:,} rows")
                else:
                    log.warning(f"Could not create valid filter for chunk {chunk_num}, using unfiltered data")
                
                if filtered_count == 0:
                    log.debug(f"Chunk {chunk_num} filtered to 0 rows, skipping")
                    return {'rows': 0, 'original_rows': original_count}
                    
            except Exception as e:
                log.warning(f"Filter failed for chunk {chunk_num}: {e}, using unfiltered data")
        
        if is_first_chunk:
            columns_info = duck_conn.execute("DESCRIBE chunk_data").fetchall()
            full_table = create_postgres_table(key, columns_info, pg_conn)
        else:
            clean_name = key.lower().replace('-', '_').replace(' ', '_')
            full_table = f"{schema_name}.{clean_name}"
        
        temp_csv = os.path.join(temp_dir, f"{base_name}_transfer_{chunk_num:03d}.csv")
        
        try:
            duck_conn.execute(f"COPY chunk_data TO '{temp_csv}' (FORMAT CSV, HEADER)")
            
            with pg_conn.cursor() as cur:
                with open(temp_csv, 'r', encoding='utf-8') as f:
                    if is_first_chunk:
                        with cur.copy(f"COPY {full_table} FROM STDIN WITH CSV HEADER") as copy:
                            while True:
                                data = f.read(8192)
                                if not data:
                                    break
                                copy.write(data)
                    else:
                        
                        next(f)
                        with cur.copy(f"COPY {full_table} FROM STDIN WITH CSV") as copy:
                            while True:
                                data = f.read(8192)
                                if not data:
                                    break
                                copy.write(data)
            
            log.debug(f"Transferred chunk {chunk_num}: {filtered_count:,} rows")
            return {'rows': filtered_count, 'original_rows': original_count}
            
        finally:
            if os.path.exists(temp_csv):
                os.remove(temp_csv)
        
    finally:
        if os.path.exists(chunk_path):
            os.remove(chunk_path)
        duck_conn.close()
        gc.collect()

def insert_large_file_streaming(key: str, filepath: str, filter_condition: str = None):
    log.info(f"Large file detected, using streaming approach")
    if filter_condition:
        log.info(f"Will apply filter to each chunk: {filter_condition}")

    pg_conn = get_postgres_connection()
    
    try:
        table_created = False
        full_table = None
        total_rows = 0
        total_filtered_rows = 0
        
        temp_dir = tempfile.gettempdir()
        base_name = Path(filepath).stem
        target_chunk_size = 500 * 1024 * 1024
        chunk_num = 0
        
        with open(filepath, 'r', encoding='utf-8', errors='replace') as infile:
            header = infile.readline()
            
            current_chunk_size = 0
            chunk_lines = [header]
            
            for line in infile:
                chunk_lines.append(line)
                current_chunk_size += len(line.encode('utf-8'))
                
                if current_chunk_size >= target_chunk_size:
                    chunk_num += 1
                    
                    chunk_result = process_chunk_to_postgres(
                        chunk_lines, chunk_num, base_name, temp_dir,
                        None, pg_conn, key,
                        is_first_chunk=not table_created,
                        filter_condition=filter_condition
                    )
                    
                    if chunk_result and chunk_result['rows'] > 0:
                        total_rows += chunk_result['original_rows']
                        total_filtered_rows += chunk_result['rows']
                        table_created = True
                        
                        pg_conn.commit()
                        
                        if filter_condition:
                            filter_pct = chunk_result['rows'] / chunk_result['original_rows'] * 100 if chunk_result['original_rows'] > 0 else 0
                            log.success(f"Committed chunk {chunk_num}: {chunk_result['original_rows']:,} → {chunk_result['rows']:,} rows ({filter_pct:.1f}%) (total: {total_filtered_rows:,})")
                        else:
                            log.success(f"Committed chunk {chunk_num}: {chunk_result['rows']:,} rows (total: {total_filtered_rows:,})")
                    
                    chunk_lines.clear()
                    chunk_lines = [header]
                    current_chunk_size = len(header.encode('utf-8'))
                    gc.collect()
            
            if len(chunk_lines) > 1:
                chunk_num += 1
                chunk_result = process_chunk_to_postgres(
                    chunk_lines, chunk_num, base_name, temp_dir,
                    None, pg_conn, key,
                    is_first_chunk=not table_created,
                    filter_condition=filter_condition
                )
                
                if chunk_result and chunk_result['rows'] > 0:
                    total_rows += chunk_result['original_rows']
                    total_filtered_rows += chunk_result['rows']
                    pg_conn.commit()
                    
                    if filter_condition:
                        filter_pct = chunk_result['rows'] / chunk_result['original_rows'] * 100 if chunk_result['original_rows'] > 0 else 0
                        log.success(f"Committed final chunk {chunk_num}: {chunk_result['original_rows']:,} → {chunk_result['rows']:,} rows ({filter_pct:.1f}%) (total: {total_filtered_rows:,})")
                    else:
                        log.success(f"Committed final chunk {chunk_num}: {chunk_result['rows']:,} rows (total: {total_filtered_rows:,})")
                
                chunk_lines.clear()
                gc.collect()
        
        if filter_condition and total_rows > 0:
            overall_pct = total_filtered_rows / total_rows * 100
            log.success(f"Successfully filtered and streamed {key}: {total_rows:,} → {total_filtered_rows:,} rows ({overall_pct:.1f}%) in {chunk_num} chunks")
        else:
            log.success(f"Successfully streamed {key}: {total_filtered_rows:,} rows in {chunk_num} chunks")
        
        return total_filtered_rows
        
    except Exception as e:
        log.error(f"Error streaming {key}: {e}")
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()

def insert_single_file(key: str, filepath: str, filter_condition: str = None):
    log.info(f"Processing {key} from {filepath}")
    if filter_condition:
        log.info(f"Applying filter: {filter_condition}")
    
    if not os.path.exists(filepath):
        log.error(f"File not found: {filepath}")
        return 0
    
    file_size_gb = get_file_size_gb(filepath)
    log.debug(f"File size: {file_size_gb:.2f} GB")
    
    if file_size_gb > 1.0:
        return insert_large_file_streaming(key, filepath, filter_condition)
    else:
        return insert_small_file_standard(key, filepath, filter_condition)

def insert_source(key: str, path: str, filter_condition: str = None):
    files = get_files_from_path(path)
    
    if not files:
        log.warning(f"No files found for {key} at {path}")
        return
    
    for file_key, filepath in files:
        final_key = key if len(files) == 1 else f"{key}_{file_key}"
        insert_single_file(final_key, filepath, filter_condition)


if __name__ == "__main__":
    paths = {
        'wfo_taxonomy': r'datasets\wfo\wfo.csv',
        'gbif_taxonomy': r'datasets\gbif\backbone\Taxon.tsv', 
        #'gbif_occurences': r'datasets\gbif\occurences\occurences.csv',
        'wiz_species': r'datasets\wiz\AcceptedSpecies.csv',
        'wiz_genera' : r'datasets\wiz\XGenera.csv',
        'wiz_classifications' : r'datasets\wiz\Classifications.csv',
        'wiz_infras' : r'datasets\wiz\Infras.csv',
        'wiz_species_names' : r'datasets\wiz\SpeciesNames.csv'
    }
    
    for key, path in paths.items():
        if 'occurences' in key:
            insert_source(key, path, filter_condition="kingdom = 'Plantae'")
        else:
            insert_source(key, path)