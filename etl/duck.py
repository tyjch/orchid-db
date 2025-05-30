import duckdb
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class SimpleCSVLoader:
    def __init__(self, db_path="csv_data.duckdb"):
        self.conn = duckdb.connect(db_path)
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS raw_data")
    
    def load_csv(self, csv_path, table_name=None):
        """Load CSV with fallback strategies for problematic files"""
        if table_name is None:
            table_name = Path(csv_path).stem.replace('-', '_').replace(' ', '_').lower()
        
        full_table = f"raw_data.{table_name}"
        
        # Strategy 1: Force date columns as VARCHAR
        try:
            date_cols = ['eventDate', 'dateIdentified', 'modified', 'year', 'month', 'day']
            types = {col: 'VARCHAR' for col in date_cols}
            
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {full_table} AS 
                SELECT * FROM read_csv('{csv_path}', auto_detect=true, types={types})
            """)
            print(f"✅ Loaded {table_name} with date overrides")
            
        except Exception:
            # Strategy 2: All columns as VARCHAR
            try:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {full_table} AS 
                    SELECT * FROM read_csv('{csv_path}', all_varchar=true)
                """)
                print(f"✅ Loaded {table_name} as all VARCHAR")
                
            except Exception as e:
                print(f"❌ Failed to load {table_name}: {e}")
                return None
        
        # Get row count
        count = self.conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
        print(f"📊 {count:,} rows loaded")
        return full_table
    
    def list_tables(self):
        """List all loaded tables"""
        tables = self.conn.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'raw_data'
        """).fetchall()
        
        for (table,) in tables:
            count = self.conn.execute(f"SELECT COUNT(*) FROM raw_data.{table}").fetchone()[0]
            print(f"📋 {table}: {count:,} rows")
        
        return [t[0] for t in tables]
    
    def query(self, sql):
        """Execute SQL query"""
        return self.conn.execute(sql).fetchall()
    
    def sample(self, table_name, limit=5):
        """Show sample data"""
        if '.' not in table_name:
            table_name = f"raw_data.{table_name}"
        
        result = self.conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchall()
        columns = [desc[0] for desc in self.conn.description]
        
        for i, row in enumerate(result):
            print(f"Row {i+1}: {dict(zip(columns[:5], row[:5]))}")
    
    def transform_to_taxonomy(self, source_table, target_table="transformed_taxa"):
        """Transform raw data to taxonomy format"""
        if '.' not in source_table:
            source_table = f"gbif_data.{source_table}"
        
        # Auto-detect column mapping
        columns = self.conn.execute(f"DESCRIBE {source_table}").fetchall()
        col_names = [c[0].lower() for c in columns]
        
        mapping = {}
        if 'taxonid' in col_names:
            mapping['id'] = 'taxonID'
        if 'scientificname' in col_names:
            mapping['name'] = 'scientificName'
        if 'taxonrank' in col_names:
            mapping['rank'] = 'taxonRank'
        if 'taxonomicstatus' in col_names:
            mapping['status'] = 'taxonomicStatus'
        if 'family' in col_names:
            mapping['family'] = 'family'
        
        if not mapping:
            print("❌ Could not detect taxonomy columns")
            return None
        
        # Build transform query
        select_parts = []
        for target_col, source_col in mapping.items():
            if target_col == 'status':
                select_parts.append(f"""
                    CASE 
                        WHEN {source_col} ILIKE '%accepted%' THEN 'accepted'
                        WHEN {source_col} ILIKE '%synonym%' THEN 'synonym'  
                        WHEN {source_col} ILIKE '%invalid%' THEN 'invalid'
                        ELSE 'unresolved'
                    END as {target_col}
                """)
            else:
                select_parts.append(f"{source_col} as {target_col}")
        
        query = f"""
            CREATE OR REPLACE TABLE {target_table} AS
            SELECT {', '.join(select_parts)}
            FROM {source_table}
            WHERE {mapping.get('id', 'NULL')} IS NOT NULL
            LIMIT 10000
        """
        
        self.conn.execute(query)
        count = self.conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
        print(f"✅ Transformed {count:,} records to {target_table}")
        return target_table

if __name__ == "__main__":
    csv_file = r"Z:\occurences\0016315-250515123054153.csv"
    
    loader = SimpleCSVLoader("gbif_data.duckdb")
    
    # Load CSV
    table = loader.load_csv(csv_file, "gbif_occurrences")
    
    if table:
        # Show what we have
        loader.list_tables()
        
        # Sample data
        loader.sample("gbif_occurrences", 3)
        
        # Transform to taxonomy format
        loader.transform_to_taxonomy("gbif_occurrences")
        
        # Show transformed data
        loader.sample("transformed_taxa", 3)