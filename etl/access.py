import pyodbc
import duckdb
import os
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

log = logger.bind(tags=['wfo-etl'])


class AccessExporter:
    def __init__(self, export_dir="datasets\\wiz\\"):
        self.access_path = os.getenv('ACCESS_PATH')
        log.debug(f"Raw ACCESS_PATH from env: {repr(self.access_path)}")
        
        self.access_username = os.getenv('ACCESS_USERNAME')
        self.access_password = os.getenv('ACCESS_PASSWORD')
        
        if not self.access_path:
            raise ValueError("ACCESS_PATH not set in .env file")
        
        if not os.path.exists(self.access_path):
            raise FileNotFoundError(f"Access database not found: {self.access_path}")
        
        log.info(f"Using Access database: {self.access_path}")
        
        self.export_dir = Path(export_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)
        
        self.conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={self.access_path};'
        if self.access_username and self.access_password:
            self.conn_str += f'UID={self.access_username};PWD={self.access_password};'
    
    @property
    def table_names(self):
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            return [row.table_name for row in cursor.tables() if row.table_type == 'TABLE']
    
    def export_table(self, table_name):
        csv_path = self.export_dir / f"{table_name}.csv"
        
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM [{table_name}]")
            
            # Get column names and data
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            
            # Use DuckDB for fast CSV export with proper column names
            duck = duckdb.connect()
            
            # Create VALUES clause with proper column aliases
            column_aliases = ", ".join([f"col{i} AS \"{col}\"" for i, col in enumerate(columns)])
            values_rows = ",".join([f"({','.join(['?' for _ in columns])})" for _ in rows])
            
            duck.execute(f"""
                CREATE TABLE temp AS 
                SELECT {column_aliases} 
                FROM (VALUES {values_rows})
            """, [item for row in rows for item in row])
            
            duck.execute(f"COPY temp TO '{csv_path}' (FORMAT CSV, HEADER)")
            
        log.info(f"Exported {table_name} -> {csv_path}")
    
    def export_all(self):
        tables = self.table_names
        for table in tables:
            self.export_table(table)
        log.success(f"Exported {len(tables)} tables to {self.export_dir}")


if __name__ == '__main__':
    exporter = AccessExporter()
    exporter.export_all()
