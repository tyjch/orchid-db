
import pyodbc
import os
import re
import logging
import json
import shutil
from collections import defaultdict
from dotenv import load_dotenv
from pathlib import Path

"""
MS Access Foreign Key Analyzer
------------------------------
This script analyzes an MS Access database to identify potential foreign key relationships
between tables by examining data patterns and column properties.

Usage:
1. Set the ACCESS_PATH in your .env file
2. Run this script to analyze the database and generate table_relationships.json
3. Run the diagram.py script to create ER diagrams from the relationships

Note on caching:
- By default, the script will clean up all cache and regenerate everything
- To enable caching (for faster subsequent runs), set force_clean=False 
  in the clean_cache_and_output() call at the bottom of this file
"""

load_dotenv()

access_path     = os.getenv('ACCESS_PATH')
access_username = os.getenv('ACCESS_USERNAME')
access_password = os.getenv('ACCESS_PASSWORD')

# Clean up function to remove cache and generated files
def clean_cache_and_output(force_clean=True):
	"""
	Clean up cache and output files to ensure fresh results.
	
	Args:
		force_clean: If True, delete all cache and output files
	"""
	script_dir = Path(__file__).parent
	
	if force_clean:
		# Define paths to clean
		cache_dir = script_dir / "cache"
		json_file = script_dir / "table_relationships.json"
		er_diagram = script_dir / "er_diagram.md"
		compact_diagram = script_dir / "compact_er_diagram.md"
		filtered_diagram = script_dir / "filtered_er_diagram.md"
		cluster_dir = script_dir / "diagram_clusters"
		
		# Remove cache directory
		if cache_dir.exists():
			logging.info(f"Cleaning cache directory: {cache_dir}")
			shutil.rmtree(cache_dir, ignore_errors=True)
		
		# Remove output files
		for file_path in [json_file, er_diagram, compact_diagram, filtered_diagram]:
			if file_path.exists():
				logging.info(f"Removing file: {file_path}")
				file_path.unlink()
		
		# Remove cluster directory
		if cluster_dir.exists():
			logging.info(f"Cleaning diagram clusters directory: {cluster_dir}")
			shutil.rmtree(cluster_dir, ignore_errors=True)
		
		logging.info("Clean-up completed. Will generate fresh results.")
	
	# Create cache directory (it was deleted if force_clean was True)
	cache_dir = script_dir / "cache"
	cache_dir.mkdir(exist_ok=True)

# Create cache directory if it doesn't exist
CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)

class AccessDB:
	def __init__(self, path, username=None, password=None):
		username = username or os.getenv('ACCESS_USERNAME')
		password = password or os.getenv('ACCESS_PASSWORD')
		
		self.connection_str = (
			f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};'
			f'DBQ={path};'
		)
		
		if username and password:
			self.connection_str += f'UID={username};PWD={password};'
			
	def get_connection(self):
		return pyodbc.connect(self.connection_str)


db = AccessDB(path=access_path)


class Table:
	def __init__(self, name, load_from_cache=True):
		self.name = name
		self.columns = {}
		self.column_types = {}  # Store column data types
		self.column_stats = {}  # Store column statistics (unique values, etc.)
		self.cache_file = CACHE_DIR / f"{self._sanitize_filename(name)}.json"
		self.loaded_from_cache = False
		
		# Try to load from cache first if requested
		if load_from_cache:
			self.loaded_from_cache = self._load_from_cache()
	
	def _sanitize_filename(self, name):
		"""Create a safe filename from table name"""
		return re.sub(r'[^\w\-_]', '_', name)
	
	def _load_from_cache(self):
		"""Load column data from cache file if it exists"""
		if self.cache_file.exists():
			try:
				with open(self.cache_file, 'r') as f:
					data = json.load(f)
					self.columns = data.get('columns', {})
					self.column_types = data.get('column_types', {})
					self.column_stats = data.get('column_stats', {})
				logging.info(f"Loaded table {self.name} from cache")
				return True
			except Exception as e:
				logging.warning(f"Failed to load cache for {self.name}: {e}")
		return False
	
	def save_to_cache(self):
		"""Save column data to cache file"""
		try:
			data = {
				'columns': self.columns,
				'column_types': self.column_types,
				'column_stats': self.column_stats
			}
			with open(self.cache_file, 'w') as f:
				json.dump(data, f, indent=2, default=str)
			logging.info(f"Saved table {self.name} to cache")
			return True
		except Exception as e:
			logging.warning(f"Failed to save cache for {self.name}: {e}")
			return False
	
	def add_column(self, name, values, col_type=None):
		"""Add a column and its values to the table"""
		self.columns[name] = values
		if col_type:
			self.column_types[name] = col_type
	
	def has_columns(self):
		"""Check if the table has loaded any columns"""
		return len(self.columns) > 0
	
	def load_from_database(self, conn):
		"""Load table data from database if not already loaded from cache"""
		# Skip database query if already loaded from cache
		if self.loaded_from_cache:
			logging.info(f"Table {self.name} already loaded from cache, skipping database query")
			return
		
		# Skip database query if columns were added some other way
		if self.has_columns():
			logging.info(f"Table {self.name} already has columns, skipping database query")
			return
		
		logging.info(f"Loading data for table {self.name} from database...")
		cursor = conn.cursor()
		
		# Get column information including data types
		columns_data = cursor.columns(table=self.name)
		
		# Need to fetch all column data before iterating
		columns_list = []
		column_types = {}
		for column_row in columns_data:
			col_name = column_row.column_name
			col_type = column_row.type_name
			columns_list.append(col_name)
			column_types[col_name] = col_type
			self.column_types[col_name] = col_type
		
		# Now process each column
		for column_name in columns_list:
			try:
				# Alternative approach to check for uniqueness - using two separate queries
				# This works better with MS Access which may not support COUNT(DISTINCT)
				try:
					# Get total count
					total_sql = f'''
					SELECT COUNT([{column_name}])
					FROM [{self.name}]
					WHERE [{column_name}] IS NOT NULL
					'''
					cursor.execute(total_sql)
					total_count = cursor.fetchone()[0]
					
					# Get distinct count using GROUP BY approach
					unique_sql = f'''
					SELECT COUNT(*)
					FROM (
						SELECT [{column_name}]
						FROM [{self.name}]
						WHERE [{column_name}] IS NOT NULL
						GROUP BY [{column_name}]
					) AS DistinctCount
					'''
					
					try:
						cursor.execute(unique_sql)
						unique_count = cursor.fetchone()[0]
					except pyodbc.Error:
						# Some versions of Access don't support subqueries
						# Fall back to a simple approach that may work in basic Access versions
						cursor.execute(f"SELECT DISTINCT [{column_name}] FROM [{self.name}] WHERE [{column_name}] IS NOT NULL")
						unique_values = cursor.fetchall()
						unique_count = len(unique_values)
					
					is_unique = (unique_count == total_count)
					
					# Check for NULL values
					null_check_sql = f'''
					SELECT COUNT(*) 
					FROM [{self.name}]
					WHERE [{column_name}] IS NULL
					'''
					cursor.execute(null_check_sql)
					null_count = cursor.fetchone()[0]
					
					# Store column statistics
					self.column_stats[column_name] = {
						'unique_count': unique_count,
						'total_count': total_count,
						'is_unique': is_unique,
						'null_count': null_count,
						'has_nulls': null_count > 0
					}
				except pyodbc.Error as e:
					logging.warning(f"Could not get uniqueness stats for {self.name}.{column_name}: {e}")
					# Set default stats assuming not unique and no nulls
					self.column_stats[column_name] = {
						'unique_count': 0,
						'total_count': 0,
						'is_unique': False,
						'null_count': 0, 
						'has_nulls': False
					}
				
				# Get top 50 most frequent values for the column
				sql = f'''
				SELECT TOP 50 [{column_name}], COUNT(*) as frequency
				FROM [{self.name}]
				WHERE [{column_name}] IS NOT NULL
				GROUP BY [{column_name}]
				ORDER BY COUNT(*) DESC
				'''
				
				# Also get value range for numeric columns
				if column_types[column_name] in ('INTEGER', 'SMALLINT', 'TINYINT', 'BIGINT', 
											  'DECIMAL', 'NUMERIC', 'FLOAT', 'REAL', 'DOUBLE'):
					try:
						range_sql = f'''
						SELECT MIN([{column_name}]), MAX([{column_name}])
						FROM [{self.name}]
						WHERE [{column_name}] IS NOT NULL
						'''
						cursor.execute(range_sql)
						min_val, max_val = cursor.fetchone()
						
						# Store range information
						if column_name not in self.column_stats:
							self.column_stats[column_name] = {}
						self.column_stats[column_name]['min_value'] = min_val
						self.column_stats[column_name]['max_value'] = max_val
					except pyodbc.Error as e:
						logging.warning(f"Could not get range stats for {self.name}.{column_name}: {e}")
				
				# Fetch the values
				try:
					cursor.execute(sql)
					values = [row[0] for row in cursor.fetchall()]
					self.add_column(column_name, values, column_types[column_name])
				except pyodbc.Error as e:
					logging.error(f"Error fetching values for column {column_name} in table {self.name}: {e}")
					# Add empty values to avoid further errors
					self.add_column(column_name, [], column_types[column_name])
				
			except pyodbc.Error as e:
				logging.error(f"Error processing column {column_name} in table {self.name}: {e}")
				# Add empty values for this column
				self.add_column(column_name, [], column_types.get(column_name, 'UNKNOWN'))
		
		# Automatically save to cache after loading all columns
		self.save_to_cache()
		
		cursor.close()
	
	def __str__(self):
		return self.name


class TableRelationshipAnalyzer:
    def __init__(self, tables):
        self.tables = tables
        self.relationships = []
        self.foreign_keys = []
    
    def get_pk_candidates(self):
        """
        Find columns that are good primary key candidates based on:
        1. Uniqueness
        2. No NULL values
        3. Appropriate data type
        """
        pk_candidates = []
        
        for table in self.tables:
            for col_name, col_type in table.column_types.items():
                # Skip if we don't have statistics
                if col_name not in table.column_stats:
                    continue
                
                stats = table.column_stats[col_name]
                
                # Ensure we have values for this column
                if not table.columns.get(col_name):
                    continue
                
                # Check if column is a good PK candidate
                # Relaxed criteria if we couldn't get good stats
                has_stats = stats.get('total_count', 0) > 0
                is_unique = stats.get('is_unique', False) if has_stats else len(table.columns.get(col_name)) > 0
                has_nulls = stats.get('has_nulls', True) if has_stats else False
                
                # Check if column type is suitable for keys
                suitable_type = col_type in ('INTEGER', 'SMALLINT', 'TINYINT', 'BIGINT', 'GUID', 'TEXT', 'VARCHAR', 'CHAR')
                
                if suitable_type and (is_unique or not has_stats) and not has_nulls:
                    values = set(str(v) for v in table.columns.get(col_name, []) if v is not None)
                    
                    pk_candidates.append({
                        'table': table.name,
                        'column': col_name,
                        'type': col_type,
                        'values': values,
                        'unique_count': stats.get('unique_count', len(values)),
                        'min_value': stats.get('min_value'),
                        'max_value': stats.get('max_value')
                    })
        
        logging.info(f"Found {len(pk_candidates)} primary key candidates")
        return pk_candidates
    
    def analyze_foreign_key_relationships(self):
        """
        Analyze tables to identify potential foreign key relationships using improved heuristics.
        """
        logging.info("Analyzing foreign key relationships between tables...")
        
        foreign_keys = []
        
        # Get primary key candidates
        pk_candidates = self.get_pk_candidates()
        
        # Now check each column to see if it could be a foreign key
        for source_table in self.tables:
            for source_col, source_type in source_table.column_types.items():
                # Skip if the column type doesn't match common FK types
                if source_type not in ('INTEGER', 'SMALLINT', 'TINYINT', 'BIGINT', 'GUID', 'TEXT', 'VARCHAR', 'CHAR'):
                    continue
                
                # Skip empty columns
                if not source_table.columns.get(source_col, []):
                    continue
                
                # Check numerical columns differently
                is_numeric = source_type in ('INTEGER', 'SMALLINT', 'TINYINT', 'BIGINT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'REAL', 'DOUBLE')
                
                # Convert to strings and filter None values for non-numeric columns
                source_values = source_table.columns.get(source_col, [])
                source_set = set(str(v) for v in source_values if v is not None)
                
                if not source_set:
                    continue
                
                # Check against all PK candidates
                for pk in pk_candidates:
                    # Skip self-reference 
                    if source_table.name == pk['table'] and source_col == pk['column']:
                        continue
                    
                    # Skip if column types are incompatible
                    if is_numeric != pk['type'] in ('INTEGER', 'SMALLINT', 'TINYINT', 'BIGINT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'REAL', 'DOUBLE'):
                        continue
                    
                    # Special handling for numeric columns (check range overlap)
                    if is_numeric:
                        # Skip if we don't have min/max stats or if they're None
                        source_stats = source_table.column_stats.get(source_col, {})
                        if ('min_value' not in source_stats or 
                            'max_value' not in source_stats or
                            'min_value' not in pk or 
                            'max_value' not in pk or
                            source_stats.get('min_value') is None or
                            source_stats.get('max_value') is None or
                            pk.get('min_value') is None or
                            pk.get('max_value') is None):
                            # Skip range check but continue with other checks
                            pass
                        else:
                            # Skip if ranges don't overlap
                            try:
                                if (source_stats['max_value'] < pk['min_value'] or 
                                    source_stats['min_value'] > pk['max_value']):
                                    continue
                            except (TypeError, ValueError):
                                # If comparison fails, skip range check
                                pass
                        
                        # For numeric columns, check if all values in our sample are in the PK
                        try:
                            values_in_pk = all(v in pk['values'] for v in source_set if v and v != '0')
                        except Exception:
                            # If check fails, assume not a match
                            continue
                        
                        # Skip simple increasing sequences (likely autoincrement IDs with coincidental overlap)
                        try:
                            if (len(source_set) > 1 and 
                                is_likely_autoincrement(source_values) and 
                                is_likely_autoincrement(list(map(try_int_cast, pk['values'])))):
                                # Skip unless there's strong evidence (like column name matches)
                                if not (source_col.lower() == pk['column'].lower() or 
                                       pk['column'].lower() in source_col.lower() or
                                       source_col.lower() in pk['column'].lower()):
                                    continue
                        except Exception as e:
                            # If autoincrement check fails, log and continue
                            logging.debug(f"Autoincrement check failed: {e}")
                    else:
                        # For non-numeric, do a standard subset check
                        try:
                            values_in_pk = source_set.issubset(pk['values'])
                        except Exception:
                            # If check fails, assume not a match
                            continue
                    
                    # Check if ALL source values exist in the PK
                    try:
                        if 'values_in_pk' in locals() and values_in_pk:
                            confidence = "high" if source_col.lower() == pk['column'].lower() else "medium"
                            
                            # Higher confidence if column names suggest a relationship
                            if (pk['table'].lower() in source_col.lower() or 
                                pk['column'].lower() in source_col.lower()):
                                confidence = "high"
                            
                            fk_relation = {
                                'fk_table': source_table.name,
                                'fk_column': source_col,
                                'pk_table': pk['table'],
                                'pk_column': pk['column'],
                                'fk_values_count': len(source_set),
                                'pk_values_count': pk['unique_count'],
                                'confidence': confidence,
                                'fk_type': source_type,
                                'pk_type': pk['type']
                            }
                            foreign_keys.append(fk_relation)
                    except Exception as e:
                        logging.warning(f"Error adding relationship: {e}")
        
        # Sort by confidence and then by table name
        self.foreign_keys = sorted(foreign_keys, key=lambda x: (0 if x['confidence'] == "high" else 1, x['fk_table'], x['fk_column']))
        return self.foreign_keys
    
    def analyze_near_matches(self, threshold=0.9):
        """
        Find columns that are almost foreign keys (most values match but not all).
        """
        logging.info(f"Analyzing near-match relationships with threshold {threshold}...")
        
        near_matches = []
        
        # Only check near matches against PK candidates
        pk_candidates = self.get_pk_candidates()
        pk_values_dict = {(pk['table'], pk['column']): pk['values'] for pk in pk_candidates}
        
        for source_table in self.tables:
            for source_col, source_values in source_table.columns.items():
                # Skip empty columns
                if not source_values:
                    continue
                
                # Get column type
                source_type = source_table.column_types.get(source_col)
                if not source_type:
                    continue
                
                # Skip numeric columns with likely autoincrement (to avoid false positives)
                try:
                    if (source_type in ('INTEGER', 'SMALLINT', 'TINYINT', 'BIGINT') and 
                        is_likely_autoincrement(source_values)):
                        continue
                except Exception:
                    # If check fails, continue with analysis
                    pass
                
                # Convert to strings and filter None values
                source_set = set(str(v) for v in source_values if v is not None)
                if not source_set:
                    continue
                
                # Check against all PK candidates
                for pk in pk_candidates:
                    # Skip self-reference
                    if source_table.name == pk['table'] and source_col == pk['column']:
                        continue
                    
                    # Skip type mismatches
                    try:
                        if not are_compatible_types(source_type, pk['type']):
                            continue
                    except Exception:
                        # If type check fails, skip this comparison
                        continue
                    
                    # Skip exact matches (these are handled by foreign key analysis)
                    try:
                        if source_set.issubset(pk['values']):
                            continue
                    except Exception:
                        # If subset check fails, continue analysis
                        pass
                    
                    # Calculate intersection and overlap ratio
                    try:
                        intersection = source_set.intersection(pk['values'])
                        overlap_ratio = len(intersection) / len(source_set) if source_set else 0
                        
                        # Check if it's a near match
                        if overlap_ratio >= threshold and overlap_ratio < 1.0:
                            near_match = {
                                'source_table': source_table.name,
                                'source_column': source_col,
                                'target_table': pk['table'],
                                'target_column': pk['column'],
                                'overlap_ratio': overlap_ratio,
                                'matched_values': len(intersection),
                                'source_values': len(source_set),
                                'target_values': len(pk['values']),
                                'missing_values': len(source_set - pk['values']),
                                'missing_value_examples': list(source_set - pk['values'])[:5]  # Show a few examples
                            }
                            near_matches.append(near_match)
                    except Exception as e:
                        logging.debug(f"Error checking near match: {e}")
                        continue
        
        self.near_matches = sorted(near_matches, key=lambda x: x['overlap_ratio'], reverse=True)
        return self.near_matches
    
    def print_foreign_keys(self):
        """Print the potential foreign key relationships found"""
        if not self.foreign_keys:
            print("No foreign key relationships found")
            return
        
        print(f"Found {len(self.foreign_keys)} potential foreign key relationships")
        print("-" * 80)
        
        # Group by FK table and column for readability
        grouped = defaultdict(list)
        for fk in self.foreign_keys:
            key = (fk['fk_table'], fk['fk_column'])
            grouped[key].append(fk)
        
        for i, ((fk_table, fk_column), relations) in enumerate(sorted(grouped.items())):
            print(f"{i+1}. {fk_table}.{fk_column} references:")
            for rel in relations:
                print(f"   → {rel['pk_table']}.{rel['pk_column']} "
                      f"({rel['fk_values_count']} values, confidence: {rel['confidence']})")
            print()
    
    def print_near_matches(self, max_results=20):
        """Print the near match relationships found"""
        if not hasattr(self, 'near_matches') or not self.near_matches:
            print("No near match relationships found")
            return
        
        print(f"Found {len(self.near_matches)} near match relationships")
        print("-" * 80)
        
        # Sort by overlap ratio (descending)
        for i, rel in enumerate(self.near_matches[:max_results]):
            print(f"{i+1}. {rel['source_table']}.{rel['source_column']} → {rel['target_table']}.{rel['target_column']}")
            print(f"   Overlap: {rel['overlap_ratio']:.2%} ({rel['matched_values']}/{rel['source_values']} values)")
            print(f"   Missing: {rel['missing_values']} values not found in target")
            if rel['missing_value_examples']:
                print(f"   Examples of missing values: {rel['missing_value_examples']}")
            print()
        
        if len(self.near_matches) > max_results:
            print(f"...and {len(self.near_matches) - max_results} more near matches")
    
    def save_results_to_file(self, filename='table_relationships.json'):
        """Save all relationships to a JSON file"""
        try:
            results = {
                'foreign_keys': self.foreign_keys,
                'near_matches': getattr(self, 'near_matches', [])
            }
            
            output_path = Path(__file__).parent / filename
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            logging.info(f"Saved {len(self.foreign_keys)} foreign keys and {len(getattr(self, 'near_matches', []))} near matches to {output_path}")
        except Exception as e:
            logging.error(f"Error saving results to file: {e}")
		
		
if __name__ == '__main__':
	# Configure logging
	logging.basicConfig(
		level=logging.INFO,
		format='%(asctime)s - %(levelname)s - %(message)s'
	)
	
	# Clean up cache and output files before running
	# Set to False if you want to keep cache for faster results
	clean_cache_and_output(force_clean=True)
	
	try:
		conn = db.get_connection()
		cursor = conn.cursor()
		
		# Get all table names
		tables_data = [row.table_name for row in cursor.tables() if row.table_type == 'TABLE']
		cursor.close()
		
		# Create Table objects with automatic cache loading
		tables = []
		for table_name in tables_data:
			try:
				table = Table(name=table_name)
				# Only load from database if not already loaded from cache
				table.load_from_database(conn)
				tables.append(table)
			except Exception as e:
				logging.error(f"Error loading table {table_name}: {e}")
		
		# Close the database connection as we no longer need it
		conn.close()
		
		# Analyze table relationships
		analyzer = TableRelationshipAnalyzer(tables)
		
		# Find strict foreign key relationships
		try:
			analyzer.analyze_foreign_key_relationships()
			analyzer.print_foreign_keys()
		except Exception as e:
			logging.error(f"Error analyzing foreign key relationships: {e}")
			import traceback
			traceback.print_exc()
		
		# Also find near matches (90% or more values match)
		try:
			analyzer.analyze_near_matches(threshold=0.9)
			analyzer.print_near_matches()
		except Exception as e:
			logging.error(f"Error analyzing near matches: {e}")
		
		# Save all relationships to file for further analysis
		analyzer.save_results_to_file()
	
	except Exception as e:
		logging.error(f"Fatal error in main routine: {e}")
		import traceback
		traceback.print_exc()