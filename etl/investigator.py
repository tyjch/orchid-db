import csv
import pandas as pd
import numpy as np
from collections import Counter, defaultdict
import re
from datetime import datetime
from pathlib import Path
import json

class CSVInvestigator:
    """
    Comprehensive CSV analysis tool for Postgres migration planning.
    Analyzes structure, data types, quality issues, and provides migration recommendations.
    """
    
    def __init__(self, csv_path, sample_size=10000, delimiter=None, encoding='utf-8'):
        """
        Initialize the investigator with a CSV file.
        
        Args:
            csv_path: Path to the CSV file
            sample_size: Number of rows to analyze (None for all rows)
            delimiter: CSV delimiter (auto-detected if None)
            encoding: File encoding
        """
        self.csv_path = Path(csv_path)
        self.sample_size = sample_size
        self.delimiter = delimiter
        self.encoding = encoding
        self.df = None
        self.analysis = {}
        
    def investigate(self):
        """Run the full investigation and return results."""
        print(f"🔍 Investigating CSV: {self.csv_path.name}")
        print("=" * 60)
        
        # Load and analyze the CSV
        self._load_csv()
        self._analyze_structure()
        self._analyze_columns()
        self._detect_relationships()
        self._check_data_quality()
        self._suggest_postgres_schema()
        
        return self.analysis
    
    def _load_csv(self):
        """Load the CSV file with proper detection of delimiter and encoding."""
        try:
            # Try to detect delimiter if not provided
            if self.delimiter is None:
                self.delimiter = self._detect_delimiter()
                print(f"📋 Detected delimiter: '{self.delimiter}' ({repr(self.delimiter)})")
            
            # Load with pandas
            if self.sample_size:
                self.df = pd.read_csv(
                    self.csv_path, 
                    delimiter=self.delimiter,
                    encoding=self.encoding,
                    nrows=self.sample_size,
                    low_memory=False
                )
                print(f"📊 Loaded {len(self.df):,} rows (sample of first {self.sample_size:,})")
            else:
                self.df = pd.read_csv(
                    self.csv_path,
                    delimiter=self.delimiter, 
                    encoding=self.encoding,
                    low_memory=False
                )
                print(f"📊 Loaded {len(self.df):,} rows (complete file)")
                
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            raise
    
    def _detect_delimiter(self):
        """Robust delimiter detection with multiple fallback strategies."""
        delimiters_to_try = ['\t', ',', ';', '|', ' ']
        
        try:
            # First, try the csv.Sniffer
            with open(self.csv_path, 'r', encoding=self.encoding) as f:
                sample = f.read(8192)  # Larger sample
                if sample.strip():  # Make sure we have content
                    sniffer = csv.Sniffer()
                    try:
                        detected = sniffer.sniff(sample, delimiters='\t,;| ')
                        return detected.delimiter
                    except:
                        pass
        except:
            pass
        
        # Fallback: Count occurrences of common delimiters
        try:
            with open(self.csv_path, 'r', encoding=self.encoding) as f:
                # Read first few lines to analyze
                lines = []
                for i, line in enumerate(f):
                    if i >= 5:  # Just check first 5 lines
                        break
                    lines.append(line.strip())
                
                if not lines:
                    return ','  # Default fallback
                
                # Count delimiter occurrences
                delimiter_counts = {}
                for delimiter in delimiters_to_try:
                    counts = [line.count(delimiter) for line in lines if line]
                    if counts and max(counts) > 0:
                        # Good delimiter should have consistent counts across lines
                        avg_count = sum(counts) / len(counts)
                        consistency = 1 - (max(counts) - min(counts)) / (max(counts) + 1)
                        delimiter_counts[delimiter] = avg_count * consistency
                
                if delimiter_counts:
                    best_delimiter = max(delimiter_counts.items(), key=lambda x: x[1])[0]
                    return best_delimiter
                    
        except Exception as e:
            print(f"⚠️  Delimiter detection failed: {e}")
        
        # Final fallback based on filename patterns
        filename = self.csv_path.name.lower()
        if 'gbif' in filename or any(char.isdigit() for char in filename):
            print("🔍 GBIF-style filename detected, trying tab delimiter")
            return '\t'
        
        # Ultimate fallback
        return ','
    
    def _analyze_structure(self):
        """Analyze basic structure of the CSV."""
        file_size = self.csv_path.stat().st_size
        
        self.analysis['structure'] = {
            'file_size_mb': round(file_size / (1024 * 1024), 2),
            'total_rows': len(self.df),
            'total_columns': len(self.df.columns),
            'column_names': list(self.df.columns),
            'delimiter': self.delimiter,
            'encoding': self.encoding
        }
        
        print(f"\n📏 STRUCTURE ANALYSIS")
        print(f"File size: {self.analysis['structure']['file_size_mb']} MB")
        print(f"Rows: {self.analysis['structure']['total_rows']:,}")
        print(f"Columns: {self.analysis['structure']['total_columns']}")
        print(f"Delimiter: '{self.delimiter}'")
    
    def _analyze_columns(self):
        """Detailed analysis of each column."""
        self.analysis['columns'] = {}
        
        print(f"\n🔍 COLUMN ANALYSIS")
        print("-" * 60)
        
        for col in self.df.columns:
            col_analysis = self._analyze_single_column(col)
            self.analysis['columns'][col] = col_analysis
            self._print_column_summary(col, col_analysis)
    
    def _analyze_single_column(self, col):
        """Analyze a single column in detail."""
        series = self.df[col]
        
        # Basic stats
        total_count = len(series)
        null_count = series.isnull().sum()
        unique_count = series.nunique()
        
        # Data type detection
        postgres_type = self._suggest_postgres_type(series)
        
        # Sample values
        non_null_series = series.dropna()
        sample_values = non_null_series.head(5).tolist() if len(non_null_series) > 0 else []
        
        # Value distribution for categorical-like columns
        value_counts = None
        if unique_count <= 50 and unique_count > 1:  # Categorical-like
            value_counts = series.value_counts().head(10).to_dict()
        
        # String analysis for text columns
        string_analysis = None
        if series.dtype == 'object':
            string_analysis = self._analyze_string_column(non_null_series)
        
        # Numeric analysis
        numeric_analysis = None
        if pd.api.types.is_numeric_dtype(series):
            numeric_analysis = self._analyze_numeric_column(series)
        
        # Pattern analysis
        patterns = self._analyze_patterns(non_null_series)
        
        return {
            'pandas_dtype': str(series.dtype),
            'postgres_type': postgres_type,
            'total_count': total_count,
            'null_count': null_count,
            'null_percentage': round((null_count / total_count) * 100, 2),
            'unique_count': unique_count,
            'unique_percentage': round((unique_count / total_count) * 100, 2),
            'sample_values': sample_values,
            'value_counts': value_counts,
            'string_analysis': string_analysis,
            'numeric_analysis': numeric_analysis,
            'patterns': patterns
        }
    
    def _analyze_string_column(self, series):
        """Analyze string patterns and characteristics."""
        if len(series) == 0:
            return None
            
        lengths = series.astype(str).str.len()
        
        return {
            'min_length': int(lengths.min()),
            'max_length': int(lengths.max()),
            'avg_length': round(lengths.mean(), 1),
            'contains_numbers': bool(series.astype(str).str.contains(r'\d').any()),
            'contains_special_chars': bool(series.astype(str).str.contains(r'[^a-zA-Z0-9\s]').any()),
            'all_uppercase': bool(series.astype(str).str.isupper().all()),
            'all_lowercase': bool(series.astype(str).str.islower().all()),
        }
    
    def _analyze_numeric_column(self, series):
        """Analyze numeric column characteristics."""
        numeric_series = pd.to_numeric(series, errors='coerce')
        non_null_numeric = numeric_series.dropna()
        
        if len(non_null_numeric) == 0:
            return None
            
        return {
            'min_value': float(non_null_numeric.min()),
            'max_value': float(non_null_numeric.max()),
            'mean_value': round(float(non_null_numeric.mean()), 3),
            'median_value': float(non_null_numeric.median()),
            'has_decimals': bool((non_null_numeric % 1 != 0).any()),
            'negative_values': bool((non_null_numeric < 0).any()),
            'zero_values': int((non_null_numeric == 0).sum())
        }
    
    def _analyze_patterns(self, series):
        """Analyze common patterns in the data."""
        if len(series) == 0:
            return {}
        
        string_series = series.astype(str)
        patterns = {}
        
        # Common patterns
        pattern_tests = {
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'phone': r'^\+?[\d\s\-\(\)]{7,}$',
            'url': r'^https?://',
            'uuid': r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            'date_iso': r'^\d{4}-\d{2}-\d{2}',
            'time': r'\d{2}:\d{2}',
            'scientific_name': r'^[A-Z][a-z]+ [a-z]+',
            'wfo_id': r'^wfo-\d+$',
            'numeric_id': r'^\d+$',
            'mixed_alphanumeric': r'^[a-zA-Z0-9]+$'
        }
        
        for pattern_name, pattern in pattern_tests.items():
            matches = string_series.str.match(pattern, na=False).sum()
            if matches > 0:
                patterns[pattern_name] = {
                    'matches': int(matches),
                    'percentage': round((matches / len(series)) * 100, 1)
                }
        
        return patterns
    
    def _suggest_postgres_type(self, series):
        """Suggest appropriate PostgreSQL data type."""
        # Handle null-only columns
        if series.isnull().all():
            return "TEXT"
        
        non_null_series = series.dropna()
        
        # Try to convert to numeric
        numeric_series = pd.to_numeric(series, errors='coerce')
        numeric_percentage = (1 - numeric_series.isnull().sum() / len(series)) * 100
        
        # If mostly numeric, suggest numeric types
        if numeric_percentage > 90:
            non_null_numeric = numeric_series.dropna()
            if len(non_null_numeric) > 0:
                # Check if all values are integers
                if (non_null_numeric % 1 == 0).all():
                    min_val = non_null_numeric.min()
                    max_val = non_null_numeric.max()
                    
                    if min_val >= -32768 and max_val <= 32767:
                        return "SMALLINT"
                    elif min_val >= -2147483648 and max_val <= 2147483647:
                        return "INTEGER"
                    else:
                        return "BIGINT"
                else:
                    return "NUMERIC"
        
        # Try datetime
        try:
            pd.to_datetime(non_null_series, errors='raise')
            return "TIMESTAMP"
        except:
            pass
        
        # Try boolean
        if series.dtype == 'bool' or set(non_null_series.astype(str).str.lower().unique()).issubset({'true', 'false', 't', 'f', '1', '0', 'yes', 'no'}):
            return "BOOLEAN"
        
        # String analysis
        if series.dtype == 'object':
            max_length = non_null_series.astype(str).str.len().max()
            unique_ratio = series.nunique() / len(series)
            
            # Very short and limited values - might be enum
            if max_length <= 20 and unique_ratio < 0.1 and series.nunique() <= 20:
                return f"VARCHAR({max_length}) -- Consider ENUM"
            
            # Short strings
            elif max_length <= 255:
                return f"VARCHAR({max_length})"
            
            # Long strings
            else:
                return "TEXT"
        
        return "TEXT"
    
    def _detect_relationships(self):
        """Detect potential relationships between columns."""
        self.analysis['relationships'] = {}
        
        print(f"\n🔗 RELATIONSHIP ANALYSIS")
        print("-" * 40)
        
        # Look for potential foreign keys
        potential_fks = []
        id_columns = [col for col in self.df.columns if 'id' in col.lower()]
        
        for col in id_columns:
            # Check if values in this column exist in other ID columns
            for other_col in id_columns:
                if col != other_col:
                    overlap = set(self.df[col].dropna()) & set(self.df[other_col].dropna())
                    if len(overlap) > 0:
                        overlap_percentage = len(overlap) / max(self.df[col].nunique(), 1) * 100
                        if overlap_percentage > 10:  # Significant overlap
                            potential_fks.append({
                                'child_column': col,
                                'parent_column': other_col,
                                'overlap_percentage': round(overlap_percentage, 1),
                                'overlap_count': len(overlap)
                            })
        
        self.analysis['relationships']['potential_foreign_keys'] = potential_fks
        
        if potential_fks:
            print("Potential foreign key relationships:")
            for fk in potential_fks:
                print(f"  {fk['child_column']} → {fk['parent_column']} ({fk['overlap_percentage']}% overlap)")
        else:
            print("No obvious foreign key relationships detected")
    
    def _check_data_quality(self):
        """Check for data quality issues."""
        self.analysis['data_quality'] = {}
        issues = []
        
        print(f"\n⚠️  DATA QUALITY ISSUES")
        print("-" * 40)
        
        # Check for completely empty columns
        empty_cols = [col for col in self.df.columns if self.df[col].isnull().all()]
        if empty_cols:
            issues.append(f"Empty columns: {', '.join(empty_cols)}")
        
        # Check for high null percentages
        high_null_cols = []
        for col in self.df.columns:
            null_pct = (self.df[col].isnull().sum() / len(self.df)) * 100
            if null_pct > 50:
                high_null_cols.append(f"{col} ({null_pct:.1f}%)")
        
        if high_null_cols:
            issues.append(f"High null percentages: {', '.join(high_null_cols)}")
        
        # Check for duplicate rows
        duplicate_count = self.df.duplicated().sum()
        if duplicate_count > 0:
            issues.append(f"Duplicate rows: {duplicate_count:,}")
        
        # Check for mixed data types in object columns
        mixed_type_cols = []
        for col in self.df.select_dtypes(include=['object']).columns:
            sample = self.df[col].dropna().head(100)
            types = set(type(x).__name__ for x in sample)
            if len(types) > 1:
                mixed_type_cols.append(f"{col} ({', '.join(types)})")
        
        if mixed_type_cols:
            issues.append(f"Mixed data types: {', '.join(mixed_type_cols)}")
        
        self.analysis['data_quality']['issues'] = issues
        
        if issues:
            for issue in issues:
                print(f"  ⚠️  {issue}")
        else:
            print("  ✅ No major data quality issues detected")
    
    def _suggest_postgres_schema(self):
        """Generate PostgreSQL CREATE TABLE statement."""
        table_name = self.csv_path.stem.lower().replace(' ', '_').replace('-', '_')
        
        print(f"\n🗄️  SUGGESTED POSTGRESQL SCHEMA")
        print("-" * 50)
        
        create_statement = f"CREATE TABLE {table_name} (\n"
        columns = []
        
        for col_name, col_info in self.analysis['columns'].items():
            # Clean column name for PostgreSQL
            clean_name = col_name.lower().replace(' ', '_').replace('-', '_')
            clean_name = re.sub(r'[^a-z0-9_]', '', clean_name)
            
            postgres_type = col_info['postgres_type']
            
            # Add NOT NULL if very few nulls
            null_constraint = ""
            if col_info['null_percentage'] < 5:
                null_constraint = " NOT NULL"
            
            columns.append(f"    {clean_name:<30} {postgres_type}{null_constraint}")
        
        create_statement += ",\n".join(columns)
        create_statement += "\n);"
        
        self.analysis['postgres_schema'] = {
            'table_name': table_name,
            'create_statement': create_statement
        }
        
        print(create_statement)
        
        # Suggest indexes
        print(f"\n-- Suggested indexes:")
        for col_name, col_info in self.analysis['columns'].items():
            clean_name = col_name.lower().replace(' ', '_').replace('-', '_')
            clean_name = re.sub(r'[^a-z0-9_]', '', clean_name)
            
            # Suggest index for columns that look like IDs or have high uniqueness
            if ('id' in col_name.lower() or 
                col_info['unique_percentage'] > 80 or
                any('id' in pattern for pattern in col_info.get('patterns', {}))):
                print(f"CREATE INDEX idx_{table_name}_{clean_name} ON {table_name}({clean_name});")
    
    def _print_column_summary(self, col, analysis):
        """Print a summary for a single column."""
        print(f"\n📊 {col}")
        print(f"   Type: {analysis['pandas_dtype']} → {analysis['postgres_type']}")
        print(f"   Nulls: {analysis['null_count']:,} ({analysis['null_percentage']}%)")
        print(f"   Unique: {analysis['unique_count']:,} ({analysis['unique_percentage']}%)")
        
        if analysis['sample_values']:
            sample_str = ', '.join(str(v)[:30] for v in analysis['sample_values'][:3])
            print(f"   Sample: {sample_str}")
        
        if analysis['patterns']:
            pattern_str = ', '.join([f"{k}:{v['percentage']}%" 
                                   for k, v in analysis['patterns'].items() 
                                   if v['percentage'] > 10])
            if pattern_str:
                print(f"   Patterns: {pattern_str}")
        
        if analysis['string_analysis']:
            sa = analysis['string_analysis']
            print(f"   Length: {sa['min_length']}-{sa['max_length']} (avg: {sa['avg_length']})")
        
        if analysis['numeric_analysis']:
            na = analysis['numeric_analysis']
            print(f"   Range: {na['min_value']} to {na['max_value']} (avg: {na['mean_value']})")
    
    def export_analysis(self, output_path=None):
        """Export the analysis to a JSON file."""
        if output_path is None:
            output_path = self.csv_path.with_suffix('.analysis.json')
        
        # Convert numpy types to native Python types for JSON serialization
        analysis_copy = json.loads(json.dumps(self.analysis, default=str))
        
        with open(output_path, 'w') as f:
            json.dump(analysis_copy, f, indent=2)
        
        print(f"\n💾 Analysis exported to: {output_path}")
        return output_path

# Convenience function
def investigate_csv(csv_path, sample_size=10000, delimiter=None, encoding='utf-8'):
    """
    Quick function to investigate a CSV file.
    
    Args:
        csv_path: Path to CSV file
        sample_size: Number of rows to analyze (None for all)
        delimiter: CSV delimiter (auto-detected if None)
        encoding: File encoding
    
    Returns:
        Dictionary containing full analysis
    """
    investigator = CSVInvestigator(csv_path, sample_size, delimiter, encoding)
    return investigator.investigate()

# Example usage
if __name__ == "__main__":
    # Update this path to your actual file
    csv_file = r"Z:\occurences\0016315-250515123054153.csv"
    csv_file = r"datasets\backbone\Taxon.tsv"
    
    # Check if file exists
    import os
    if not os.path.exists(csv_file):
        print(f"❌ File not found: {csv_file}")
        print("\n📝 Please update the csv_file variable with your actual file path")
        print("Example:")
        print('csv_file = r"Z:\\occurences\\0016315-250515123054153.csv"')
        exit(1)
    
    # Check file size first
    file_size_gb = os.path.getsize(csv_file) / (1024**3)
    print(f"📁 File size: {file_size_gb:.2f} GB")
    
    # Adjust sample size based on file size
    if file_size_gb > 2:
        sample_size = 1000
        print("🔍 Large file detected - using small sample of 1,000 rows")
    elif file_size_gb > 0.5:
        sample_size = 5000
        print("🔍 Medium file detected - using sample of 5,000 rows")
    else:
        sample_size = 10000
    
    try:
        # Try with different encodings and delimiters for GBIF files
        encodings_to_try = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        delimiters_to_try = [None, '\t', ',']  # None means auto-detect
        
        success = False
        for encoding in encodings_to_try:
            for delimiter in delimiters_to_try:
                try:
                    print(f"\n🔄 Trying encoding: {encoding}, delimiter: {repr(delimiter) if delimiter else 'auto-detect'}")
                    
                    investigator = CSVInvestigator(
                        csv_path=csv_file,
                        sample_size=sample_size,
                        delimiter=delimiter,
                        encoding=encoding
                    )
                    
                    analysis = investigator.investigate()
                    success = True
                    break
                    
                except Exception as e:
                    print(f"   ❌ Failed: {e}")
                    continue
            
            if success:
                break
        
        if success:
            print(f"\n✅ Investigation complete!")
            
            # Export analysis
            output_file = csv_file.replace('.csv', '_analysis.json')
            investigator.export_analysis(output_file)
            
        else:
            print(f"\n❌ Could not load the file with any combination of encoding/delimiter")
            print("Try manually specifying the delimiter:")
            print("investigator = CSVInvestigator(csv_path, delimiter='\\t', encoding='utf-8')")
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print("\n🔧 Troubleshooting suggestions:")
        print("1. Check if the file path is correct")
        print("2. Ensure you have read permissions")
        print("3. Try opening the file in a text editor to see the format")
        print("4. For GBIF files, try delimiter='\\t' explicitly")