import pandas as pd
import os

    
def inspect_gbif_data(file_path, sample_size=1000):
    """
    Inspect the structure of a large GBIF occurrence CSV file
    """
    
    print(f"Inspecting file: {file_path}")
    print(f"File size: {os.path.getsize(file_path) / (1024**3):.2f} GB")
    print("=" * 60)
    
    # Read just the header first
    print("Reading header...")
    header_df = pd.read_csv(file_path, nrows=0, sep='\t', low_memory=False)
    
    print(f"\nTotal columns: {len(header_df.columns)}")
    print("\nColumn names:")
    for i, col in enumerate(header_df.columns):
        print(f"{i+1:2d}. {col}")
    
    print("\n" + "=" * 60)
    
    # Read a small sample to understand data types and content
    print(f"Reading first {sample_size} rows for analysis...")
    
    try:
        sample_df = pd.read_csv(file_path, nrows=sample_size, sep='\t', low_memory=False)
        
        print(f"\nSample data shape: {sample_df.shape}")
        print(f"Memory usage: {sample_df.memory_usage(deep=True).sum() / (1024**2):.2f} MB")
        
        # Show key plant-related columns if they exist
        plant_columns = ['family', 'genus', 'species', 'scientificName', 'taxonRank', 'kingdom']
        available_plant_cols = [col for col in plant_columns if col in sample_df.columns]
        
        if available_plant_cols:
            print("\nKey taxonomic columns found:")
            for col in available_plant_cols:
                unique_count = sample_df[col].nunique()
                print(f"  {col}: {unique_count} unique values in sample")
                
                # Show top values for family if it exists
                if col == 'family' and not sample_df[col].isna().all():
                    print(f"    Top families in sample: {list(sample_df[col].value_counts().head(5).index)}")
        
        # Show geographic columns
        geo_columns = ['decimalLatitude', 'decimalLongitude', 'country', 'stateProvince', 'locality']
        available_geo_cols = [col for col in geo_columns if col in sample_df.columns]
        
        if available_geo_cols:
            print("\nGeographic columns found:")
            for col in available_geo_cols:
                non_null = sample_df[col].notna().sum()
                print(f"  {col}: {non_null}/{len(sample_df)} records have data ({non_null/len(sample_df)*100:.1f}%)")
        
        # Show temporal columns
        time_columns = ['eventDate', 'year', 'month', 'day']
        available_time_cols = [col for col in time_columns if col in sample_df.columns]
        
        if available_time_cols:
            print("\nTemporal columns found:")
            for col in available_time_cols:
                non_null = sample_df[col].notna().sum()
                print(f"  {col}: {non_null}/{len(sample_df)} records have data")
                
                if col == 'year' and non_null > 0:
                    year_range = f"{sample_df['year'].min():.0f} - {sample_df['year'].max():.0f}"
                    print(f"    Year range in sample: {year_range}")
        
        # Show first few rows
        print("\nFirst 3 rows (key columns only):")
        display_cols = ['scientificName', 'family', 'genus', 'species', 'country', 'year'][:6]
        display_cols = [col for col in display_cols if col in sample_df.columns]
        
        if display_cols:
            print(sample_df[display_cols].head(3).to_string(index=False))
        
        # Estimate total rows
        print(f"\nEstimating total rows...")
        file_size_bytes = os.path.getsize(file_path)
        sample_size_bytes = len(sample_df.to_csv(sep='\t', index=False).encode('utf-8'))
        estimated_total_rows = int((file_size_bytes / sample_size_bytes) * sample_size)
        print(f"Estimated total rows: {estimated_total_rows:,}")
        
        return sample_df
        
    except Exception as e:
        print(f"Error reading sample: {e}")
        return None

def suggest_filtering_strategy(sample_df, plant_families_file=None):
    """
    Suggest how to filter the data based on structure
    """
    print("\n" + "=" * 60)
    print("FILTERING SUGGESTIONS:")
    
    if 'family' in sample_df.columns:
        families_in_sample = sample_df['family'].value_counts()
        print(f"\nFound {len(families_in_sample)} unique families in sample")
        
        if plant_families_file:
            print(f"You can filter by your families in {plant_families_file}")
            print("Consider this approach:")
            print("1. Load your plant families list")
            print("2. Filter the CSV in chunks to avoid memory issues")
            print("3. Save filtered results to a new file")
    
    # Memory-efficient processing suggestion
    print(f"\nFor memory-efficient processing of this large file:")
    print("- Use pd.read_csv() with chunksize parameter")
    print("- Process ~10,000-50,000 rows at a time")
    print("- Filter each chunk and append results")
    
    if 'decimalLatitude' in sample_df.columns and 'decimalLongitude' in sample_df.columns:
        coords_available = sample_df[['decimalLatitude', 'decimalLongitude']].notna().all(axis=1).sum()
        print(f"- {coords_available}/{len(sample_df)} records have coordinates in sample")
    
    return None

if __name__ == "__main__":
    # Main execution
    file_path = r"Z:\occurences\0016315-250515123054153.csv"  # Updated path
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        print("Please check the file path and try again.")
    else:
        # Inspect the data
        sample_data = inspect_gbif_data(file_path, sample_size=1000)
        
        if sample_data is not None:
            # Suggest filtering strategies
            suggest_filtering_strategy(sample_data, "plant_families.yaml")
            
            print(f"\nScript completed! You now have a sample of {len(sample_data)} rows to work with.")