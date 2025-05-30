#!/usr/bin/env python3
"""
Debug WFO Import Process  
========================

This script analyzes your WFO import process to identify why parent relationships
aren't being built properly.
"""

import os
import csv
import zipfile
import glob
from collections import defaultdict, Counter
from dotenv import load_dotenv

load_dotenv()

def analyze_wfo_file_structure():
    """Analyze the structure of WFO files to understand the data"""
    print("🔍 ANALYZING WFO FILE STRUCTURE")
    print("=" * 50)
    
    base_path = r"datasets\World Flora Online\families"
    
    # Find a sample WFO file
    pattern = f"{base_path}/*.zip"
    files = glob.glob(pattern)
    
    if not files:
        print(f"❌ No WFO files found in {base_path}")
        return None
    
    print(f"Found {len(files)} WFO files")
    sample_file = files[0]
    print(f"Analyzing sample file: {os.path.basename(sample_file)}")
    
    return sample_file

def analyze_classification_csv(zip_path, limit=1000):
    """Analyze the classification.csv structure in a WFO file"""
    print(f"\n📊 ANALYZING CLASSIFICATION.CSV STRUCTURE")
    print("=" * 50)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            with z.open('classification.csv') as f:
                content = f.read().decode('utf-8')
                csv_reader = csv.DictReader(content.splitlines())
                
                # Get column names
                columns = csv_reader.fieldnames
                print(f"Columns in CSV: {len(columns)}")
                for i, col in enumerate(columns):
                    print(f"  {i+1:2d}. {col}")
                
                # Analyze sample records
                records = []
                parent_ids = set()
                taxon_ids = set()
                parent_child_pairs = []
                
                for i, row in enumerate(csv_reader):
                    if i >= limit:
                        break
                    records.append(row)
                    
                    taxon_id = row.get('taxonID')
                    parent_id = row.get('parentNameUsageID')
                    
                    if taxon_id:
                        taxon_ids.add(taxon_id)
                    if parent_id:
                        parent_ids.add(parent_id)
                        parent_child_pairs.append((parent_id, taxon_id))
        
        print(f"\nAnalyzed {len(records)} records:")
        print(f"  Unique taxon IDs: {len(taxon_ids)}")
        print(f"  Unique parent IDs: {len(parent_ids)}")
        print(f"  Parent-child pairs: {len(parent_child_pairs)}")
        
        # Check how many parent IDs actually exist as taxon IDs
        matching_parents = parent_ids.intersection(taxon_ids)
        orphaned_parents = parent_ids - taxon_ids
        
        print(f"  Parent IDs that exist as taxon IDs: {len(matching_parents)}")
        print(f"  Parent IDs that DON'T exist as taxon IDs: {len(orphaned_parents)}")
        
        if orphaned_parents:
            print(f"  Sample orphaned parent IDs: {list(orphaned_parents)[:5]}")
        
        return records, parent_child_pairs, matching_parents, orphaned_parents
        
    except Exception as e:
        print(f"❌ Error analyzing CSV: {e}")
        return None, None, None, None

def analyze_sample_records(records):
    """Analyze sample records to understand the data structure"""
    if not records:
        return
        
    print(f"\n🔬 SAMPLE RECORD ANALYSIS")
    print("=" * 50)
    
    # Group by taxonomic status
    status_counts = Counter(r.get('taxonomicStatus', 'Unknown') for r in records)
    print("Taxonomic status distribution:")
    for status, count in status_counts.most_common():
        print(f"  {status}: {count}")
    
    # Group by rank
    rank_counts = Counter(r.get('taxonRank', 'Unknown') for r in records)
    print("\nTaxon rank distribution:")
    for rank, count in rank_counts.most_common():
        print(f"  {rank}: {count}")
    
    # Show sample records
    print("\nSample records:")
    for i, record in enumerate(records[:3]):
        print(f"\nRecord {i+1}:")
        print(f"  taxonID: {record.get('taxonID')}")
        print(f"  scientificName: {record.get('scientificName')}")
        print(f"  taxonRank: {record.get('taxonRank')}")
        print(f"  taxonomicStatus: {record.get('taxonomicStatus')}")
        print(f"  parentNameUsageID: {record.get('parentNameUsageID')}")
        print(f"  family: {record.get('family', 'N/A')}")

def diagnose_parent_relationship_issues(parent_child_pairs, matching_parents, orphaned_parents):
    """Diagnose why parent relationships might be failing"""
    print(f"\n🔧 DIAGNOSING PARENT RELATIONSHIP ISSUES")
    print("=" * 50)
    
    if len(orphaned_parents) > len(matching_parents):
        print("🔴 MAJOR ISSUE: Most parent IDs don't exist as taxon IDs")
        print("   This suggests:")
        print("   1. Data spans multiple families/files and parents are in other files")
        print("   2. Parent taxa are filtered out during import")
        print("   3. Parent IDs use different format than taxon IDs")
        
    if len(matching_parents) == 0:
        print("🔴 CRITICAL: No parent IDs match taxon IDs in this file")
        print("   This means hierarchy cannot be built from single-family files")
        print("   You need to import ALL families first, then build relationships")
    else:
        print(f"✅ {len(matching_parents)} parent relationships can be resolved within file")
    
    # Analyze parent-child patterns
    if parent_child_pairs:
        print(f"\nSample parent-child relationships:")
        for i, (parent, child) in enumerate(parent_child_pairs[:5]):
            exists = "✅" if parent in matching_parents else "❌"
            print(f"  {parent} → {child} {exists}")

def check_import_process_assumptions():
    """Check assumptions made by the import process"""
    print(f"\n🧪 CHECKING IMPORT PROCESS ASSUMPTIONS")
    print("=" * 50)
    
    print("Your current import process assumes:")
    print("1. ✅ Each family file contains complete hierarchies")
    print("2. ❌ Parent relationships can be resolved within single files")
    print("3. ❌ WFO IDs are stored in wfo_id column (missing!)")
    print("4. ❌ Synonyms and invalid taxa are properly handled")
    
    print("\nLikely issues:")
    print("1. 🔴 Missing wfo_id column prevents proper unique identification")
    print("2. 🔴 Parent taxa may be in different family files")
    print("3. 🔴 Import processes families individually, breaking cross-references")
    print("4. 🔴 Only 'accepted' status imported (no synonyms)")

def suggest_fixes():
    """Suggest fixes for the import process"""
    print(f"\n💡 SUGGESTED FIXES")
    print("=" * 50)
    
    print("IMMEDIATE FIXES:")
    print("1. 🔧 Add wfo_id column (run fix_schema.sql)")
    print("2. 🔧 Import ALL families first, build relationships second")
    print("3. 🔧 Store original WFO IDs during initial import")
    
    print("\nIMPORT PROCESS CHANGES:")
    print("1. 📥 Phase 1: Import all taxa without parent relationships")
    print("2. 🔗 Phase 2: Build all parent relationships using WFO IDs")
    print("3. 🧹 Phase 3: Import synonyms and other statuses")
    
    print("\nTEST STRATEGY:")
    print("1. 🧪 Test with current 'flat' data structure")
    print("2. 🧪 Add tests for data repair process")
    print("3. 🧪 Test hierarchical integrity after repair")

def main():
    """Main diagnostic function"""
    print("🔍 WFO IMPORT DIAGNOSIS")
    print("="*60)
    
    # Analyze file structure
    sample_file = analyze_wfo_file_structure()
    
    if sample_file:
        # Analyze CSV content
        records, pairs, matching, orphaned = analyze_classification_csv(sample_file)
        
        if records:
            analyze_sample_records(records)
            diagnose_parent_relationship_issues(pairs, matching, orphaned)
    
    # Check import assumptions
    check_import_process_assumptions()
    
    # Suggest fixes
    suggest_fixes()
    
    print(f"\n✅ Diagnosis complete!")

if __name__ == "__main__":
    main()