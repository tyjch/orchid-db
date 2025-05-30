#!/usr/bin/env python3
"""
Database Structure Inspector
============================

This script inspects your current taxonomy database to understand:
1. What columns exist in taxonomy.taxa
2. Current data distribution
3. Hierarchy issues
4. Recommendations for fixes
"""

import psycopg
import os
from dotenv import load_dotenv
from collections import Counter

load_dotenv()

def get_db_connection():
    """Get database connection"""
    return psycopg.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'), 
        password=os.getenv('POSTGRES_PASSWORD'),
        port=os.getenv('POSTGRES_PORT', '5432')
    )

def inspect_table_structure(conn):
    """Inspect the structure of taxonomy.taxa table"""
    print("🔍 INSPECTING TABLE STRUCTURE")
    print("=" * 50)
    
    with conn.cursor() as cur:
        # Get column information
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_schema = 'taxonomy' AND table_name = 'taxa'
            ORDER BY ordinal_position
        """)
        
        columns = cur.fetchall()
        print("Columns in taxonomy.taxa:")
        for col_name, data_type, nullable, default in columns:
            nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
            default_str = f" DEFAULT {default}" if default else ""
            print(f"  {col_name}: {data_type} {nullable_str}{default_str}")
        
        return [col[0] for col in columns]

def analyze_data_distribution(conn):
    """Analyze the distribution of data in the table"""
    print("\n📊 DATA DISTRIBUTION ANALYSIS")
    print("=" * 50)
    
    with conn.cursor() as cur:
        # Total count
        cur.execute("SELECT COUNT(*) FROM taxonomy.taxa")
        total = cur.fetchone()[0]
        print(f"Total taxa: {total:,}")
        
        # Status distribution
        print("\nStatus distribution:")
        cur.execute("""
            SELECT status, COUNT(*) as count
            FROM taxonomy.taxa 
            GROUP BY status 
            ORDER BY count DESC
        """)
        for status, count in cur.fetchall():
            pct = (count / total) * 100
            print(f"  {status}: {count:,} ({pct:.1f}%)")
        
        # Rank distribution
        print("\nRank distribution:")
        cur.execute("""
            SELECT rank, COUNT(*) as count
            FROM taxonomy.taxa 
            GROUP BY rank 
            ORDER BY count DESC
        """)
        for rank, count in cur.fetchall():
            pct = (count / total) * 100
            print(f"  {rank}: {count:,} ({pct:.1f}%)")

def analyze_hierarchy_issues(conn):
    """Analyze hierarchy and parent-child relationship issues"""
    print("\n🌳 HIERARCHY ANALYSIS")
    print("=" * 50)
    
    with conn.cursor() as cur:
        # Taxa without parents
        cur.execute("""
            SELECT rank, COUNT(*) as count
            FROM taxonomy.taxa 
            WHERE parent_id IS NULL 
            GROUP BY rank 
            ORDER BY count DESC
        """)
        
        orphans = cur.fetchall()
        total_orphans = sum(count for _, count in orphans)
        
        print(f"Taxa without parents: {total_orphans:,}")
        for rank, count in orphans[:10]:  # Show top 10
            print(f"  {rank}: {count:,}")
        
        # Accepted taxa without parents (your specific concern)
        cur.execute("""
            SELECT COUNT(*) 
            FROM taxonomy.taxa 
            WHERE status = 'accepted' AND parent_id IS NULL
        """)
        accepted_orphans = cur.fetchone()[0]
        print(f"\nAccepted taxa without parents: {accepted_orphans:,}")
        
        # Check for self-references
        cur.execute("SELECT COUNT(*) FROM taxonomy.taxa WHERE id = parent_id")
        self_refs = cur.fetchone()[0]
        print(f"Self-referencing taxa: {self_refs}")
        
        # Check for broken references
        cur.execute("""
            SELECT COUNT(*) 
            FROM taxonomy.taxa t1 
            WHERE t1.parent_id IS NOT NULL 
              AND NOT EXISTS (
                  SELECT 1 FROM taxonomy.taxa t2 
                  WHERE t2.id = t1.parent_id
              )
        """)
        broken_refs = cur.fetchone()[0]
        print(f"Broken parent references: {broken_refs}")

def sample_data_issues(conn):
    """Show sample problematic data"""
    print("\n🔬 SAMPLE PROBLEMATIC DATA")
    print("=" * 50)
    
    with conn.cursor() as cur:
        # Sample accepted species without parents
        print("Sample accepted species without parents:")
        cur.execute("""
            SELECT id, name, rank, status
            FROM taxonomy.taxa 
            WHERE status = 'accepted' 
              AND parent_id IS NULL 
              AND rank = 'species'
            LIMIT 5
        """)
        
        for row in cur.fetchall():
            print(f"  ID {row[0]}: {row[1]} ({row[2]}, {row[3]})")
        
        # Sample genera without parents (simpler query)
        print("\nSample genera without parents:")
        cur.execute("""
            SELECT name
            FROM taxonomy.taxa 
            WHERE rank = 'genus' AND parent_id IS NULL
            LIMIT 5
        """)
        
        genera = cur.fetchall()
        for (genus_name,) in genera:
            print(f"  {genus_name}")

def check_for_wfo_data(conn):
    """Check if this looks like WFO imported data"""
    print("\n🌍 WFO DATA CHECK")
    print("=" * 50)
    
    with conn.cursor() as cur:
        # Check for WFO-style IDs in name or other columns
        cur.execute("""
            SELECT COUNT(*) 
            FROM taxonomy.taxa 
            WHERE name LIKE '%wfo-%'
        """)
        wfo_in_names = cur.fetchone()[0]
        print(f"Names containing 'wfo-': {wfo_in_names}")
        
        # Check if there's a pattern suggesting WFO import without proper hierarchy
        cur.execute("""
            SELECT COUNT(*) 
            FROM taxonomy.taxa 
            WHERE status = 'accepted' 
              AND rank IN ('species', 'genus', 'family')
              AND parent_id IS NULL
        """)
        flat_structure = cur.fetchone()[0]
        print(f"Flat structure indicators: {flat_structure:,}")
        
        if flat_structure > 10000:
            print("⚠️  This looks like WFO data imported without building hierarchy!")
            print("   The import process may have skipped parent-child relationships.")

def generate_recommendations(conn):
    """Generate recommendations based on analysis"""
    print("\n💡 RECOMMENDATIONS")
    print("=" * 50)
    
    with conn.cursor() as cur:
        # Check total accepted orphans
        cur.execute("""
            SELECT COUNT(*) 
            FROM taxonomy.taxa 
            WHERE status = 'accepted' AND parent_id IS NULL
        """)
        orphans = cur.fetchone()[0]
        
        if orphans > 1000:
            print("🔴 CRITICAL: Massive hierarchy issues detected!")
            print("\nImmediate actions needed:")
            print("1. Check WFO import process - parent relationships weren't built properly")
            print("2. Review etl/dwc.py - the parent_id assignment logic may have issues")
            print("3. Consider re-running import with parent relationship debugging")
            print("4. Add wfo_id column if missing for proper unique identification")
        
        # Check for missing wfo_id column
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_schema = 'taxonomy' 
              AND table_name = 'taxa' 
              AND column_name = 'wfo_id'
        """)
        has_wfo_id = cur.fetchone()[0] > 0
        
        if not has_wfo_id:
            print("\n🟡 SCHEMA ISSUE: Missing wfo_id column")
            print("   Run: ALTER TABLE taxonomy.taxa ADD COLUMN wfo_id VARCHAR(50);")
            print("        CREATE UNIQUE INDEX unique_wfo_id ON taxonomy.taxa (wfo_id);")
        
        print("\n📋 Test Modifications Needed:")
        print("1. Update tests to handle current data structure")
        print("2. Create 'data repair' tests vs 'perfect data' tests")
        print("3. Focus on data consistency rather than perfect hierarchy")

def main():
    """Main inspection function"""
    print("🔍 TAXONOMY DATABASE INSPECTION")
    print("="*60)
    
    try:
        with get_db_connection() as conn:
            columns = inspect_table_structure(conn)
            analyze_data_distribution(conn)
            analyze_hierarchy_issues(conn)
            sample_data_issues(conn)
            check_for_wfo_data(conn)
            generate_recommendations(conn)
            
        print(f"\n✅ Inspection complete!")
        
    except Exception as e:
        print(f"❌ Error during inspection: {e}")
        raise

if __name__ == "__main__":
    main()