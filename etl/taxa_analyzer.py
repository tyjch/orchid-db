import os
import psycopg
from dotenv import load_dotenv
import pandas as pd
from collections import Counter
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_db_config():
    """Load database configuration from .env file"""
    load_dotenv()
    
    config = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD')
    }
    
    # Check for required values
    required = ['database', 'user', 'password']
    missing = [key for key in required if not config[key]]
    
    if missing:
        logger.error("Missing required environment variables:")
        logger.error("Expected format in .env file:")
        logger.error("POSTGRES_DB=\"your-database-name\"")
        logger.error("POSTGRES_USER=\"your-username\"") 
        logger.error("POSTGRES_PASSWORD=\"your-password\"")
        logger.error("POSTGRES_HOST=\"localhost\"  # optional, defaults to localhost")
        logger.error("POSTGRES_PORT=5432  # optional, defaults to 5432")
        raise ValueError(f"Missing required environment variables: {missing}")
    
    return config

def get_db_connection():
    """Create database connection using psycopg3"""
    config = load_db_config()
    
    try:
        conn = psycopg.connect(
            host=config['host'],
            port=config['port'],
            dbname=config['database'],
            user=config['user'],
            password=config['password']
        )
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def analyze_basic_stats(conn):
    """Analyze basic statistics of the taxa table"""
    logger.info("=== BASIC STATISTICS ===")
    
    queries = [
        ("Total records", "SELECT COUNT(*) FROM taxonomy.taxa"),
        ("Records with names", "SELECT COUNT(*) FROM taxonomy.taxa WHERE name IS NOT NULL"),
        ("Records with WFO IDs", "SELECT COUNT(*) FROM taxonomy.taxa WHERE wfo_id IS NOT NULL"),
        ("Unique names", "SELECT COUNT(DISTINCT name) FROM taxonomy.taxa WHERE name IS NOT NULL"),
        ("Records with parents", "SELECT COUNT(*) FROM taxonomy.taxa WHERE parent_id IS NOT NULL"),
        ("Root records (no parent)", "SELECT COUNT(*) FROM taxonomy.taxa WHERE parent_id IS NULL"),
    ]
    
    with conn.cursor() as cur:
        for title, query in queries:
            cur.execute(query)
            result = cur.fetchone()[0]
            logger.info(f"{title}: {result:,}")

def analyze_ranks(conn):
    """Analyze distribution of taxonomic ranks"""
    logger.info("\n=== RANK DISTRIBUTION ===")
    
    query = """
    SELECT rank, COUNT(*) as count
    FROM taxonomy.taxa 
    WHERE rank IS NOT NULL
    GROUP BY rank 
    ORDER BY count DESC
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
        
        total = sum(row[1] for row in results)
        logger.info(f"Total records with rank: {total:,}")
        
        for rank, count in results:
            percentage = (count / total) * 100
            logger.info(f"  {rank}: {count:,} ({percentage:.1f}%)")

def analyze_status(conn):
    """Analyze distribution of taxonomic status"""
    logger.info("\n=== STATUS DISTRIBUTION ===")
    
    query = """
    SELECT status, COUNT(*) as count
    FROM taxonomy.taxa 
    WHERE status IS NOT NULL
    GROUP BY status 
    ORDER BY count DESC
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
        
        total = sum(row[1] for row in results)
        logger.info(f"Total records with status: {total:,}")
        
        for status, count in results:
            percentage = (count / total) * 100
            logger.info(f"  {status}: {count:,} ({percentage:.1f}%)")

def analyze_duplicates(conn):
    """Analyze duplicate names and their patterns"""
    logger.info("\n=== DUPLICATE NAME ANALYSIS ===")
    
    # Find duplicate names
    duplicate_query = """
    SELECT name, COUNT(*) as count
    FROM taxonomy.taxa 
    WHERE name IS NOT NULL
    GROUP BY name 
    HAVING COUNT(*) > 1
    ORDER BY count DESC
    LIMIT 20
    """
    
    with conn.cursor() as cur:
        cur.execute(duplicate_query)
        duplicates = cur.fetchall()
        
        if duplicates:
            logger.info(f"Found {len(duplicates)} names with duplicates (showing top 20):")
            for name, count in duplicates[:10]:
                logger.info(f"  '{name}': {count} records")
            
            # Analyze patterns in duplicates
            logger.info("\n--- Analyzing duplicate patterns ---")
            
            # Get details for most duplicated names
            top_duplicate = duplicates[0][0]
            detail_query = """
            SELECT id, name, rank, status, parent_id, wfo_id
            FROM taxonomy.taxa 
            WHERE name = %s
            ORDER BY id
            """
            
            cur.execute(detail_query, (top_duplicate,))
            details = cur.fetchall()
            
            logger.info(f"\nExample: '{top_duplicate}' has {len(details)} records:")
            for record in details:
                id_val, name, rank, status, parent_id, wfo_id = record
                logger.info(f"  ID: {id_val}, Rank: {rank}, Status: {status}, Parent: {parent_id}, WFO: {wfo_id}")
        else:
            logger.info("No duplicate names found")

def analyze_species_duplicates(conn):
    """Specifically analyze species-level duplicates"""
    logger.info("\n=== SPECIES-LEVEL DUPLICATE ANALYSIS ===")
    
    species_duplicate_query = """
    SELECT name, COUNT(*) as count,
           STRING_AGG(DISTINCT status::text, ', ') as statuses,
           STRING_AGG(DISTINCT wfo_id, ', ') as wfo_ids
    FROM taxonomy.taxa 
    WHERE rank = 'species' AND name IS NOT NULL
    GROUP BY name 
    HAVING COUNT(*) > 1
    ORDER BY count DESC
    LIMIT 15
    """
    
    with conn.cursor() as cur:
        cur.execute(species_duplicate_query)
        results = cur.fetchall()
        
        if results:
            logger.info(f"Found {len(results)} species names with duplicates:")
            for name, count, statuses, wfo_ids in results:
                logger.info(f"  '{name}': {count} records")
                logger.info(f"    Statuses: {statuses}")
                logger.info(f"    WFO IDs: {wfo_ids}")
                logger.info("")
        else:
            logger.info("No duplicate species names found")

def analyze_status_rank_combinations(conn):
    """Analyze combinations of status and rank"""
    logger.info("\n=== STATUS-RANK COMBINATIONS ===")
    
    combo_query = """
    SELECT rank, status, COUNT(*) as count
    FROM taxonomy.taxa 
    WHERE rank IS NOT NULL AND status IS NOT NULL
    GROUP BY rank, status
    ORDER BY rank, count DESC
    """
    
    with conn.cursor() as cur:
        cur.execute(combo_query)
        results = cur.fetchall()
        
        current_rank = None
        for rank, status, count in results:
            if rank != current_rank:
                logger.info(f"\n{rank.upper()}:")
                current_rank = rank
            logger.info(f"  {status}: {count:,}")

def analyze_hierarchical_structure(conn):
    """Analyze the hierarchical structure"""
    logger.info("\n=== HIERARCHICAL STRUCTURE ANALYSIS ===")
    
    # Count records at each level
    level_query = """
    WITH RECURSIVE taxa_hierarchy AS (
        -- Root nodes (no parent)
        SELECT id, name, rank, parent_id, 0 as level
        FROM taxonomy.taxa
        WHERE parent_id IS NULL
        
        UNION ALL
        
        -- Recursive: children
        SELECT t.id, t.name, t.rank, t.parent_id, th.level + 1
        FROM taxonomy.taxa t
        JOIN taxa_hierarchy th ON t.parent_id = th.id
        WHERE th.level < 10  -- Prevent infinite recursion
    )
    SELECT level, COUNT(*) as count
    FROM taxa_hierarchy
    GROUP BY level
    ORDER BY level
    """
    
    with conn.cursor() as cur:
        cur.execute(level_query)
        levels = cur.fetchall()
        
        logger.info("Records by hierarchy level:")
        for level, count in levels:
            logger.info(f"  Level {level}: {count:,} records")

def analyze_wfo_coverage(conn):
    """Analyze WFO ID coverage"""
    logger.info("\n=== WFO ID COVERAGE ANALYSIS ===")
    
    coverage_query = """
    SELECT 
        rank,
        COUNT(*) as total,
        COUNT(wfo_id) as with_wfo,
        ROUND(COUNT(wfo_id) * 100.0 / COUNT(*), 1) as coverage_percent
    FROM taxonomy.taxa
    WHERE rank IS NOT NULL
    GROUP BY rank
    ORDER BY coverage_percent DESC
    """
    
    with conn.cursor() as cur:
        cur.execute(coverage_query)
        results = cur.fetchall()
        
        logger.info("WFO ID coverage by rank:")
        for rank, total, with_wfo, coverage in results:
            logger.info(f"  {rank}: {with_wfo:,}/{total:,} ({coverage}%)")

def check_data_quality_issues(conn):
    """Check for various data quality issues"""
    logger.info("\n=== DATA QUALITY ISSUES ===")
    
    quality_queries = [
        ("Records with NULL name", "SELECT COUNT(*) FROM taxonomy.taxa WHERE name IS NULL"),
        ("Records with empty name", "SELECT COUNT(*) FROM taxonomy.taxa WHERE name = ''"),
        ("Records with NULL rank", "SELECT COUNT(*) FROM taxonomy.taxa WHERE rank IS NULL"),
        ("Records with NULL status", "SELECT COUNT(*) FROM taxonomy.taxa WHERE status IS NULL"),
        ("Records referencing non-existent parent", """
            SELECT COUNT(*) FROM taxonomy.taxa t1 
            WHERE t1.parent_id IS NOT NULL 
            AND NOT EXISTS (SELECT 1 FROM taxonomy.taxa t2 WHERE t2.id = t1.parent_id)
        """),
        ("Self-referencing records", "SELECT COUNT(*) FROM taxonomy.taxa WHERE id = parent_id"),
    ]
    
    with conn.cursor() as cur:
        for title, query in quality_queries:
            cur.execute(query)
            result = cur.fetchone()[0]
            if result > 0:
                logger.warning(f"⚠️  {title}: {result:,}")
            else:
                logger.info(f"✅ {title}: {result}")

def sample_records(conn):
    """Show sample records from different categories"""
    logger.info("\n=== SAMPLE RECORDS ===")
    
    sample_queries = [
        ("Sample accepted species", """
            SELECT id, name, rank, status, wfo_id 
            FROM taxonomy.taxa 
            WHERE rank = 'species' AND status = 'accepted'
            LIMIT 5
        """),
        ("Sample synonym species", """
            SELECT id, name, rank, status, wfo_id 
            FROM taxonomy.taxa 
            WHERE rank = 'species' AND status = 'synonym'
            LIMIT 5
        """),
        ("Sample unresolved species", """
            SELECT id, name, rank, status, wfo_id 
            FROM taxonomy.taxa 
            WHERE rank = 'species' AND status = 'unresolved'
            LIMIT 5
        """),
    ]
    
    with conn.cursor() as cur:
        for title, query in sample_queries:
            logger.info(f"\n{title}:")
            cur.execute(query)
            results = cur.fetchall()
            
            if results:
                for record in results:
                    id_val, name, rank, status, wfo_id = record
                    logger.info(f"  ID: {id_val}, Name: '{name}', Status: {status}, WFO: {wfo_id}")
            else:
                logger.info("  No records found")

def main():
    """Main analysis function"""
    logger.info("Starting taxonomy.taxa table analysis...")
    
    try:
        conn = get_db_connection()
        
        with conn:
            analyze_basic_stats(conn)
            analyze_ranks(conn)
            analyze_status(conn)
            analyze_duplicates(conn)
            analyze_species_duplicates(conn)
            analyze_status_rank_combinations(conn)
            analyze_hierarchical_structure(conn)
            analyze_wfo_coverage(conn)
            check_data_quality_issues(conn)
            sample_records(conn)
        
        logger.info("\n" + "="*60)
        logger.info("Analysis complete!")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()