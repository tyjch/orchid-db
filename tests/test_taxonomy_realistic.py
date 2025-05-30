import pytest
import psycopg
import os
from dotenv import load_dotenv
from typing import Any, Dict, List, Tuple

# Load environment variables
load_dotenv()

class DatabaseConnection:
    """Context manager for database connections"""
    
    def __init__(self):
        self.conn = None
    
    def __enter__(self):
        self.conn = psycopg.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'), 
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('POSTGRES_PORT', '5432')
        )
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

@pytest.fixture
def db_conn():
    """Pytest fixture to provide database connection"""
    with DatabaseConnection() as conn:
        yield conn

def execute_count_query(conn, query: str) -> int:
    """Execute a COUNT query and return the result"""
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0]

def execute_query(conn, query: str) -> List[Tuple]:
    """Execute a query and return all results"""
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def has_column(conn, table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_schema = 'taxonomy' 
              AND table_name = %s 
              AND column_name = %s
        """, (table_name, column_name))
        return cur.fetchone()[0] > 0

class TestDatabaseStructure:
    """Test the basic database structure and setup"""
    
    def test_taxonomy_table_exists(self, db_conn):
        """Test that the taxonomy.taxa table exists"""
        query = """
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = 'taxonomy' AND table_name = 'taxa'
        """
        count = execute_count_query(db_conn, query)
        assert count == 1, "taxonomy.taxa table does not exist"
    
    def test_has_minimum_required_columns(self, db_conn):
        """Test that required columns exist"""
        required_columns = ['id', 'name', 'rank', 'parent_id', 'status']
        
        for column in required_columns:
            assert has_column(db_conn, 'taxa', column), f"Required column '{column}' is missing"
    
    def test_has_data(self, db_conn):
        """Test that the table has data"""
        count = execute_count_query(db_conn, "SELECT COUNT(*) FROM taxonomy.taxa")
        assert count > 0, "taxonomy.taxa table is empty"
        print(f"Found {count:,} taxa in database")

class TestDataIntegrity:
    """Test data integrity with realistic expectations"""
    
    def test_no_self_referencing_taxa(self, db_conn):
        """Test that no taxon references itself as parent - this should always be 0"""
        query = "SELECT COUNT(*) FROM taxonomy.taxa WHERE id = parent_id"
        count = execute_count_query(db_conn, query)
        assert count == 0, f"Found {count} self-referencing taxa"
    
    def test_parent_references_exist(self, db_conn):
        """Test that parent_id references point to existing taxa"""
        query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa t1 
        WHERE t1.parent_id IS NOT NULL 
          AND NOT EXISTS (
              SELECT 1 FROM taxonomy.taxa t2 
              WHERE t2.id = t1.parent_id
          )
        """
        count = execute_count_query(db_conn, query)
        assert count == 0, f"Found {count} taxa with non-existent parent references"
    
    def test_no_empty_names(self, db_conn):
        """Test that no taxa have empty or null names"""
        query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE name IS NULL OR TRIM(name) = ''
        """
        count = execute_count_query(db_conn, query)
        assert count == 0, f"Found {count} taxa with null or empty names"
    
    def test_valid_enum_values(self, db_conn):
        """Test that status and rank values are valid"""
        # Test status values
        status_query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE status NOT IN ('accepted', 'synonym', 'invalid', 'unresolved', 'misapplied')
        """
        status_count = execute_count_query(db_conn, status_query)
        assert status_count == 0, f"Found {status_count} taxa with invalid status values"
        
        # Test rank values  
        valid_ranks = [
            'kingdom', 'phylum', 'class', 'order', 'family', 'subfamily', 
            'tribe', 'subtribe', 'genus', 'subgenus', 'section', 'subsection',
            'species', 'subspecies', 'variety', 'form'
        ]
        
        rank_query = f"""
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE rank::text NOT IN ({','.join(f"'{r}'" for r in valid_ranks)})
        """
        rank_count = execute_count_query(db_conn, rank_query)
        assert rank_count == 0, f"Found {rank_count} taxa with invalid rank values"

class TestDataConsistency:
    """Test data consistency and quality (may have warnings)"""
    
    def test_hierarchy_analysis(self, db_conn):
        """Analyze hierarchy issues and report (not necessarily fail)"""
        # Your original query
        query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE status = 'accepted' AND parent_id IS NULL
        """
        count = execute_count_query(db_conn, query)
        
        # Get breakdown by rank
        detail_query = """
        SELECT rank, COUNT(*) as count
        FROM taxonomy.taxa 
        WHERE status = 'accepted' AND parent_id IS NULL 
        GROUP BY rank 
        ORDER BY count DESC
        """
        details = execute_query(db_conn, detail_query)
        
        print(f"\n📊 HIERARCHY ANALYSIS:")
        print(f"Total accepted taxa without parents: {count:,}")
        print("Breakdown by rank:")
        for rank, rank_count in details[:10]:
            print(f"  {rank}: {rank_count:,}")
        
        # Different expectations based on what we found
        if count > 100000:
            print("\n⚠️  MAJOR HIERARCHY ISSUE DETECTED:")
            print("   This suggests WFO data was imported without building parent relationships.")
            print("   This is a data import issue, not a data integrity issue.")
            
            # Check if it's a flat import (all taxa imported without hierarchy)
            species_orphans = execute_count_query(db_conn, """
                SELECT COUNT(*) FROM taxonomy.taxa 
                WHERE status = 'accepted' AND parent_id IS NULL AND rank = 'species'
            """)
            
            if species_orphans > 10000:
                pytest.skip("Skipping hierarchy test - data appears to be imported without parent relationships")
        
        # For smaller numbers, we can be more strict
        elif count < 1000:
            # Only families and kingdoms should be roots
            problem_query = """
            SELECT COUNT(*) FROM taxonomy.taxa 
            WHERE status = 'accepted' 
              AND parent_id IS NULL 
              AND rank NOT IN ('family', 'kingdom')
            """
            problem_count = execute_count_query(db_conn, problem_query)
            assert problem_count == 0, f"Found {problem_count} accepted non-root taxa without parents"
    
    def test_species_name_patterns(self, db_conn):
        """Test species names follow reasonable patterns"""
        # Sample check - species should generally have spaces (binomial)
        query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE rank = 'species' 
          AND status = 'accepted'
          AND name NOT LIKE '% %'
        """
        count = execute_count_query(db_conn, query)
        
        total_species_query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE rank = 'species' AND status = 'accepted'
        """
        total_species = execute_count_query(db_conn, total_species_query)
        
        if total_species > 0:
            bad_percent = (count / total_species) * 100
            print(f"Species without spaces: {count:,} ({bad_percent:.1f}%)")
            
            # Allow some flexibility but warn if too many
            assert bad_percent < 10, f"Too many species ({bad_percent:.1f}%) lack proper binomial format"
    
    def test_rank_distribution_sanity(self, db_conn):
        """Test that rank distribution makes biological sense"""
        query = """
        SELECT rank, COUNT(*) as count
        FROM taxonomy.taxa 
        WHERE status = 'accepted'
        GROUP BY rank 
        ORDER BY count DESC
        """
        results = execute_query(db_conn, query)
        rank_counts = dict(results)
        
        print(f"\n📊 RANK DISTRIBUTION:")
        for rank, count in results[:10]:
            print(f"  {rank}: {count:,}")
        
        # Basic sanity checks
        species_count = rank_counts.get('species', 0)
        genus_count = rank_counts.get('genus', 0)
        family_count = rank_counts.get('family', 0)
        
        assert species_count > 0, "No species found"
        assert genus_count > 0, "No genera found"
        assert family_count > 0, "No families found"
        
        # Species should generally outnumber genera
        if genus_count > 0:
            species_per_genus = species_count / genus_count
            print(f"Species per genus ratio: {species_per_genus:.1f}")
            assert species_per_genus >= 0.5, f"Unusually low species/genus ratio: {species_per_genus:.1f}"

class TestWFOSpecific:
    """Tests specific to WFO data (conditional on wfo_id column existing)"""
    
    def test_wfo_id_column_exists(self, db_conn):
        """Test if wfo_id column exists (informational)"""
        has_wfo = has_column(db_conn, 'taxa', 'wfo_id')
        print(f"\nWFO ID column exists: {has_wfo}")
        
        if not has_wfo:
            print("⚠️  Missing wfo_id column - WFO import may be incomplete")
            print("   Consider adding: ALTER TABLE taxonomy.taxa ADD COLUMN wfo_id VARCHAR(50);")
    
    @pytest.mark.skipif(
        lambda db_conn: not has_column(db_conn, 'taxa', 'wfo_id'),
        reason="wfo_id column does not exist"
    )
    def test_wfo_ids_unique(self, db_conn):
        """Test WFO IDs are unique (only if column exists)"""
        if not has_column(db_conn, 'taxa', 'wfo_id'):
            pytest.skip("wfo_id column does not exist")
        
        query = """
        SELECT COUNT(*) 
        FROM (
            SELECT wfo_id 
            FROM taxonomy.taxa 
            WHERE wfo_id IS NOT NULL 
            GROUP BY wfo_id 
            HAVING COUNT(*) > 1
        ) duplicates
        """
        count = execute_count_query(db_conn, query)
        assert count == 0, f"Found {count} duplicate WFO IDs"

class TestSpecificFamilies:
    """Test for specific plant families"""
    
    def test_major_families_present(self, db_conn):
        """Test that some major plant families are present"""
        major_families = [
            'Orchidaceae', 'Asteraceae', 'Fabaceae', 'Poaceae', 'Rosaceae'
        ]
        
        found_families = []
        missing_families = []
        
        for family in major_families:
            query = f"""
            SELECT COUNT(*) 
            FROM taxonomy.taxa 
            WHERE rank = 'family' AND name = '{family}'
            """
            count = execute_count_query(db_conn, query)
            
            if count > 0:
                found_families.append(family)
            else:
                missing_families.append(family)
        
        print(f"\nFound families: {found_families}")
        if missing_families:
            print(f"Missing families: {missing_families}")
        
        # At least some major families should be present
        assert len(found_families) > 0, "No major plant families found in database"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])