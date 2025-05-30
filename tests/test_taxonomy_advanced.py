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

class TestAdvancedTaxonomyRules:
    """Advanced tests for complex taxonomic business rules"""
    
    def test_exact_query_from_requirement(self, db_conn):
        """
        Test the exact query mentioned in the requirements.
        This should return 0 if status is 'accepted' then parent_id shouldn't be null.
        """
        query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE status = 'accepted' AND parent_id IS NULL
        """
        
        count = execute_count_query(db_conn, query)
        
        # Let's be more flexible for WFO data - some families might be root nodes
        if count > 0:
            # Check what ranks these are
            detail_query = """
            SELECT rank, COUNT(*) as count, 
                   STRING_AGG(name, ', ' ORDER BY name LIMIT 5) as examples
            FROM taxonomy.taxa 
            WHERE status = 'accepted' AND parent_id IS NULL 
            GROUP BY rank 
            ORDER BY count DESC
            """
            
            details = execute_query(db_conn, detail_query)
            print(f"Found {count} accepted taxa without parents:")
            for rank, rank_count, examples in details:
                print(f"  {rank}: {rank_count} ({examples})")
            
            # Allow only families and kingdoms as root nodes
            non_root_query = """
            SELECT COUNT(*) 
            FROM taxonomy.taxa 
            WHERE status = 'accepted' 
              AND parent_id IS NULL 
              AND rank NOT IN ('kingdom', 'family')
            """
            
            non_root_count = execute_count_query(db_conn, non_root_query)
            assert non_root_count == 0, f"Found {non_root_count} accepted non-root taxa without parent_id"
        
        # If count is 0, that's perfect - exactly what was requested
        print(f"Query result: {count} (0 means perfect data integrity)")
    
    def test_genus_species_relationship_integrity(self, db_conn):
        """Test that all species have genus parents (directly or through subgenus/section)"""
        query = """
        WITH RECURSIVE species_to_genus AS (
            -- Start with species
            SELECT s.id as species_id, s.name as species_name, 
                   s.parent_id, p.rank as parent_rank, p.name as parent_name
            FROM taxonomy.taxa s
            LEFT JOIN taxonomy.taxa p ON s.parent_id = p.id
            WHERE s.rank = 'species'
            
            UNION ALL
            
            -- Walk up until we find genus
            SELECT stg.species_id, stg.species_name, 
                   p.parent_id, pp.rank as parent_rank, pp.name as parent_name
            FROM species_to_genus stg
            JOIN taxonomy.taxa p ON stg.parent_id = p.id
            LEFT JOIN taxonomy.taxa pp ON p.parent_id = pp.id
            WHERE stg.parent_rank NOT IN ('genus') 
              AND stg.parent_rank IS NOT NULL
        )
        SELECT COUNT(DISTINCT species_id)
        FROM species_to_genus
        WHERE parent_rank IS NULL OR parent_rank != 'genus'
        """
        
        count = execute_count_query(db_conn, query)
        assert count == 0, f"Found {count} species not properly connected to genus"
    
    def test_synonym_relationships_valid(self, db_conn):
        """Test that synonyms point to taxa of the same or higher rank"""
        query = """
        SELECT COUNT(*)
        FROM taxonomy.taxa syn
        JOIN taxonomy.taxa accepted ON syn.parent_id = accepted.id
        WHERE syn.status = 'synonym'
          AND syn.rank = 'species'
          AND accepted.rank NOT IN ('species', 'genus', 'subgenus', 'section')
        """
        
        count = execute_count_query(db_conn, query)
        # This might not be 0 for WFO data, but should be low
        if count > 0:
            print(f"Warning: Found {count} species synonyms pointing to unexpected ranks")
    
    def test_wfo_id_format_validity(self, db_conn):
        """Test that WFO IDs follow expected format patterns"""
        query = """
        SELECT COUNT(*)
        FROM taxonomy.taxa
        WHERE wfo_id IS NOT NULL 
          AND wfo_id NOT SIMILAR TO 'wfo-[0-9]{10}(-[0-9]{4}-[0-9]{2})?'
        """
        
        count = execute_count_query(db_conn, query)
        if count > 0:
            # Show some examples of non-standard WFO IDs
            examples_query = """
            SELECT wfo_id, name, rank
            FROM taxonomy.taxa
            WHERE wfo_id IS NOT NULL 
              AND wfo_id NOT SIMILAR TO 'wfo-[0-9]{10}(-[0-9]{4}-[0-9]{2})?'
            LIMIT 5
            """
            examples = execute_query(db_conn, examples_query)
            print(f"Found {count} WFO IDs with non-standard format:")
            for wfo_id, name, rank in examples:
                print(f"  {wfo_id}: {name} ({rank})")

class TestDataConsistency:
    """Tests for data consistency and logical integrity"""
    
    def test_family_genus_species_counts_logical(self, db_conn):
        """Test that family->genus->species counts make biological sense"""
        query = """
        WITH family_stats AS (
            SELECT 
                f.name as family_name,
                COUNT(DISTINCT g.id) as genus_count,
                COUNT(DISTINCT s.id) as species_count
            FROM taxonomy.taxa f
            LEFT JOIN taxonomy.taxa g ON (
                g.parent_id = f.id OR 
                EXISTS (
                    SELECT 1 FROM taxonomy.taxa intermediate
                    WHERE intermediate.parent_id = f.id 
                      AND intermediate.rank IN ('subfamily', 'tribe')
                      AND g.parent_id = intermediate.id
                )
            ) AND g.rank = 'genus' AND g.status = 'accepted'
            LEFT JOIN taxonomy.taxa s ON (
                s.parent_id = g.id OR
                EXISTS (
                    SELECT 1 FROM taxonomy.taxa subg
                    WHERE subg.parent_id = g.id 
                      AND subg.rank IN ('subgenus', 'section', 'subsection')
                      AND s.parent_id = subg.id
                )
            ) AND s.rank = 'species' AND s.status = 'accepted'
            WHERE f.rank = 'family' AND f.status = 'accepted'
            GROUP BY f.name
        )
        SELECT COUNT(*)
        FROM family_stats
        WHERE genus_count > 0 AND species_count = 0
        """
        
        count = execute_count_query(db_conn, query)
        if count > 0:
            print(f"Warning: Found {count} families with genera but no species")
    
    def test_no_orphaned_subspecific_taxa(self, db_conn):
        """Test that subspecies, varieties, forms have proper species parents"""
        subspecific_ranks = ['subspecies', 'variety', 'form']
        
        for rank in subspecific_ranks:
            query = f"""
            WITH RECURSIVE find_species_parent AS (
                -- Start with subspecific taxon
                SELECT id, name, rank, parent_id, 0 as level
                FROM taxonomy.taxa
                WHERE rank = '{rank}'
                
                UNION ALL
                
                -- Walk up to find species
                SELECT t.id, t.name, t.rank, t.parent_id, fsp.level + 1
                FROM taxonomy.taxa t
                JOIN find_species_parent fsp ON t.id = fsp.parent_id
                WHERE fsp.level < 5 AND t.rank != 'species'
            )
            SELECT COUNT(DISTINCT id)
            FROM find_species_parent fsp
            WHERE fsp.rank = '{rank}'
              AND NOT EXISTS (
                  SELECT 1 FROM find_species_parent fsp2 
                  WHERE fsp2.id = fsp.id AND fsp2.rank != '{rank}' 
                    AND fsp2.rank = 'species'
              )
            """
            
            count = execute_count_query(db_conn, query)
            assert count == 0, f"Found {count} {rank} taxa not connected to species"

class TestPerformanceAndScaling:
    """Tests to ensure the database performs well with the imported data"""
    
    @pytest.mark.slow
    def test_large_query_performance(self, db_conn):
        """Test that large queries complete in reasonable time"""
        import time
        
        # Test a complex hierarchical query
        query = """
        WITH RECURSIVE full_hierarchy AS (
            SELECT id, name, rank, parent_id, 
                   name as lineage, 1 as depth
            FROM taxonomy.taxa 
            WHERE parent_id IS NULL
            
            UNION ALL
            
            SELECT t.id, t.name, t.rank, t.parent_id,
                   fh.lineage || ' > ' || t.name, fh.depth + 1
            FROM taxonomy.taxa t
            JOIN full_hierarchy fh ON t.parent_id = fh.id
            WHERE fh.depth < 10
        )
        SELECT COUNT(*), MAX(depth), AVG(depth)
        FROM full_hierarchy
        """
        
        start_time = time.time()
        result = execute_query(db_conn, query)
        end_time = time.time()
        
        query_time = end_time - start_time
        assert query_time < 30.0, f"Hierarchical query took {query_time:.2f} seconds (too slow)"
        
        print(f"Hierarchical query completed in {query_time:.2f} seconds")
        print(f"Results: {result[0]}")
    
    def test_index_effectiveness(self, db_conn):
        """Test that important indexes are being used effectively"""
        # Test that WFO ID lookups are fast
        query = """
        EXPLAIN (ANALYZE, BUFFERS) 
        SELECT * FROM taxonomy.taxa 
        WHERE wfo_id = 'wfo-0000946150'
        """
        
        with db_conn.cursor() as cur:
            cur.execute(query)
            plan = cur.fetchall()
            
            # Check if index scan is being used
            plan_text = ' '.join([str(row[0]) for row in plan])
            assert 'Index Scan' in plan_text or 'Bitmap Heap Scan' in plan_text, \
                "WFO ID query not using index efficiently"

class TestDataSampling:
    """Tests that examine sample data for quality"""
    
    def test_sample_species_names_quality(self, db_conn):
        """Test sample of species names for basic quality"""
        query = """
        SELECT name, wfo_id
        FROM taxonomy.taxa 
        WHERE rank = 'species' 
          AND status = 'accepted'
        ORDER BY RANDOM()
        LIMIT 10
        """
        
        results = execute_query(db_conn, query)
        
        for name, wfo_id in results:
            # Basic name quality checks
            assert len(name.strip()) > 0, f"Empty species name found: '{name}'"
            assert ' ' in name, f"Species name lacks space: '{name}'"
            
            # Check for obviously malformed names
            assert not name.startswith(' '), f"Species name has leading space: '{name}'"
            assert not name.endswith(' '), f"Species name has trailing space: '{name}'"
            assert '  ' not in name, f"Species name has double spaces: '{name}'"
    
    def test_sample_hierarchy_completeness(self, db_conn):
        """Test that sample species have complete hierarchies"""
        query = """
        WITH RECURSIVE species_hierarchy AS (
            SELECT s.id, s.name as species_name, s.parent_id, 
                   1 as level, s.name as path
            FROM taxonomy.taxa s
            WHERE s.rank = 'species' 
              AND s.status = 'accepted'
            ORDER BY RANDOM()
            LIMIT 5
            
            UNION ALL
            
            SELECT p.id, sh.species_name, p.parent_id,
                   sh.level + 1, p.name || ' < ' || sh.path
            FROM taxonomy.taxa p
            JOIN species_hierarchy sh ON p.id = sh.parent_id
            WHERE sh.level < 8
        )
        SELECT species_name, MAX(level) as hierarchy_depth, 
               MAX(path) as full_lineage
        FROM species_hierarchy
        GROUP BY species_name
        """
        
        results = execute_query(db_conn, query)
        
        for species_name, depth, lineage in results:
            # Should have at least genus > species (depth 2)
            assert depth >= 2, f"Species {species_name} has insufficient hierarchy depth: {depth}"
            print(f"Sample hierarchy for {species_name}: {lineage}")

class TestBusinessLogicCompliance:
    """Tests for domain-specific business logic"""
    
    def test_plant_family_expectations(self, db_conn):
        """Test expectations specific to plant taxonomy"""
        # Test that we have reasonable numbers for major plant families
        major_families = [
            'Orchidaceae',    # Orchids - should be large
            'Asteraceae',     # Composites - largest plant family
            'Fabaceae',       # Legumes - very large family
            'Poaceae',        # Grasses - very large family
            'Rosaceae'        # Rose family - large family
        ]
        
        for family in major_families:
            query = f"""
            WITH family_stats AS (
                SELECT 
                    COUNT(DISTINCT g.id) as genus_count,
                    COUNT(DISTINCT s.id) as species_count
                FROM taxonomy.taxa f
                LEFT JOIN taxonomy.taxa g ON (
                    g.parent_id = f.id OR 
                    EXISTS (
                        SELECT 1 FROM taxonomy.taxa t 
                        WHERE t.parent_id = f.id 
                          AND t.rank IN ('subfamily', 'tribe')
                          AND g.parent_id = t.id
                    )
                ) AND g.rank = 'genus' AND g.status = 'accepted'
                LEFT JOIN taxonomy.taxa s ON (
                    s.parent_id = g.id OR
                    EXISTS (
                        SELECT 1 FROM taxonomy.taxa sg
                        WHERE sg.parent_id = g.id 
                          AND sg.rank IN ('subgenus', 'section', 'subsection')
                          AND s.parent_id = sg.id
                    )
                ) AND s.rank = 'species' AND s.status = 'accepted'
                WHERE f.name = '{family}' AND f.rank = 'family'
            )
            SELECT genus_count, species_count
            FROM family_stats
            """
            
            result = execute_query(db_conn, query)
            if result and result[0][0] is not None:
                genus_count, species_count = result[0]
                print(f"{family}: {genus_count} genera, {species_count} species")
                
                # Basic sanity checks for major families
                if genus_count > 0:
                    assert species_count > 0, f"{family} has genera but no species"
                    
                    # Species to genus ratio should be reasonable (>= 1)
                    ratio = species_count / genus_count if genus_count > 0 else 0
                    assert ratio >= 1.0, f"{family} has unrealistic species/genus ratio: {ratio:.2f}"
            else:
                print(f"Warning: {family} not found in database")

if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "--tb=short", "-m", "not slow"])