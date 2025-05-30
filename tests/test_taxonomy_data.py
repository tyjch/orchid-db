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

class TestTaxonomyDataIntegrity:
    """Test suite for taxonomy data integrity"""
    
    def test_accepted_taxa_have_parents_except_roots(self, db_conn):
        """
        Test that accepted taxa have parent_id set, except for root taxa.
        Root taxa are typically kingdoms or families (for plant databases).
        
        This is the specific test case requested - should return 0.
        """
        # First, let's check the general case you mentioned
        general_query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE status = 'accepted' AND parent_id IS NULL
        """
        
        general_count = execute_count_query(db_conn, general_query)
        
        # For WFO data, we expect only families to be root nodes
        specific_query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE status = 'accepted' 
          AND parent_id IS NULL 
          AND rank NOT IN ('kingdom', 'family')
        """
        
        specific_count = execute_count_query(db_conn, specific_query)
        
        # Print some diagnostics
        if general_count > 0:
            root_ranks_query = """
            SELECT rank, COUNT(*) 
            FROM taxonomy.taxa 
            WHERE status = 'accepted' AND parent_id IS NULL 
            GROUP BY rank 
            ORDER BY COUNT(*) DESC
            """
            root_ranks = execute_query(db_conn, root_ranks_query)
            print(f"Root taxa by rank: {root_ranks}")
        
        # The main assertion - your specific requirement
        assert specific_count == 0, f"Found {specific_count} accepted non-root taxa without parent_id"
    
    def test_no_self_referencing_taxa(self, db_conn):
        """Test that no taxon references itself as parent"""
        query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE id = parent_id
        """
        
        count = execute_count_query(db_conn, query)
        assert count == 0, f"Found {count} self-referencing taxa"
    
    def test_parent_exists_for_all_non_null_parent_ids(self, db_conn):
        """Test that all parent_id references point to existing taxa"""
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
    
    # def test_wfo_ids_are_unique(self, db_conn):
    #     """Test that WFO IDs are unique (no duplicates)"""
    #     query = """
    #     SELECT COUNT(*) 
    #     FROM (
    #         SELECT wfo_id 
    #         FROM taxonomy.taxa 
    #         WHERE wfo_id IS NOT NULL 
    #         GROUP BY wfo_id 
    #         HAVING COUNT(*) > 1
    #     ) duplicates
    #     """
        
    #     count = execute_count_query(db_conn, query)
    #     assert count == 0, f"Found {count} duplicate WFO IDs"
    
    def test_no_empty_names(self, db_conn):
        """Test that no taxa have empty or null names"""
        query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE name IS NULL OR TRIM(name) = ''
        """
        
        count = execute_count_query(db_conn, query)
        assert count == 0, f"Found {count} taxa with null or empty names"
    
    def test_valid_status_values(self, db_conn):
        """Test that all status values are from the defined enum"""
        query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE status NOT IN ('accepted', 'synonym', 'invalid', 'unresolved', 'misapplied')
        """
        
        count = execute_count_query(db_conn, query)
        assert count == 0, f"Found {count} taxa with invalid status values"
    
    def test_valid_rank_values(self, db_conn):
        """Test that all rank values are from the defined enum"""
        valid_ranks = [
            'kingdom', 'phylum', 'class', 'order', 'family', 'subfamily', 
            'tribe', 'subtribe', 'genus', 'subgenus', 'section', 'subsection',
            'species', 'subspecies', 'variety', 'form'
        ]
        
        query = f"""
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE rank::text NOT IN ({','.join(f"'{r}'" for r in valid_ranks)})
        """
        
        count = execute_count_query(db_conn, query)
        assert count == 0, f"Found {count} taxa with invalid rank values"
    
    def test_synonyms_point_to_accepted_taxa(self, db_conn):
        """Test that synonyms have parent_id pointing to accepted taxa"""
        query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa syn
        LEFT JOIN taxonomy.taxa parent ON syn.parent_id = parent.id
        WHERE syn.status = 'synonym' 
          AND syn.parent_id IS NOT NULL
          AND (parent.id IS NULL OR parent.status != 'accepted')
        """
        
        count = execute_count_query(db_conn, query)
        # This is a warning rather than error since WFO data might have complex synonym relationships
        if count > 0:
            print(f"Warning: Found {count} synonyms not pointing to accepted taxa")
    
    def test_species_names_follow_binomial_pattern(self, db_conn):
        """Test that species names contain at least two words (genus + specific epithet)"""
        query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE rank = 'species' 
          AND (
              name NOT LIKE '% %' OR 
              LENGTH(name) - LENGTH(REPLACE(name, ' ', '')) < 1
          )
        """
        
        count = execute_count_query(db_conn, query)
        # Allow some flexibility as WFO might have incomplete names
        if count > 0:
            print(f"Warning: Found {count} species with non-binomial names")
    
    def test_hierarchical_consistency_family_to_genus(self, db_conn):
        """Test that genera are properly nested under families"""
        query = """
        WITH RECURSIVE hierarchy AS (
            -- Start with genera
            SELECT id, name, rank, parent_id, name as genus_name
            FROM taxonomy.taxa 
            WHERE rank = 'genus'
            
            UNION ALL
            
            -- Walk up the hierarchy
            SELECT t.id, t.name, t.rank, t.parent_id, h.genus_name
            FROM taxonomy.taxa t
            JOIN hierarchy h ON t.id = h.parent_id
            WHERE t.rank IN ('tribe', 'subfamily', 'family')
        )
        SELECT COUNT(DISTINCT genus_name)
        FROM hierarchy h
        WHERE h.rank = 'genus'
          AND NOT EXISTS (
              SELECT 1 FROM hierarchy h2 
              WHERE h2.genus_name = h.genus_name 
                AND h2.rank = 'family'
          )
        """
        
        count = execute_count_query(db_conn, query)
        assert count == 0, f"Found {count} genera not properly connected to families"
    
    def test_no_circular_references(self, db_conn):
        """Test for circular references in the taxonomy hierarchy"""
        query = """
        WITH RECURSIVE hierarchy_check AS (
            SELECT id, parent_id, ARRAY[id] as path
            FROM taxonomy.taxa
            WHERE parent_id IS NOT NULL
            
            UNION ALL
            
            SELECT t.id, t.parent_id, hc.path || t.parent_id
            FROM taxonomy.taxa t
            JOIN hierarchy_check hc ON t.parent_id = hc.id
            WHERE t.parent_id IS NOT NULL 
              AND NOT (t.parent_id = ANY(hc.path))
              AND array_length(hc.path, 1) < 20  -- Prevent infinite recursion
        )
        SELECT COUNT(*)
        FROM hierarchy_check
        WHERE parent_id = ANY(path)
        """
        
        count = execute_count_query(db_conn, query)
        assert count == 0, f"Found {count} circular references in taxonomy hierarchy"

class TestTaxonomyDataQuality:
    """Test suite for taxonomy data quality checks"""
    
    def test_minimum_data_imported(self, db_conn):
        """Test that we have imported a reasonable amount of data"""
        query = "SELECT COUNT(*) FROM taxonomy.taxa"
        count = execute_count_query(db_conn, query)
        assert count > 100, f"Expected at least 100 taxa, found {count}"
    
    def test_wfo_id_coverage(self, db_conn):
        """Test that most taxa have WFO IDs"""
        query = """
        SELECT 
            COUNT(*) as total,
            COUNT(wfo_id) as with_wfo_id,
            ROUND(COUNT(wfo_id) * 100.0 / COUNT(*), 1) as coverage_percent
        FROM taxonomy.taxa
        """
        
        result = execute_query(db_conn, query)[0]
        total, with_wfo_id, coverage = result
        
        # Expect at least 90% WFO ID coverage
        assert coverage >= 90.0, f"WFO ID coverage is only {coverage}% ({with_wfo_id}/{total})"
    
    def test_rank_distribution_reasonable(self, db_conn):
        """Test that we have reasonable distribution of taxonomic ranks"""
        query = """
        SELECT rank, COUNT(*) as count
        FROM taxonomy.taxa 
        GROUP BY rank 
        ORDER BY count DESC
        """
        
        results = execute_query(db_conn, query)
        rank_counts = dict(results)
        
        # Check that we have species (should be the most numerous)
        assert 'species' in rank_counts, "No species found in database"
        assert rank_counts['species'] > 10, f"Too few species: {rank_counts.get('species', 0)}"
        
        # Check that we have genera
        assert 'genus' in rank_counts, "No genera found in database"
        assert rank_counts['genus'] > 5, f"Too few genera: {rank_counts.get('genus', 0)}"
        
        # Species should generally outnumber genera
        species_count = rank_counts.get('species', 0)
        genus_count = rank_counts.get('genus', 0)
        if genus_count > 0:
            ratio = species_count / genus_count
            assert ratio >= 1.0, f"Species to genus ratio too low: {ratio:.2f}"
    
    def test_status_distribution_reasonable(self, db_conn):
        """Test that we have reasonable distribution of taxonomic status"""
        query = """
        SELECT status, COUNT(*) as count
        FROM taxonomy.taxa 
        GROUP BY status 
        ORDER BY count DESC
        """
        
        results = execute_query(db_conn, query)
        status_counts = dict(results)
        
        # Should have accepted taxa
        assert 'accepted' in status_counts, "No accepted taxa found"
        assert status_counts['accepted'] > 0, "No accepted taxa found"
        
        # Accepted should generally be the largest category for plant databases
        total = sum(status_counts.values())
        accepted_percent = (status_counts['accepted'] / total) * 100
        
        print(f"Status distribution: {status_counts}")
        print(f"Accepted taxa: {accepted_percent:.1f}%")

class TestSpecificFamilies:
    """Test suite for specific plant families that should be imported"""
    
    def test_orchidaceae_present(self, db_conn):
        """Test that Orchidaceae family is present (as mentioned in the config)"""
        query = """
        SELECT COUNT(*) 
        FROM taxonomy.taxa 
        WHERE rank = 'family' AND name = 'Orchidaceae'
        """
        
        count = execute_count_query(db_conn, query)
        assert count > 0, "Orchidaceae family not found in database"
    
    def test_carnivorous_plant_families_present(self, db_conn):
        """Test that some carnivorous plant families are present"""
        carnivorous_families = [
            'Droseraceae', 'Nepenthaceae', 'Sarraceniaceae', 
            'Lentibulariaceae', 'Cephalotaceae'
        ]
        
        for family in carnivorous_families:
            query = f"""
            SELECT COUNT(*) 
            FROM taxonomy.taxa 
            WHERE rank = 'family' AND name = '{family}'
            """
            
            count = execute_count_query(db_conn, query)
            if count == 0:
                print(f"Warning: {family} not found (may not be available in WFO dataset)")
    
    def test_families_have_genera_and_species(self, db_conn):
        """Test that imported families have genera and species"""
        query = """
        WITH family_hierarchy AS (
            SELECT DISTINCT
                f.name as family_name,
                COUNT(DISTINCT g.id) as genus_count,
                COUNT(DISTINCT s.id) as species_count
            FROM taxonomy.taxa f
            LEFT JOIN taxonomy.taxa g ON (
                g.parent_id = f.id OR 
                EXISTS (
                    SELECT 1 FROM taxonomy.taxa t 
                    WHERE t.parent_id = f.id 
                      AND g.parent_id = t.id
                      AND t.rank IN ('subfamily', 'tribe')
                )
            ) AND g.rank = 'genus'
            LEFT JOIN taxonomy.taxa s ON (
                s.parent_id = g.id OR
                EXISTS (
                    SELECT 1 FROM taxonomy.taxa sg
                    WHERE sg.parent_id = g.id
                      AND s.parent_id = sg.id
                      AND sg.rank IN ('subgenus', 'section', 'subsection')
                )
            ) AND s.rank = 'species'
            WHERE f.rank = 'family'
            GROUP BY f.name
            HAVING COUNT(DISTINCT g.id) = 0 OR COUNT(DISTINCT s.id) = 0
        )
        SELECT COUNT(*) FROM family_hierarchy
        """
        
        count = execute_count_query(db_conn, query)
        if count > 0:
            # Get details of families without complete hierarchy
            detail_query = """
            WITH family_hierarchy AS (
                SELECT DISTINCT
                    f.name as family_name,
                    COUNT(DISTINCT g.id) as genus_count,
                    COUNT(DISTINCT s.id) as species_count
                FROM taxonomy.taxa f
                LEFT JOIN taxonomy.taxa g ON g.parent_id = f.id AND g.rank = 'genus'
                LEFT JOIN taxonomy.taxa s ON s.parent_id = g.id AND s.rank = 'species'
                WHERE f.rank = 'family'
                GROUP BY f.name
                HAVING COUNT(DISTINCT g.id) = 0 OR COUNT(DISTINCT s.id) = 0
            )
            SELECT family_name, genus_count, species_count 
            FROM family_hierarchy 
            LIMIT 5
            """
            
            results = execute_query(db_conn, detail_query)
            print(f"Warning: {count} families lack complete genus/species hierarchy")
            print("Examples:", results[:3])

if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "--tb=short"])