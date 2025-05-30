-- Unified Database Schema for Orchid Database
-- This file creates the complete database schema in the correct order
-- It's idempotent - can be run multiple times safely

-- =============================================================================
-- SCHEMAS
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS taxonomy;
CREATE SCHEMA IF NOT EXISTS breeding;
CREATE SCHEMA IF NOT EXISTS crosswalk;

-- =============================================================================
-- ENUMS
-- =============================================================================

-- Drop and recreate enums to ensure they're up to date
DROP TYPE IF EXISTS rank CASCADE;
CREATE TYPE rank AS ENUM (
    'kingdom',
    'phylum',
    'class',
    'order',
    'family',
    'subfamily',
    'tribe',
    'subtribe',
    'genus',
    'subgenus',
    'section',
    'subsection',
    'species',
    'subspecies',
    'variety',
    'form'
);

DROP TYPE IF EXISTS status CASCADE;
CREATE TYPE status AS ENUM (
    'accepted',
    'synonym',
    'invalid',
    'unresolved',
    'misapplied'
);

DROP TYPE IF EXISTS parentage CASCADE;
CREATE TYPE parentage AS (
    seed    INTEGER,
    pollen  INTEGER
);

-- =============================================================================
-- TAXONOMY TABLES
-- =============================================================================

-- Valid hierarchical relationships lookup table
DROP TABLE IF EXISTS taxonomy.valid_hierarchies CASCADE;
CREATE TABLE taxonomy.valid_hierarchies (
    id          SERIAL PRIMARY KEY,
    parent_rank rank NOT NULL,
    child_rank  rank NOT NULL,
    is_direct   BOOLEAN NOT NULL DEFAULT TRUE,
    notes       TEXT,

    UNIQUE(parent_rank, child_rank)
);

-- Insert valid relationships
INSERT INTO taxonomy.valid_hierarchies (parent_rank, child_rank, is_direct, notes) VALUES
('kingdom', 'phylum', TRUE, 'Standard kingdom-phylum relationship'),
('phylum', 'class', TRUE, 'Standard phylum-class relationship'),
('class', 'order', TRUE, 'Standard class-order relationship'),
('order', 'family', TRUE, 'Standard order-family relationship'),
('family', 'tribe', TRUE, 'Family can contain tribes'),
('family', 'genus', TRUE, 'Family can directly contain genus (no tribe)'),
('tribe', 'genus', TRUE, 'Standard tribe-genus relationship'),
('genus', 'subgenus', TRUE, 'Genus can contain subgenera'),
('genus', 'section', TRUE, 'Genus can contain sections'),
('genus', 'species', TRUE, 'Genus can directly contain species'),
('subgenus', 'section', TRUE, 'Subgenus can contain sections'),
('subgenus', 'species', TRUE, 'Species can be under subgenus'),
('section', 'subsection', TRUE, 'Section can contain subsections'),
('section', 'species', TRUE, 'Species can be under section'),
('subsection', 'species', TRUE, 'Species can be under subsection'),
('species', 'subspecies', TRUE, 'Standard species-subspecies relationship'),
('species', 'variety', TRUE, 'Species can have varieties'),
('subspecies', 'variety', TRUE, 'Subspecies can have varieties'),
('variety', 'form', TRUE, 'Variety can have forms')
ON CONFLICT (parent_rank, child_rank) DO NOTHING;

-- Main taxonomy table
DROP TABLE IF EXISTS taxonomy.taxa CASCADE;
CREATE TABLE taxonomy.taxa (
    id          SERIAL          PRIMARY KEY,
    name        VARCHAR(255)    NOT NULL,
    rank        rank            NOT NULL,
    parent_id   INTEGER         REFERENCES taxonomy.taxa(id),
    status      status          NOT NULL DEFAULT 'accepted',
    created_at  TIMESTAMP       DEFAULT NOW(),
    updated_at  TIMESTAMP       DEFAULT NOW(),

    CONSTRAINT no_self_reference CHECK (id != parent_id)
);

-- Indexes for taxonomy.taxa
CREATE INDEX idx_taxa_name ON taxonomy.taxa (name);
CREATE INDEX idx_taxa_rank ON taxonomy.taxa (rank);
CREATE INDEX idx_taxa_status ON taxonomy.taxa (status);
CREATE INDEX idx_taxa_parent_id ON taxonomy.taxa (parent_id);
CREATE INDEX idx_taxa_parent_rank ON taxonomy.taxa (parent_id, rank);

-- =============================================================================
-- CROSSWALK TABLES
-- =============================================================================

-- WFO (World Flora Online) crosswalk
DROP TABLE IF EXISTS crosswalk.wfo CASCADE;
CREATE TABLE crosswalk.wfo (
    wfo_id          VARCHAR(50)     PRIMARY KEY,        -- External WFO ID
    taxa_id         INTEGER         NOT NULL,           -- FK to taxonomy.taxa.id
    created_at      TIMESTAMP       DEFAULT NOW(),
    updated_at      TIMESTAMP       DEFAULT NOW(),
    
    CONSTRAINT fk_wfo_taxa FOREIGN KEY (taxa_id) REFERENCES taxonomy.taxa(id) ON DELETE CASCADE
);

CREATE INDEX idx_wfo_taxa_id ON crosswalk.wfo (taxa_id);

-- GBIF crosswalk
DROP TABLE IF EXISTS crosswalk.gbif CASCADE;
CREATE TABLE crosswalk.gbif (
    gbif_id         INTEGER         PRIMARY KEY,        -- GBIF taxon key
    taxa_id         INTEGER         NOT NULL,           -- FK to taxonomy.taxa.id
    created_at      TIMESTAMP       DEFAULT NOW(),
    updated_at      TIMESTAMP       DEFAULT NOW(),
    
    CONSTRAINT fk_gbif_taxa FOREIGN KEY (taxa_id) REFERENCES taxonomy.taxa(id) ON DELETE CASCADE
);

CREATE INDEX idx_gbif_taxa_id ON crosswalk.gbif (taxa_id);

-- IPNI crosswalk (International Plant Names Index)
DROP TABLE IF EXISTS crosswalk.ipni CASCADE;
CREATE TABLE crosswalk.ipni (
    ipni_id         VARCHAR(50)     PRIMARY KEY,        -- IPNI ID
    taxa_id         INTEGER         NOT NULL,           -- FK to taxonomy.taxa.id
    created_at      TIMESTAMP       DEFAULT NOW(),
    updated_at      TIMESTAMP       DEFAULT NOW(),
    
    CONSTRAINT fk_ipni_taxa FOREIGN KEY (taxa_id) REFERENCES taxonomy.taxa(id) ON DELETE CASCADE
);

CREATE INDEX idx_ipni_taxa_id ON crosswalk.ipni (taxa_id);

-- COL crosswalk (Catalogue of Life)
DROP TABLE IF EXISTS crosswalk.col CASCADE;
CREATE TABLE crosswalk.col (
    col_id          VARCHAR(50)     PRIMARY KEY,        -- COL ID
    taxa_id         INTEGER         NOT NULL,           -- FK to taxonomy.taxa.id
    created_at      TIMESTAMP       DEFAULT NOW(),
    updated_at      TIMESTAMP       DEFAULT NOW(),
    
    CONSTRAINT fk_col_taxa FOREIGN KEY (taxa_id) REFERENCES taxonomy.taxa(id) ON DELETE CASCADE
);

CREATE INDEX idx_col_taxa_id ON crosswalk.col (taxa_id);

-- TPL crosswalk (The Plant List)
DROP TABLE IF EXISTS crosswalk.tpl CASCADE;
CREATE TABLE crosswalk.tpl (
    tpl_id          VARCHAR(50)     PRIMARY KEY,        -- TPL ID
    taxa_id         INTEGER         NOT NULL,           -- FK to taxonomy.taxa.id
    created_at      TIMESTAMP       DEFAULT NOW(),
    updated_at      TIMESTAMP       DEFAULT NOW(),
    
    CONSTRAINT fk_tpl_taxa FOREIGN KEY (taxa_id) REFERENCES taxonomy.taxa(id) ON DELETE CASCADE
);

CREATE INDEX idx_tpl_taxa_id ON crosswalk.tpl (taxa_id);

-- =============================================================================
-- BREEDING TABLES
-- =============================================================================

-- Breeding/parentage tracking
DROP TABLE IF EXISTS breeding.parents CASCADE;
CREATE TABLE breeding.parents (
    id              SERIAL PRIMARY KEY,
    taxa_id         INTEGER REFERENCES taxonomy.taxa(id),
    parentage       parentage,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),

    type VARCHAR(10) GENERATED ALWAYS AS (
        CASE
            WHEN parentage IS NULL THEN 'species'
            WHEN parentage IS NOT NULL THEN 'hybrid'
        END
    ) STORED,

    CONSTRAINT has_valid_parentage CHECK (
        parentage IS NULL OR
        ((parentage).seed IS NOT NULL AND (parentage).pollen IS NOT NULL)
    ),

    CONSTRAINT no_self_parentage CHECK (
        parentage IS NULL OR
        (id != (parentage).seed AND id != (parentage).pollen)
    )
);

CREATE INDEX idx_breeding_parents_taxa_id ON breeding.parents (taxa_id);
CREATE INDEX idx_breeding_parents_type ON breeding.parents (type);

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- Function to get taxa_id from WFO ID
CREATE OR REPLACE FUNCTION get_taxa_id_from_wfo(wfo_external_id VARCHAR(50))
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT taxa_id FROM crosswalk.wfo WHERE wfo_id = wfo_external_id);
END;
$$ LANGUAGE plpgsql;

-- Function to get WFO ID from taxa_id
CREATE OR REPLACE FUNCTION get_wfo_id_from_taxa(internal_taxa_id INTEGER)
RETURNS VARCHAR(50) AS $$
BEGIN
    RETURN (SELECT wfo_id FROM crosswalk.wfo WHERE taxa_id = internal_taxa_id);
END;
$$ LANGUAGE plpgsql;

-- Function to get taxa_id from GBIF ID
CREATE OR REPLACE FUNCTION get_taxa_id_from_gbif(gbif_external_id INTEGER)
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT taxa_id FROM crosswalk.gbif WHERE gbif_id = gbif_external_id);
END;
$$ LANGUAGE plpgsql;

-- Function to get GBIF ID from taxa_id
CREATE OR REPLACE FUNCTION get_gbif_id_from_taxa(internal_taxa_id INTEGER)
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT gbif_id FROM crosswalk.gbif WHERE taxa_id = internal_taxa_id);
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- VIEWS
-- =============================================================================

-- View to join taxa with all external IDs
DROP VIEW IF EXISTS taxonomy.taxa_with_external_ids CASCADE;
CREATE VIEW taxonomy.taxa_with_external_ids AS
SELECT 
    t.id,
    t.name,
    t.rank,
    t.parent_id,
    t.status,
    t.created_at,
    t.updated_at,
    w.wfo_id,
    g.gbif_id,
    i.ipni_id,
    c.col_id,
    tp.tpl_id
FROM taxonomy.taxa t
LEFT JOIN crosswalk.wfo w ON t.id = w.taxa_id
LEFT JOIN crosswalk.gbif g ON t.id = g.taxa_id
LEFT JOIN crosswalk.ipni i ON t.id = i.taxa_id
LEFT JOIN crosswalk.col c ON t.id = c.taxa_id
LEFT JOIN crosswalk.tpl tp ON t.id = tp.taxa_id;

-- View for hierarchical queries (useful for recursive operations)
DROP VIEW IF EXISTS taxonomy.taxa_hierarchy CASCADE;
CREATE VIEW taxonomy.taxa_hierarchy AS
WITH RECURSIVE hierarchy AS (
    -- Root nodes (no parent)
    SELECT 
        t.id,
        t.name,
        t.rank,
        t.parent_id,
        t.status,
        0 as level,
        ARRAY[t.id] as path,
        t.name as lineage
    FROM taxonomy.taxa t
    WHERE t.parent_id IS NULL
    
    UNION ALL
    
    -- Recursive: children
    SELECT 
        t.id,
        t.name,
        t.rank,
        t.parent_id,
        t.status,
        h.level + 1,
        h.path || t.id,
        h.lineage || ' > ' || t.name
    FROM taxonomy.taxa t
    JOIN hierarchy h ON t.parent_id = h.id
    WHERE t.id != ALL(h.path)  -- Prevent cycles
      AND h.level < 20  -- Prevent infinite recursion
)
SELECT * FROM hierarchy;

-- =============================================================================
-- SCHEMA VERIFICATION
-- =============================================================================

-- Create a function to verify schema integrity
CREATE OR REPLACE FUNCTION verify_schema_integrity()
RETURNS TABLE(check_name TEXT, status TEXT, details TEXT) AS $$
BEGIN
    -- Check schemas exist
    RETURN QUERY
    SELECT 
        'Schemas'::TEXT as check_name,
        CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END as status,
        'Found ' || COUNT(*) || ' schemas (expected 3)' as details
    FROM information_schema.schemata 
    WHERE schema_name IN ('taxonomy', 'breeding', 'crosswalk');
    
    -- Check main tables exist
    RETURN QUERY
    SELECT 
        'Main Tables'::TEXT as check_name,
        CASE WHEN COUNT(*) >= 3 THEN 'PASS' ELSE 'FAIL' END as status,
        'Found ' || COUNT(*) || ' main tables' as details
    FROM information_schema.tables 
    WHERE table_schema = 'taxonomy' AND table_name IN ('taxa', 'valid_hierarchies');
    
    -- Check crosswalk tables exist
    RETURN QUERY
    SELECT 
        'Crosswalk Tables'::TEXT as check_name,
        CASE WHEN COUNT(*) >= 5 THEN 'PASS' ELSE 'FAIL' END as status,
        'Found ' || COUNT(*) || ' crosswalk tables' as details
    FROM information_schema.tables 
    WHERE table_schema = 'crosswalk';
    
    -- Check foreign keys exist
    RETURN QUERY
    SELECT 
        'Foreign Keys'::TEXT as check_name,
        CASE WHEN COUNT(*) >= 5 THEN 'PASS' ELSE 'FAIL' END as status,
        'Found ' || COUNT(*) || ' foreign key constraints' as details
    FROM information_schema.table_constraints 
    WHERE constraint_type = 'FOREIGN KEY' 
      AND table_schema IN ('taxonomy', 'breeding', 'crosswalk');
    
    -- Check helper functions exist
    RETURN QUERY
    SELECT 
        'Helper Functions'::TEXT as check_name,
        CASE WHEN COUNT(*) >= 4 THEN 'PASS' ELSE 'FAIL' END as status,
        'Found ' || COUNT(*) || ' helper functions' as details
    FROM information_schema.routines 
    WHERE routine_schema = 'public' 
      AND routine_name LIKE '%taxa%';
END;
$$ LANGUAGE plpgsql;

-- Run verification and display results
SELECT 'Schema creation completed. Verification results:' as message;
SELECT * FROM verify_schema_integrity();