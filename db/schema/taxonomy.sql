CREATE SCHEMA IF NOT EXISTS taxonomy;
CREATE SCHEMA IF NOT EXISTS breeding;

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

DROP TABLE IF EXISTS taxonomy.valid_hierarchies CASCADE;
CREATE TABLE taxonomy.valid_hierarchies (
    id          SERIAL PRIMARY KEY,
    parent_rank rank NOT NULL,
    child_rank  rank NOT NULL,
    is_direct   BOOLEAN NOT NULL DEFAULT TRUE,
    notes       TEXT,

    UNIQUE(parent_rank, child_rank)
);

-- Insert the valid relationships for your use case
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
('variety', 'form', TRUE, 'Variety can have forms');

DROP TABLE IF EXISTS taxonomy.taxa CASCADE;
CREATE TABLE taxonomy.taxa (
    id          SERIAL          PRIMARY KEY,
    name        VARCHAR(255)    NOT NULL,
    rank        rank            NOT NULL,
    parent_id   INTEGER         REFERENCES taxonomy.taxa(id),
    status      status          NOT NULL DEFAULT 'accepted',

--     CONSTRAINT valid_rank_hierarchy CHECK (
--         parent_id IS NULL OR
--         EXISTS (
--             SELECT 1 FROM taxonomy.valid_hierarchies vh
--             WHERE vh.parent_rank = (SELECT rank FROM taxonomy.taxa WHERE id = parent_id)
--             AND vh.child_rank = rank
--         )
--     ),

    CONSTRAINT no_self_reference CHECK (id != parent_id)
);
CREATE UNIQUE INDEX unique_species_names
    ON taxonomy.taxa (name)
    WHERE rank = 'species';
CREATE UNIQUE INDEX unique_name_within_parent
    ON taxonomy.taxa (name, parent_id)
    WHERE rank != 'species';

DROP TYPE IF EXISTS parentage CASCADE;
CREATE TYPE parentage AS (
    seed    INTEGER,
    pollen  INTEGER
);

DROP TABLE IF EXISTS breeding.parents CASCADE;
CREATE TABLE breeding.parents (
    id              SERIAL PRIMARY KEY,
    taxa_id         INTEGER REFERENCES taxonomy.taxa(id),
    parentage       parentage,

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

DROP INDEX IF EXISTS unique_species_names;
DROP INDEX IF EXISTS unique_name_within_parent;

-- Add the wfo_id column to taxonomy.taxa
ALTER TABLE taxonomy.taxa
ADD COLUMN wfo_id VARCHAR(50);

-- Make wfo_id unique (this is the real unique identifier)
CREATE UNIQUE INDEX unique_wfo_id ON taxonomy.taxa (wfo_id);

-- Optional: Add an index on name for faster searches (but allow duplicates)
CREATE INDEX idx_taxa_name ON taxonomy.taxa (name);

-- Try dropping the constraint by name
ALTER TABLE taxonomy.taxa DROP CONSTRAINT IF EXISTS unique_species_names;

-- Also drop any partial unique indexes that might still exist
DROP INDEX IF EXISTS taxonomy.unique_species_names;
DROP INDEX IF EXISTS taxonomy.unique_name_within_parent;