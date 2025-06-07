{{
  config(
    materialized='table'
  )
}}

with species_names_data as (
    select * from {{ source('raw', 'wiz_species_names') }}
),

genera_data as (
    select * from {{ source('raw', 'wiz_genera') }}
),

classifications_data as (
    select * from {{ source('raw', 'wiz_classifications') }}
),

infras_data as (
    select * from {{ source('raw', 'wiz_infras') }}
),

-- Main species and varieties from SpeciesNames (primary source)
main_taxa as (
    select
        sn."SpName"::integer as spname_id,
        sn."Species"::integer as species_id,
        sn."Genus"::text as genus_abbrev,
        sn."Name"::text as full_name,  -- Could be species, variety, etc.
        
        -- Determine rank from name structure
        case 
            when sn."Name" like '% var. %' or sn."Name" like '% v. %' or sn."Name" like '% nv. %' or sn."Name" like '% Br. fol. %' then 'variety'
            when sn."Name" like '% ssp. %' or sn."Name" like '% subsp. %' then 'subspecies'
            when sn."Name" like '% f. %' or sn."Name" like '% forma %' or sn."Name" like '% nm. %' or sn."Name" like '% monstr. %' then 'form'
            when sn."Name" like '% h.v. %' then 'variety'  -- horticultural variety
            when sn."Name" like '% h.f. %' then 'form'     -- horticultural form
            when sn."Name" like '% sp. %' then 'species'   -- unidentified species
            else 'species'
        end as determined_rank,
        
        -- Extract species epithet and infraspecific parts
        case 
            when sn."Name" like '% var. %' then split_part(sn."Name", ' var. ', 1)
            when sn."Name" like '% ssp. %' then split_part(sn."Name", ' ssp. ', 1)  
            when sn."Name" like '% subsp. %' then split_part(sn."Name", ' subsp. ', 1)
            when sn."Name" like '% f. %' then split_part(sn."Name", ' f. ', 1)
            when sn."Name" like '% h.v. %' then split_part(sn."Name", ' h.v. ', 1)
            when sn."Name" like '% h.f. %' then split_part(sn."Name", ' h.f. ', 1)
            else sn."Name"
        end as species_epithet,
        
        case 
            when sn."Name" like '% var. %' then split_part(sn."Name", ' var. ', 2)
            when sn."Name" like '% ssp. %' then split_part(sn."Name", ' ssp. ', 2)
            when sn."Name" like '% subsp. %' then split_part(sn."Name", ' subsp. ', 2)  
            when sn."Name" like '% f. %' then split_part(sn."Name", ' f. ', 2)
            when sn."Name" like '% h.v. %' then split_part(sn."Name", ' h.v. ', 2)
            when sn."Name" like '% h.f. %' then split_part(sn."Name", ' h.f. ', 2)
            else null
        end as infraspecific_epithet
        
    from species_names_data sn
    where sn."Name" is not null
        and trim(sn."Name") != ''
        and sn."Genus" is not null
        and trim(sn."Genus") != ''
),

-- Add infraspecific taxa from Infras table
infra_taxa as (
    select
        null::integer as spname_id,  -- No SpName ID for infras
        inf."Species"::integer as species_id,
        inf."Genus"::text as genus_abbrev,
        inf."Name"::text as full_name,
        
        -- Determine rank from name structure
        case 
            when inf."Name" like '% var. %' then 'variety'
            when inf."Name" like '% ssp. %' or inf."Name" like '% subsp. %' then 'subspecies'
            when inf."Name" like '% f. %' or inf."Name" like '% forma %' then 'form'
            when inf."Name" like '% nothovar. %' then 'variety'  -- nothovariety
            else 'subspecies'  -- Default for infras
        end as determined_rank,
        
        -- Extract species epithet and infraspecific parts  
        case 
            when inf."Name" like '% var. %' then split_part(inf."Name", ' var. ', 1)
            when inf."Name" like '% ssp. %' then split_part(inf."Name", ' ssp. ', 1)
            when inf."Name" like '% subsp. %' then split_part(inf."Name", ' subsp. ', 1)
            when inf."Name" like '% nothovar. %' then split_part(inf."Name", ' nothovar. ', 1)
            when inf."Name" like '% f. %' then split_part(inf."Name", ' f. ', 1)
            else inf."Name"
        end as species_epithet,
        
        case 
            when inf."Name" like '% var. %' then split_part(inf."Name", ' var. ', 2)
            when inf."Name" like '% ssp. %' then split_part(inf."Name", ' ssp. ', 2)
            when inf."Name" like '% subsp. %' then split_part(inf."Name", ' subsp. ', 2)
            when inf."Name" like '% nothovar. %' then split_part(inf."Name", ' nothovar. ', 2)
            when inf."Name" like '% f. %' then split_part(inf."Name", ' f. ', 2)
            else null
        end as infraspecific_epithet
        
    from infras_data inf
    where inf."Name" is not null
        and trim(inf."Name") != ''
        and inf."Genus" is not null
        and trim(inf."Genus") != ''
),

-- Union all taxa
all_taxa as (
    select * from main_taxa
    union all
    select * from infra_taxa
),

final as (
    select
        -- IDs (use SpName if available, otherwise generate from Species + sequence)
        case 
            when t.spname_id is not null then concat('wiz-', t.spname_id)
            else concat('wiz-infra-', t.species_id, '-', row_number() over (partition by t.species_id, t.genus_abbrev order by t.full_name))
        end as id,
        
        -- Parent relationships (link infraspecifics to their species)
        case 
            when t.determined_rank in ('subspecies', 'variety', 'form') 
            then (
                select concat('wiz-', mt.spname_id)
                from main_taxa mt 
                where mt.species_id = t.species_id 
                    and mt.genus_abbrev = t.genus_abbrev
                    and mt.species_epithet = t.species_epithet
                    and mt.determined_rank = 'species'
                limit 1
            )
            else null 
        end as parent_id,
        
        null as accepted_id,  -- Could link synonyms to accepted names
        null as original_id,
        
        -- Scientific name construction with standardized formatting
        case 
            when g."Name" is not null then 
                case 
                    when t.full_name like '% h.v. %' then 
                        concat(g."Name", ' ', replace(t.full_name, ' h.v. ', ' var. '))
                    when t.full_name like '% h.f. %' then 
                        concat(g."Name", ' ', replace(t.full_name, ' h.f. ', ' f. '))
                    when t.full_name like '% v. %' then 
                        concat(g."Name", ' ', replace(t.full_name, ' v. ', ' var. '))
                    when t.full_name like '% nm. %' then 
                        concat(g."Name", ' ', replace(t.full_name, ' nm. ', ' f. '))
                    when t.full_name like '% nv. %' then 
                        concat(g."Name", ' ', replace(t.full_name, ' nv. ', ' var. '))
                    when t.full_name like '% monstr. %' then 
                        concat(g."Name", ' ', replace(t.full_name, ' monstr. ', ' f. '))
                    when t.full_name like '% subsp. %' then 
                        concat(g."Name", ' ', replace(t.full_name, ' subsp. ', ' ssp. '))
                    when t.full_name like '% sp. %' then 
                        concat(g."Name", ' ', replace(t.full_name, ' sp. ', ' '))
                    when t.full_name like '% Br. fol. %' then 
                        concat(g."Name", ' ', replace(t.full_name, ' Br. fol. ', ' var. '))
                    else 
                        concat(g."Name", ' ', t.full_name)
                end
            else 
                case 
                    when t.full_name like '% h.v. %' then 
                        concat(t.genus_abbrev, ' ', replace(t.full_name, ' h.v. ', ' var. '))
                    when t.full_name like '% h.f. %' then 
                        concat(t.genus_abbrev, ' ', replace(t.full_name, ' h.f. ', ' f. '))
                    when t.full_name like '% v. %' then 
                        concat(t.genus_abbrev, ' ', replace(t.full_name, ' v. ', ' var. '))
                    when t.full_name like '% nm. %' then 
                        concat(t.genus_abbrev, ' ', replace(t.full_name, ' nm. ', ' f. '))
                    when t.full_name like '% nv. %' then 
                        concat(t.genus_abbrev, ' ', replace(t.full_name, ' nv. ', ' var. '))
                    when t.full_name like '% monstr. %' then 
                        concat(t.genus_abbrev, ' ', replace(t.full_name, ' monstr. ', ' f. '))
                    when t.full_name like '% subsp. %' then 
                        concat(t.genus_abbrev, ' ', replace(t.full_name, ' subsp. ', ' ssp. '))
                    when t.full_name like '% sp. %' then 
                        concat(t.genus_abbrev, ' ', replace(t.full_name, ' sp. ', ' '))
                    when t.full_name like '% Br. fol. %' then 
                        concat(t.genus_abbrev, ' ', replace(t.full_name, ' Br. fol. ', ' var. '))
                    else 
                        concat(t.genus_abbrev, ' ', t.full_name)
                end
        end as name,
        
        -- Name components
        t.species_epithet as specific_name,
        t.infraspecific_epithet as infra_name,
        
        -- Taxonomic hierarchy from genera + classifications
        'Plantae' as kingdom,
        'Tracheophyta' as phylum,
        'Liliopsida' as class,
        'Asparagales' as "order",
        'Orchidaceae' as family,
        g."Subfamily" as subfamily,
        g."Tribe" as tribe,
        g."Subtribe" as subtribe,
        coalesce(g."Name", t.genus_abbrev) as genus,
        c."Subgenus" as subgenus,
        coalesce(g."Name", t.genus_abbrev) as generic_name,
        
        -- Classification and status
        t.determined_rank as rank,
        'accepted' as taxonomic_status,  -- Simplified since OrchidWiz is a curated list
        'valid' as nomenclatural_status,
        
        -- External identifiers
        null as ipni_id,
        null as tpl_id,
        
        -- Metadata
        'OrchidWiz' as data_source,
        
        -- Additional OrchidWiz-specific fields
        c."Section" as section
        
    from all_taxa t
    left join genera_data g on t.genus_abbrev = g."Genus"
    left join classifications_data c on t.species_id::text = c."Species"::text
)

select * from final
order by name