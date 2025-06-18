

with source_data as (
    select * from "orchid-db"."raw"."gbif_taxonomy"
),

final as (
    select
        -- IDs (prefix with gbif-)
        concat('gbif-', "taxonID") as id,
        case when "parentNameUsageID" is not null 
             then concat('gbif-', "parentNameUsageID") 
             else null end as parent_id,
        case when "acceptedNameUsageID" is not null 
             then concat('gbif-', "acceptedNameUsageID") 
             else null end as accepted_id,
        case when "originalNameUsageID" is not null 
             then concat('gbif-', "originalNameUsageID") 
             else null end as original_id,
        
        -- Name with WFO-style formatting for infraspecifics
        case 
            when "taxonRank" = 'subspecies' and "infraspecificEpithet" is not null 
                then concat("genericName", ' ', "specificEpithet", ' subsp. ', "infraspecificEpithet")
            when "taxonRank" = 'variety' and "infraspecificEpithet" is not null 
                then concat("genericName", ' ', "specificEpithet", ' var. ', "infraspecificEpithet")
            when "taxonRank" = 'form' and "infraspecificEpithet" is not null 
                then concat("genericName", ' ', "specificEpithet", ' f. ', "infraspecificEpithet")
            else coalesce("canonicalName", "scientificName")
        end as name,
        
        -- Name components
        "specificEpithet" as specific_name,
        "infraspecificEpithet" as infra_name,
        
        -- Taxonomic hierarchy (GBIF has broad hierarchy)
        kingdom,
        phylum,
        class,
        "order",
        family,
        null as subfamily,
        null as tribe,
        null as subtribe,
        genus,
        null as subgenus,
        "genericName" as generic_name,
        
        -- Classification and status
        "taxonRank" as rank,
        case 
            when upper("taxonomicStatus") = 'ACCEPTED' then 'accepted'
            when upper("taxonomicStatus") = 'SYNONYM' then 'synonym'
            when upper("taxonomicStatus") = 'INVALID' then 'invalid'
            when upper("taxonomicStatus") = 'UNRESOLVED' then 'unresolved'
            when upper("taxonomicStatus") = 'MISAPPLIED' then 'misapplied'
            else lower("taxonomicStatus")
        end as taxonomic_status,
        case 
            when upper("nomenclaturalStatus") = 'LEGITIMATE' then 'legitimate'
            when upper("nomenclaturalStatus") = 'ILLEGITIMATE' then 'illegitimate'
            when upper("nomenclaturalStatus") = 'SUPERFLUOUS' then 'superfluous'
            when upper("nomenclaturalStatus") = 'REJECTED' then 'rejected'
            when upper("nomenclaturalStatus") = 'INVALID' then 'invalid'
            else lower("nomenclaturalStatus")
        end as nomenclatural_status,
        
        -- External identifiers (not available in GBIF)
        null as ipni_id,
        null as tpl_id,
        
        -- Metadata
        'GBIF' as data_source,
        current_timestamp as dbt_loaded_at
        
    from source_data
    where 
        -- Basic data quality filters
        "taxonID" is not null
        and coalesce("canonicalName", "scientificName") is not null
        and trim(coalesce("canonicalName", "scientificName")) != ''
        and kingdom = 'Plantae'  -- Only plants
)

select * from final