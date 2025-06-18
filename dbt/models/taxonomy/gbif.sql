with source_data as (
    select * from {{ source('raw', 'gbif_taxonomy') }}
),

final as (
    select
        concat('gbif-', "taxonID") as id,

        case when "parentNameUsageID" is not null 
             then concat('gbif-', "parentNameUsageID") 
             else null 
        end as parent_id,

        case when "acceptedNameUsageID" is not null 
             then concat('gbif-', "acceptedNameUsageID") 
             else null 
        end as accepted_id,

        case when "originalNameUsageID" is not null 
             then concat('gbif-', "originalNameUsageID") 
             else null 
        end as original_id,
        
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
        
        "specificEpithet" as specific_name,
        "infraspecificEpithet" as infra_name,
    
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
       
        lower("taxonomicStatus") as taxonomic_status,
        lower("nomenclaturalStatus") as nomenclatural_status,

        null as ipni_id,
        null as tpl_id,
        
        'GBIF' as data_source,
        current_timestamp as dbt_loaded_at
        
    FROM source_data
    WHERE kingdom = 'Plantae' and lower("taxonomicStatus") = 'accepted'
)

SELECT * FROM final