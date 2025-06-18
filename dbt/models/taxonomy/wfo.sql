with source_data as (
    select * from {{ source('raw', 'wfo_taxonomy') }}
),

-- Add deduplication step
deduplicated as (
    select *,
        row_number() over (
            partition by scientificname 
            order by 
                case when taxonid like 'wfo-0000%' then 1 else 2 end,  -- Prefer WCS
                created desc  -- Then by most recent
        ) as rn
    from source_data
    where taxonid is not null
        and scientificname is not null
        and trim(scientificname) != ''
        and taxonomicstatus = 'Accepted'
),

final as (
    select
        taxonid as id,
        parentnameusageid as parent_id,
        acceptednameusageid as accepted_id,
        originalnameusageid as original_id,
        scientificname as name,
        specificepithet as specific_name,
        infraspecificepithet as infra_name,
        null as kingdom,
        null as phylum,
        null as class,
        null as "order",
        family,
        subfamily,
        tribe,
        subtribe,
        genus,
        subgenus,
        null as generic_name,
        taxonrank as rank,
        case 
            when upper(taxonomicstatus) = 'ACCEPTED' then 'accepted'
            when upper(taxonomicstatus) = 'SYNONYM' then 'synonym'
            when upper(taxonomicstatus) = 'INVALID' then 'invalid'
            when upper(taxonomicstatus) = 'UNRESOLVED' then 'unresolved'
            when upper(taxonomicstatus) = 'MISAPPLIED' then 'misapplied'
            else lower(taxonomicstatus)
        end as taxonomic_status,
        case 
            when upper(nomenclaturalstatus) = 'LEGITIMATE' then 'legitimate'
            when upper(nomenclaturalstatus) = 'ILLEGITIMATE' then 'illegitimate'
            when upper(nomenclaturalstatus) = 'SUPERFLUOUS' then 'superfluous'
            when upper(nomenclaturalstatus) = 'REJECTED' then 'rejected'
            when upper(nomenclaturalstatus) = 'INVALID' then 'invalid'
            else lower(nomenclaturalstatus)
        end as nomenclatural_status,
        -- External identifiers
        case 
            when scientificnameid like 'urn:lsid:ipni.org:names:%'
            then split_part(scientificnameid, ':', -1)
            else scientificnameid
        end as ipni_id,
        tplid as tpl_id,
        'WFO' as data_source,
        current_timestamp as dbt_loaded_at
    from deduplicated
    where rn = 1  -- Only keep the first (preferred) record for each name
)

select * from final