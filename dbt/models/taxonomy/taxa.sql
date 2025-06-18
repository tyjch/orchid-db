with merged_data as (
    select * from {{ ref('taxonomy_merged') }}
),

unified as (
    select 
        name,
        rank,
        MAX(generic_name) as generic_name,
        MAX(specific_name) as specific_name,
        MAX(infra_name) as infra_name,
        MAX(section) as section,
        MAX(subgenus) as subgenus,
        MAX(genus) as genus,
        MAX(subtribe) as subtribe,
        MAX(tribe) as tribe,
        MAX(subfamily) as subfamily,
        MAX(family) as family,
        MAX("order") as "order",
        MAX(class) as class,
        MAX(phylum) as phylum,
        MAX(kingdom) as kingdom,
        ARRAY_AGG(id) as external_ids,
        ARRAY_AGG(data_source) as data_sources
    from merged_data
    group by name, rank
),

unified_ids as (
    select * from unified
    where rank IN ('species', 'subspecies', 'form', 'variety')
    order by kingdom, phylum, class, "order", family, subfamily, tribe, subtribe, genus, subgenus, section, generic_name, specific_name, infra_name
)

select row_number() over (order by kingdom, phylum, class, "order", family, subfamily, tribe, subtribe, genus, subgenus, section, generic_name, specific_name, infra_name) as id, * 
from unified_ids
order by id