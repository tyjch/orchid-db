with wfo_data as (
    select * from {{ ref('wfo') }}
),

gbif_data as (
    select * from {{ ref('gbif') }}
),

orchidwiz_data as (
    select * from {{ ref('wiz') }}
),

unified as (
    select
        id,
        parent_id,
        accepted_id,
        original_id,
        name,
        generic_name,
        specific_name,
        infra_name,
        kingdom,
        phylum,
        class,
        "order",
        family,
        subfamily,
        tribe,
        subtribe,
        genus,
        subgenus,
        rank,
        taxonomic_status,
        nomenclatural_status,
        ipni_id,
        tpl_id,
        data_source,
        null as section
    from wfo_data
    
    union all
    
    select
        id,
        parent_id,
        accepted_id,
        original_id,
        name,
        generic_name,
        specific_name,
        infra_name,
        kingdom,
        phylum,
        class,
        "order",
        family,
        subfamily,
        tribe,
        subtribe,
        genus,
        subgenus,
        rank,
        taxonomic_status,
        nomenclatural_status,
        ipni_id,
        tpl_id,
        data_source,
        null as section
    from gbif_data
    
    union all
    
    select
        id,
        parent_id,
        accepted_id,
        original_id,
        name,
        generic_name,
        specific_name,
        infra_name,
        kingdom,
        phylum,
        class,
        "order",
        family,
        subfamily,
        tribe,
        subtribe,
        genus,
        subgenus,
        rank,
        taxonomic_status,
        nomenclatural_status,
        ipni_id,
        tpl_id,
        data_source,
        section
    from orchidwiz_data
),

normalized as (
    select
        id,
        parent_id,
        accepted_id,
        original_id,
        {{ normalize_botanical_names('name') }} as name,
        generic_name,
        specific_name,
        infra_name,
        kingdom,
        phylum,
        class,
        "order",
        family,
        subfamily,
        tribe,
        subtribe,
        genus,
        coalesce(
            subgenus,
            {{ extract_taxonomic_rank('name', 'subg') }}
        ) as subgenus,
        coalesce(
            section,
            {{ extract_taxonomic_rank('name', 'sect') }}
        ) as section,
        rank,
        taxonomic_status,
        nomenclatural_status,
        ipni_id,
        tpl_id,
        data_source
    from unified
    where rank != 'unranked'
)

select * from normalized
order by name, data_source