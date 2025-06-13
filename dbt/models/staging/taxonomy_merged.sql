-- models/staging/taxonomy_merged.sql
{{
  config(
    materialized = 'table'
  )
}}

with wfo_data as (
    select * from {{ ref('taxonomy_wfo') }}
),

gbif_data as (
    select * from {{ ref('taxonomy_gbif') }}
),

orchidwiz_data as (
    select * from {{ ref('taxonomy_wiz') }}
),

unified as (
    select
        id,
        parent_id,
        accepted_id,
        original_id,
        name,
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
        generic_name,
        rank,
        taxonomic_status,
        nomenclatural_status,
        ipni_id,
        tpl_id,
        data_source,
        null as section  -- Add section column for consistency
    from wfo_data
    
    union all
    
    select
        id,
        parent_id,
        accepted_id,
        original_id,
        name,
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
        generic_name,
        rank,
        taxonomic_status,
        nomenclatural_status,
        ipni_id,
        tpl_id,
        data_source,
        null as section  -- Add section column for consistency
    from gbif_data
    
    union all
    
    select
        id,
        parent_id,
        accepted_id,
        original_id,
        name,
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
        generic_name,
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
        
        -- Normalize botanical abbreviations and clean name
        {{ normalize_botanical_names('name') }} as name,
        
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
        
        -- Extract subgenus from name if not already populated
        coalesce(
            subgenus,
            {{ extract_taxonomic_rank('name', 'subg') }}
        ) as subgenus,
        
        generic_name,
        rank,
        taxonomic_status,
        nomenclatural_status,
        ipni_id,
        tpl_id,
        data_source,
        
        -- Extract section from name if not already populated
        coalesce(
            section,
            {{ extract_taxonomic_rank('name', 'sect') }}
        ) as section
        
    from unified
)

select * from normalized
order by name, data_source