{{
  config(
    materialized='view'
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
)

select * from unified
order by name, data_source