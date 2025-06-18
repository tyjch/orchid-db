
  
    

  create  table "orchid-db"."public"."taxonomy_merged__dbt_tmp"
  
  
    as
  
  (
    -- models/staging/taxonomy_merged.sql


with wfo_data as (
    select * from "orchid-db"."public"."taxonomy_wfo"
),

gbif_data as (
    select * from "orchid-db"."public_taxonomy"."taxonomy_gbif"
),

orchidwiz_data as (
    select * from "orchid-db"."public"."taxonomy_wiz"
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
        
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    name,
                    ' subsp\. ', ' ssp. ', 'g'
                ),
                ' fo\. ', ' f. ', 'g'
            ),
            ' sect\. [A-Z][a-z]*', '', 'g'
        ),
        ' subg\. [A-Z][a-z]*', '', 'g'
    )
 as name,
        
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
            
    CASE 
        WHEN name ~ ' subg\. [A-Z][a-z]*'
        THEN substring(name, ' subg\. ([A-Z][a-z]*)')
        ELSE NULL
    END

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
            
    CASE 
        WHEN name ~ ' sect\. [A-Z][a-z]*'
        THEN substring(name, ' sect\. ([A-Z][a-z]*)')
        ELSE NULL
    END

        ) as section
        
    from unified
)

select * from normalized
order by name, data_source
  );
  