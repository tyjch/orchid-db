-- models/orchid_taxonomy_deduplicated.sql
-- This model removes fuzzy duplicates where OrchidWiz-only records have the same 
-- generic_name, specific_name, and infra_name but different ranks

with source_with_computed as (
  select 
    src.*,  -- Include all columns from the original table
    
    -- Count unique data sources (arrays)
    array_length(src.data_sources, 1) as source_count,
    
    -- Check if OrchidWiz is the only source
    (src.data_sources = ARRAY['OrchidWiz']) as orchidwiz_only
    
  from {{ ref('taxa') }} src  -- Replace with your actual source table reference
),

-- Find groups of potential duplicates (all sources, not just OrchidWiz)
duplicate_groups as (
  select 
    swc.generic_name,
    swc.specific_name,
    swc.infra_name,
    count(*) as group_size
  from source_with_computed swc
  where swc.infra_name is not null  -- Only look at infraspecific names
  group by swc.generic_name, swc.specific_name, swc.infra_name
  having count(*) > 1
),

-- Get all records that match duplicate groups (including non-OrchidWiz-only records)
all_potential_duplicates as (
  select 
    swc.*  -- Include all original columns plus computed ones
  from source_with_computed swc
  inner join duplicate_groups dg
    on swc.generic_name = dg.generic_name
    and swc.specific_name = dg.specific_name
    and swc.infra_name = dg.infra_name
),

-- Rank records within each duplicate group by source count (desc) and then by rank preference
ranked_duplicates as (
  select 
    apd.*,  -- Include all columns
    row_number() over (
      partition by apd.generic_name, apd.specific_name, apd.infra_name
      order by 
        apd.source_count desc,
        case apd.rank
          when 'subspecies' then 1
          when 'variety' then 2
          when 'form' then 3
          else 4
        end,
        apd.name  -- tie-breaker for consistency
    ) as rn
  from all_potential_duplicates apd
),

-- Get records to keep from duplicate groups and merge external_ids from all duplicates
records_to_keep_with_merged_ids as (
  select 
    rd.id,
    rd.name,
    rd.rank,
    rd.generic_name,
    rd.specific_name,
    rd.infra_name,
    rd.section,
    rd.subgenus,
    rd.genus,
    rd.subtribe,
    rd.tribe,
    rd.subfamily,
    rd.family,
    rd."order",
    rd.class,
    rd.phylum,
    rd.kingdom,
    -- Merge external_ids from all records in the duplicate group
    array(
      select distinct unnest(rd_all.external_ids)
      from ranked_duplicates rd_all
      where rd_all.generic_name = rd.generic_name
        and rd_all.specific_name = rd.specific_name
        and rd_all.infra_name = rd.infra_name
      order by unnest(rd_all.external_ids)
    ) as external_ids,
    rd.data_sources
  from ranked_duplicates rd
  where rd.rn = 1
),

-- Get records that are not part of duplicate groups (keep original external_ids)
non_duplicate_records as (
  select 
    swc.id,
    swc.name,
    swc.rank,
    swc.generic_name,
    swc.specific_name,
    swc.infra_name,
    swc.section,
    swc.subgenus,
    swc.genus,
    swc.subtribe,
    swc.tribe,
    swc.subfamily,
    swc.family,
    swc."order",
    swc.class,
    swc.phylum,
    swc.kingdom,
    swc.external_ids,
    swc.data_sources
  from source_with_computed swc
  left join duplicate_groups dg
    on swc.generic_name = dg.generic_name
    and swc.specific_name = dg.specific_name
    and swc.infra_name = dg.infra_name
  where dg.generic_name is null
),

-- Combine all records we want to keep
final_result as (
  select * from records_to_keep_with_merged_ids
  union all
  select * from non_duplicate_records
)

select 
  -- All original columns from taxa table
  id,
  name,
  rank,
  generic_name,
  specific_name,
  infra_name,
  section,
  subgenus,
  genus,
  subtribe,
  tribe,
  subfamily,
  family,
  "order",
  class,
  phylum,
  kingdom,
  external_ids,
  data_sources
from final_result
order by name