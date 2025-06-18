
  
    

  create  table "orchid-db"."public"."taxonomy_unified__dbt_tmp"
  
  
    as
  
  (
    

with merged_data as (
    select * from "orchid-db"."public"."taxonomy_merged"
),

unified as (
    select 
        name,
        MAX(kingdom) as kingdom,
        MAX(phylum) as phylum,
        MAX(class) as class,
        MAX("order") as "order",
        MAX(family) as family,
        MAX(subfamily) as subfamily,
        MAX(tribe) as tribe,
        MAX(subtribe) as subtribe,
        MAX(genus) as genus,
        MAX(subgenus) as subgenus,
        MAX(section) as section,
        rank
    from merged_data
    where taxonomic_status = 'accepted' 
        and rank != 'unranked'
    group by name, rank
)

select * from unified
order by kingdom, phylum, class, "order", family, subfamily, tribe, subtribe, genus, subgenus, name
  );
  