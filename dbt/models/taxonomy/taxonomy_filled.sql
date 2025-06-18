WITH kingdom_taxonomy AS (
  SELECT 
    kingdom,
    MODE() WITHIN GROUP (ORDER BY phylum) FILTER (WHERE phylum IS NOT NULL) as best_phylum
  FROM "orchid-db"."dbt_taxonomy"."taxa_deduplicated"
  WHERE kingdom IS NOT NULL
  GROUP BY kingdom
),

phylum_taxonomy AS (
  SELECT 
    phylum,
    MODE() WITHIN GROUP (ORDER BY kingdom) FILTER (WHERE kingdom IS NOT NULL) as best_kingdom,
    MODE() WITHIN GROUP (ORDER BY class) FILTER (WHERE class IS NOT NULL) as best_class
  FROM "orchid-db"."dbt_taxonomy"."taxa_deduplicated"
  WHERE phylum IS NOT NULL
  GROUP BY phylum
),

class_taxonomy AS (
  SELECT 
    class,
    MODE() WITHIN GROUP (ORDER BY phylum) FILTER (WHERE phylum IS NOT NULL) as best_phylum,
    MODE() WITHIN GROUP (ORDER BY kingdom) FILTER (WHERE kingdom IS NOT NULL) as best_kingdom,
    MODE() WITHIN GROUP (ORDER BY "order") FILTER (WHERE "order" IS NOT NULL) as best_order
  FROM "orchid-db"."dbt_taxonomy"."taxa_deduplicated"
  WHERE class IS NOT NULL
  GROUP BY class
),

order_taxonomy AS (
  SELECT 
    "order",
    MODE() WITHIN GROUP (ORDER BY class) FILTER (WHERE class IS NOT NULL) as best_class,
    MODE() WITHIN GROUP (ORDER BY phylum) FILTER (WHERE phylum IS NOT NULL) as best_phylum,
    MODE() WITHIN GROUP (ORDER BY kingdom) FILTER (WHERE kingdom IS NOT NULL) as best_kingdom,
    MODE() WITHIN GROUP (ORDER BY family) FILTER (WHERE family IS NOT NULL) as best_family
  FROM "orchid-db"."dbt_taxonomy"."taxa_deduplicated"
  WHERE "order" IS NOT NULL
  GROUP BY "order"
),

family_taxonomy AS (
  SELECT 
    family,
    MODE() WITHIN GROUP (ORDER BY "order") FILTER (WHERE "order" IS NOT NULL) as best_order,
    MODE() WITHIN GROUP (ORDER BY class) FILTER (WHERE class IS NOT NULL) as best_class,
    MODE() WITHIN GROUP (ORDER BY phylum) FILTER (WHERE phylum IS NOT NULL) as best_phylum,
    MODE() WITHIN GROUP (ORDER BY kingdom) FILTER (WHERE kingdom IS NOT NULL) as best_kingdom,
    MODE() WITHIN GROUP (ORDER BY subfamily) FILTER (WHERE subfamily IS NOT NULL) as best_subfamily
  FROM "orchid-db"."dbt_taxonomy"."taxa_deduplicated"
  WHERE family IS NOT NULL
  GROUP BY family
),

subfamily_taxonomy AS (
  SELECT 
    subfamily,
    MODE() WITHIN GROUP (ORDER BY family) FILTER (WHERE family IS NOT NULL) as best_family,
    MODE() WITHIN GROUP (ORDER BY "order") FILTER (WHERE "order" IS NOT NULL) as best_order,
    MODE() WITHIN GROUP (ORDER BY class) FILTER (WHERE class IS NOT NULL) as best_class,
    MODE() WITHIN GROUP (ORDER BY phylum) FILTER (WHERE phylum IS NOT NULL) as best_phylum,
    MODE() WITHIN GROUP (ORDER BY kingdom) FILTER (WHERE kingdom IS NOT NULL) as best_kingdom,
    MODE() WITHIN GROUP (ORDER BY tribe) FILTER (WHERE tribe IS NOT NULL) as best_tribe,
    MODE() WITHIN GROUP (ORDER BY subtribe) FILTER (WHERE subtribe IS NOT NULL) as best_subtribe
  FROM "orchid-db"."dbt_taxonomy"."taxa_deduplicated"
  WHERE subfamily IS NOT NULL
  GROUP BY subfamily
),

tribe_taxonomy AS (
  SELECT 
    tribe,
    MODE() WITHIN GROUP (ORDER BY subfamily) FILTER (WHERE subfamily IS NOT NULL) as best_subfamily,
    MODE() WITHIN GROUP (ORDER BY family) FILTER (WHERE family IS NOT NULL) as best_family,
    MODE() WITHIN GROUP (ORDER BY "order") FILTER (WHERE "order" IS NOT NULL) as best_order,
    MODE() WITHIN GROUP (ORDER BY class) FILTER (WHERE class IS NOT NULL) as best_class,
    MODE() WITHIN GROUP (ORDER BY phylum) FILTER (WHERE phylum IS NOT NULL) as best_phylum,
    MODE() WITHIN GROUP (ORDER BY kingdom) FILTER (WHERE kingdom IS NOT NULL) as best_kingdom,
    MODE() WITHIN GROUP (ORDER BY subtribe) FILTER (WHERE subtribe IS NOT NULL) as best_subtribe
  FROM "orchid-db"."dbt_taxonomy"."taxa_deduplicated"
  WHERE tribe IS NOT NULL
  GROUP BY tribe
),

subtribe_taxonomy AS (
  SELECT 
    subtribe,
    MODE() WITHIN GROUP (ORDER BY tribe) FILTER (WHERE tribe IS NOT NULL) as best_tribe,
    MODE() WITHIN GROUP (ORDER BY subfamily) FILTER (WHERE subfamily IS NOT NULL) as best_subfamily,
    MODE() WITHIN GROUP (ORDER BY family) FILTER (WHERE family IS NOT NULL) as best_family,
    MODE() WITHIN GROUP (ORDER BY "order") FILTER (WHERE "order" IS NOT NULL) as best_order,
    MODE() WITHIN GROUP (ORDER BY class) FILTER (WHERE class IS NOT NULL) as best_class,
    MODE() WITHIN GROUP (ORDER BY phylum) FILTER (WHERE phylum IS NOT NULL) as best_phylum,
    MODE() WITHIN GROUP (ORDER BY kingdom) FILTER (WHERE kingdom IS NOT NULL) as best_kingdom,
    MODE() WITHIN GROUP (ORDER BY genus) FILTER (WHERE genus IS NOT NULL) as best_genus
  FROM "orchid-db"."dbt_taxonomy"."taxa_deduplicated"
  WHERE subtribe IS NOT NULL
  GROUP BY subtribe
),

genus_taxonomy AS (
  SELECT 
    genus,
    MODE() WITHIN GROUP (ORDER BY subtribe) FILTER (WHERE subtribe IS NOT NULL) as best_subtribe,
    MODE() WITHIN GROUP (ORDER BY tribe) FILTER (WHERE tribe IS NOT NULL) as best_tribe,
    MODE() WITHIN GROUP (ORDER BY subfamily) FILTER (WHERE subfamily IS NOT NULL) as best_subfamily,
    MODE() WITHIN GROUP (ORDER BY family) FILTER (WHERE family IS NOT NULL) as best_family,
    MODE() WITHIN GROUP (ORDER BY "order") FILTER (WHERE "order" IS NOT NULL) as best_order,
    MODE() WITHIN GROUP (ORDER BY class) FILTER (WHERE class IS NOT NULL) as best_class,
    MODE() WITHIN GROUP (ORDER BY phylum) FILTER (WHERE phylum IS NOT NULL) as best_phylum,
    MODE() WITHIN GROUP (ORDER BY kingdom) FILTER (WHERE kingdom IS NOT NULL) as best_kingdom
  FROM "orchid-db"."dbt_taxonomy"."taxa_deduplicated"
  WHERE genus IS NOT NULL
  GROUP BY genus
),

hierarchically_filled AS (
  SELECT 
    t.id,
    t.name,
    t.rank,
    COALESCE(t.generic_name, t.genus) as generic_name,
    t.specific_name,
    t.infra_name,
    t.section,
    t.subgenus,
    t.genus,
    
    -- Fill subtribe hierarchically
    COALESCE(
      t.subtribe, 
      gt.best_subtribe,
      sft.best_subtribe,
      tt.best_subtribe
    ) as subtribe,
    
    -- Fill tribe hierarchically  
    COALESCE(
      t.tribe,
      gt.best_tribe,
      stt.best_tribe,
      sft.best_tribe
    ) as tribe,
    
    -- Fill subfamily hierarchically
    COALESCE(
      t.subfamily,
      gt.best_subfamily,
      stt.best_subfamily,
      tt.best_subfamily,
      ft.best_subfamily
    ) as subfamily,
    
    -- Fill family hierarchically
    COALESCE(
      t.family,
      gt.best_family,
      stt.best_family,
      tt.best_family,
      sft.best_family,
      ot.best_family
    ) as family,
    
    -- Fill order hierarchically
    COALESCE(
      t."order",
      gt.best_order,
      stt.best_order,
      tt.best_order,
      sft.best_order,
      ft.best_order,
      ct.best_order
    ) as "order",
    
    -- Fill class hierarchically
    COALESCE(
      t.class,
      gt.best_class,
      stt.best_class,
      tt.best_class,
      sft.best_class,
      ft.best_class,
      ot.best_class,
      pt.best_class
    ) as class,
    
    -- Fill phylum hierarchically  
    COALESCE(
      t.phylum,
      gt.best_phylum,
      stt.best_phylum,
      tt.best_phylum,
      sft.best_phylum,
      ft.best_phylum,
      ot.best_phylum,
      ct.best_phylum,
      kt.best_phylum
    ) as phylum,
    
    -- Fill kingdom hierarchically
    COALESCE(
      t.kingdom,
      gt.best_kingdom,
      stt.best_kingdom,
      tt.best_kingdom,
      sft.best_kingdom,
      ft.best_kingdom,
      ot.best_kingdom,
      ct.best_kingdom,
      pt.best_kingdom
    ) as kingdom,
    
    t.external_ids,
    t.data_sources
    
  FROM "orchid-db"."dbt_taxonomy"."taxa_deduplicated" t
  LEFT JOIN genus_taxonomy gt ON t.genus = gt.genus
  LEFT JOIN subtribe_taxonomy stt ON t.subtribe = stt.subtribe
  LEFT JOIN tribe_taxonomy tt ON t.tribe = tt.tribe  
  LEFT JOIN subfamily_taxonomy sft ON t.subfamily = sft.subfamily
  LEFT JOIN family_taxonomy ft ON t.family = ft.family
  LEFT JOIN order_taxonomy ot ON t."order" = ot."order"
  LEFT JOIN class_taxonomy ct ON t.class = ct.class
  LEFT JOIN phylum_taxonomy pt ON t.phylum = pt.phylum
  LEFT JOIN kingdom_taxonomy kt ON t.kingdom = kt.kingdom
)

SELECT * FROM hierarchically_filled