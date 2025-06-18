WITH gbif_source_records AS (
  SELECT DISTINCT 
    id as source_id,
    name as source_name
  FROM {{ ref('gbif') }}
  WHERE id IS NOT NULL
    AND id LIKE 'gbif-%'
),

final_gbif_external_ids AS (
  SELECT DISTINCT
    id as final_id,
    name as final_name,
    JSON_EXTRACT_SCALAR(external_id_entry, '$.source') as external_source,
    JSON_EXTRACT_SCALAR(external_id_entry, '$.id') as external_id
  FROM {{ ref('taxonomy_filled') }}
  CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(external_ids)) as external_id_entry
  WHERE JSON_EXTRACT_SCALAR(external_id_entry, '$.source') = 'gbif'
),

missing_gbif_records AS (
  SELECT g.*
  FROM gbif_source_records g
  LEFT JOIN final_gbif_external_ids f ON g.source_id = f.external_id
  WHERE f.external_id IS NULL
)

SELECT 
  source_id,
  source_name,
  'GBIF source record missing from final dataset' as error_message,
  'Expected to find in external_ids but did not' as details
FROM missing_gbif_records