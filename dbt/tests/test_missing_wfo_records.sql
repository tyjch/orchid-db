WITH wfo_source_records AS (
  SELECT DISTINCT 
    id as source_id,
    name as source_name
  FROM {{ ref('wfo') }}
  WHERE id IS NOT NULL
    AND id LIKE 'wfo-%'
),

final_wfo_external_ids AS (
  SELECT DISTINCT
    id as final_id,
    name as final_name,
    JSON_EXTRACT_SCALAR(external_id_entry, '$.source') as external_source,
    JSON_EXTRACT_SCALAR(external_id_entry, '$.id') as external_id
  FROM {{ ref('taxonomy_filled') }}
  CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(external_ids)) as external_id_entry
  WHERE JSON_EXTRACT_SCALAR(external_id_entry, '$.source') = 'wfo'
),

missing_wfo_records AS (
  SELECT w.*
  FROM wfo_source_records w
  LEFT JOIN final_wfo_external_ids f ON w.source_id = f.external_id
  WHERE f.external_id IS NULL
)

SELECT 
  source_id,
  source_name,
  'WFO source record missing from final dataset' as error_message,
  'Expected to find in external_ids but did not' as details
FROM missing_wfo_records