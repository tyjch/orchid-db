WITH wiz_source_records AS (
  SELECT DISTINCT 
    id as source_id,
    name as source_name
  FROM {{ ref('wiz') }}
  WHERE id IS NOT NULL
    AND id LIKE 'wiz-%'
),

final_wiz_external_ids AS (
  SELECT DISTINCT
    id as final_id,
    name as final_name,
    JSON_EXTRACT_SCALAR(external_id_entry, '$.source') as external_source,
    JSON_EXTRACT_SCALAR(external_id_entry, '$.id') as external_id
  FROM {{ ref('taxonomy_filled') }}
  CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(external_ids)) as external_id_entry
  WHERE JSON_EXTRACT_SCALAR(external_id_entry, '$.source') = 'wiz'
),

missing_wiz_records AS (
  SELECT w.*
  FROM wiz_source_records w
  LEFT JOIN final_wiz_external_ids f ON w.source_id = f.external_id
  WHERE f.external_id IS NULL
)

SELECT 
  source_id,
  source_name,
  'WIZ source record missing from final dataset' as error_message,
  'Expected to find in external_ids but did not' as details
FROM missing_wiz_records