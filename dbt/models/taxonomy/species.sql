with taxonomy_unified as (
    select * from {{ ref('taxa') }}
),

genera as (
  SELECT * FROM taxonomy_unified
  WHERE rank IN ('species', 'subspecies', 'variety', 'form')
)


SELECT * FROM genera