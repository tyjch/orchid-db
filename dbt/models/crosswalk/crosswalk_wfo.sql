with merged_data as (
    select * from {{ ref('taxonomy_merged') }}
),

taxa as (  -- Remove the second "with" keyword
    select * from {{ ref('taxa') }}
)

select t.id as id, d.id as external_id from taxa t
join merged_data d on (
    t.rank = d.rank 
    AND t.generic_name  = d.generic_name 
    AND t.specific_name = d.specific_name
    AND t.infra_name    = d.infra_name
)
where d.data_source = 'WFO'  -- Also specify which table the column comes from
