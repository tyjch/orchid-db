{% macro fill_taxonomy_gaps() %}
  WITH genus_taxonomy AS (
    SELECT 
      genus,
      -- Get the most frequent non-null value for each taxonomic rank
      {% for rank in ['family', 'order', 'class', 'phylum', 'kingdom', 'subfamily', 'tribe', 'subtribe'] %}
      MODE() WITHIN GROUP (ORDER BY {{ rank }}) FILTER (WHERE {{ rank }} IS NOT NULL) as best_{{ rank }}
      {%- if not loop.last -%},{%- endif %}
      {% endfor %}
    FROM {{ this }}
    WHERE genus IS NOT NULL
    GROUP BY genus
  ),
  
  filled_taxonomy AS (
    SELECT 
      t.*,
      {% for rank in ['family', 'order', 'class', 'phylum', 'kingdom', 'subfamily', 'tribe', 'subtribe'] %}
      COALESCE(t.{{ rank }}, gt.best_{{ rank }}) as filled_{{ rank }}
      {%- if not loop.last -%},{%- endif %}
      {% endfor %}
    FROM {{ this }} t
    LEFT JOIN genus_taxonomy gt ON t.genus = gt.genus
  )
  
  SELECT * FROM filled_taxonomy
{% endmacro %}