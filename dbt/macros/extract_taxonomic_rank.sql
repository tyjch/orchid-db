{% macro extract_taxonomic_rank(column_name, abbreviation) %}
    CASE 
        WHEN {{ column_name }} ~ ' {{ abbreviation }}\. [A-Z][a-z]*'
        THEN substring({{ column_name }}, ' {{ abbreviation }}\. ([A-Z][a-z]*)')
        ELSE NULL
    END
{% endmacro %}