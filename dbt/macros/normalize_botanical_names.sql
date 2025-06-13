{% macro normalize_botanical_names(column_name) %}
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    {{ column_name }},
                    ' subsp\. ', ' ssp. ', 'g'
                ),
                ' fo\. ', ' f. ', 'g'
            ),
            ' sect\. [A-Z][a-z]*', '', 'g'
        ),
        ' subg\. [A-Z][a-z]*', '', 'g'
    )
{% endmacro %}
