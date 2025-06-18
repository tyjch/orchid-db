# DBT Testing

## Creating Custom Tests
### Create Test Macro
```sql
-- macros/test_not_empty_string.sql
{% macro test_not_empty_string(model, column_name) %}
    select *
    from {{ model }}
    where trim({{ column_name }}) = '' or {{ column_name }} is null
{% endmacro %}
```
### Using Test Macros
```yaml
models:
  - name: customers
    columns:
      - name: name
        tests:
          - not_empty_string
```

## Model-level Tests
```yaml
models:
  - name: my_model
    tests:
      - dbt_utils.expression_is_true:
          expression: "col_a + col_b = col_c"
```

## Built-In Tests
- unique
- not_null
- accepted_values
- relationships

```yaml
models:
  - name: orders
    columns:
      - name: order_id
        tests:
          - unique          # No duplicate order IDs
          - not_null        # Every row must have an order ID
      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'shipped', 'delivered', 'cancelled']
      - name: customer_id
        tests:
          - relationships:
              to: ref('customers')
              field: customer_id    # Must exist in customers table
```

### `dbt_utils`

### Table Shape
#### `equal_rowcount`
```yaml
models:
  - name: my_model
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: ref('other_model')
```

#### `fewer_rows_than`
```yaml
models:
  - name: my_model
    tests:
      - dbt_utils.fewer_rows_than:
          compare_model: ref('other_model')
```

### Column Values
#### `accepted_range`
```yaml
models:
  - name: orders
    columns:
      - name: amount
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 10000
```

#### `not_accepted_values`
```yaml
models:
  - name: users
    columns:
      - name: status
        tests:
          - dbt_utils.not_accepted_values:
              values: ['deleted', 'banned']
```

#### `not_null_proportion`
```yaml
models:
  - name: customers
    columns:
      - name: email
        tests:
          - dbt_utils.not_null_proportion:
              at_least: 0.95  # At least 95% should be non-null
```

### Expressions & Logic
#### `expression_is_true`
```yaml
models:
  - name: orders
    tests:
      - dbt_utils.expression_is_true:
          expression: "col_a + col_b = col_c"
```

#### `recency`
```yaml
models:
  - name: events
    tests:
      - dbt_utils.recency:
          datepart: day
          field: created_at
          interval: 1  # Within last 1 day
```

### Uniqueness
#### `unique_combination_of_columns`
```yaml
models:
  - name: order_items
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id
            - product_id
```

### Sequential Values
#### `sequential_values`
```yaml
models:
  - name: monthly_data
    columns:
      - name: month_number
        tests:
          - dbt_utils.sequential_values:
              interval: 1
              datepart: month
```

### Cardinality
#### `cardinality_equality`
```yaml
models:
  - name: my_model
    tests:
      - dbt_utils.cardinality_equality:
          field: customer_id
          to: ref('customers')
          to_field: id
```

### Mutual Exclusivity
#### `mutually_exclusive_ranges`
```yaml
models:
  - name: promotions
    tests:
      - dbt_utils.mutually_exclusive_ranges:
          lower_bound_column: start_date
          upper_bound_column: end_date
          partition_by: product_id  # Optional grouping
```

### Data Comparison
#### `equality`
```yaml
models:
  - name: my_model
    tests:
      - dbt_utils.equality:
          compare_model: ref('expected_results')
```




# Tests
- Test that names don't have spaces in them (e.g. generic_name, specific_name, infra_name should all be one word).
- Test that all entries in OrchidWiz and WFO are added to final taxonomy table.
- Test that there are no taxonomic abbreviations that are unsupported.
- Test that periods only exist in ranks lower than species. (Uh, except we have sections, subgenera, etc.)
- Test that all name columns do not contain numbers.
- Test that no unranked rows exist

