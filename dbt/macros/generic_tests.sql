{% test regex_match(model, column_name, pattern) %}
select {{ column_name }} from {{ model }}
where {{ column_name }} is not null and {{ column_name }} !~ '{{ pattern }}'
{% endtest %}

{% test non_negative(model, column_name) %}
select {{ column_name }} from {{ model }} where {{ column_name }} < 0
{% endtest %}

{# Completeness-rate guard: fails when the rate is below the threshold (use severity: warn for soft targets). #}
{% test rate_at_least(model, column_name, threshold) %}
select {{ column_name }} from {{ model }} where {{ column_name }} < {{ threshold }}
{% endtest %}
