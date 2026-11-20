{# Watermark for incremental models: last load time minus a lookback window, so late-committed rows are re-read.
   Models using it must recompute whole groups idempotently (delete+insert on a natural key). #}
{% macro incremental_watermark() -%}
(select max(_loaded_at) - interval '{{ var("lookback_hours", 1) }} hours' from {{ this }})
{%- endmacro %}

{% macro rate(numerator, denominator) -%}
round({{ numerator }}::numeric / nullif({{ denominator }}, 0), 4)
{%- endmacro %}
