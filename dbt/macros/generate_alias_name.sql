{# Mart tables get the documented physical names (dim.journal, dws.journal_statistics, ads.paper_search):
   the dim_/dws_/ads_ prefix only exists on the dbt model name, which keeps ref() names unambiguous. #}
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- if custom_alias_name -%}{{ custom_alias_name | trim }}
  {%- elif node.fqn[1] == 'marts' -%}{{ node.name | replace('dim_', '', 1) | replace('dws_', '', 1) | replace('ads_', '', 1) }}
  {%- else -%}{{ node.name }}{%- endif -%}
{%- endmacro %}
