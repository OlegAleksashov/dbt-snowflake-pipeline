{% macro slugify(text) -%}
{{ text | replace(' ', '_') | upper }}
{%- endmacro %}