{%- macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ default_schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- end -%}
{%- endmacro -%}