{% macro tokenize_identifier(expression) -%}
sha2(concat('{{ env_var("TOKENIZATION_SALT", "local-demo-tokenization-salt") }}', '::', cast({{ expression }} as string)), 256)
{%- endmacro %}
