{# a comment that won't appear #}
--a comment that will appear
{% set my_long_var -%}
    SELECT 1 AS my_col
{%- endset %}

{{ my_long_var }}
{{ my_long_var }}
{{ my_long_var }}

{% set my_list = ['user_id','created_at'] %}

SELECT
{%- for item in my_list %}    
    {{item}}{% if not loop.last %},{% endif %}
{%- endfor %}


{{ target.name }}
{{ target.location }}
{{ target.type }}