{% snapshot snapshot__distribution_centers %}

{{
    config(
      target_schema='dbt_as',
      unique_key='id',
      strategy='check',
      check_cols=['name', 'latitude', 'longitude']
    )
}}

{#other strategy is timestamp#}

SELECT * FROM {{ source('thelook_ecommerce', 'distribution_centers') }}

{% endsnapshot %}