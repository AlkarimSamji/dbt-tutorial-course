{#
	This test is basically a "not_null" and "unique"
	rolled into one.

	It fails if a column is NULL or occurs more than once
#}
/* EXAMPLE OF A GENERIC TEST */
{% test primary_key(model, column_name) %}

WITH validation AS (
	SELECT
		{{ column_name }} AS primary_key,
		COUNT(1) AS occurrences

	FROM {{ model }}
	GROUP BY 1
)

SELECT *

FROM validation
WHERE primary_key IS NULL
	OR occurrences > 1

{% endtest %}

{% test col_greater_than(model, column_name, value=0)%}
    SELECT
        column_name AS row_that_failed,
        value
    FROM {{ model }}
    WHERE {{ column_name }} <= {{ value }}

{% endtest %}