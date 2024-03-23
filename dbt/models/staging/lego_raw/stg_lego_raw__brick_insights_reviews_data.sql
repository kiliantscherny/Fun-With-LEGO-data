WITH pre_2000_data AS (
    SELECT * FROM {{ ref( 'base_lego_raw__brick_insights_reviews_data_pre_2000') }}
),

post_2000_data AS (SELECT * FROM {{ ref ('base_lego_raw__brick_insights_reviews_data') }}),

union_cte AS (
    SELECT * FROM pre_2000_data
    UNION ALL
    SELECT * FROM post_2000_data
)

SELECT * FROM union_cte
