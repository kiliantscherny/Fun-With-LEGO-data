WITH pre_2000_data AS (
    SELECT * FROM {{ ref( 'base_lego_raw__brick_insights_ratings_reviews_pre_2000') }}
),

post_2000_data AS (SELECT * FROM {{ ref ('base_lego_raw__brick_insights_ratings_reviews_post_2000') }}),

union_cte AS (
    SELECT * FROM pre_2000_data
    UNION ALL
    SELECT * FROM post_2000_data
)

SELECT * FROM union_cte
