WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'brick_insights_ratings_reviews_pre_2000') }}

),

renamed AS (

    SELECT
        set_num,
        review_url,
        snippet,
        review_amount,
        rating_original,
        rating_converted,
        author_name,
        fetched_at

    FROM source

)

SELECT * FROM renamed
