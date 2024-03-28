WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'brick_insights_ratings_and_reviews') }}

),

renamed AS (

    SELECT
        set_num,
        review_url,
        snippet,
        SAFE_CAST(review_amount AS INT64) AS review_amount,
        rating_original,
        SAFE_CAST(rating_converted AS FLOAT64) AS rating_converted,
        author_name,
        SAFE_CAST(fetched_at AS TIMESTAMP) AS fetched_at

    FROM source

)

SELECT * FROM renamed
