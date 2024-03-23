WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'brick_insights_reviews_data') }}

),

renamed AS (

    SELECT
        set_num,
        review_url,
        snippet,
        review_amount,
        rating_original,
        rating_converted,
        author_name

    FROM source

)

SELECT * FROM renamed
