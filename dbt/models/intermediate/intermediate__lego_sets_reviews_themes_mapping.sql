WITH reviews AS (
    SELECT * EXCEPT (fetched_at) FROM {{ ref('stg_lego_raw__brick_insights_ratings_and_reviews') }}
),

sets AS (SELECT * EXCEPT (inserted_at) FROM {{ ref('stg_lego_raw__sets') }}),

themes AS (SELECT * EXCEPT (inserted_at) FROM {{ ref('stg_lego_raw__themes') }}),

reviews_joined_sets AS (
    SELECT
        reviews.*,
        sets.* EXCEPT (set_num),
        themes.* EXCEPT (theme_id),
        parent_themes.theme_name AS parent_theme_name
    FROM reviews
    LEFT JOIN sets ON reviews.set_num = sets.set_num
    LEFT JOIN themes ON sets.theme_id = themes.theme_id
    LEFT JOIN themes AS parent_themes ON themes.parent_id = parent_themes.theme_id
)

SELECT *
FROM reviews_joined_sets
