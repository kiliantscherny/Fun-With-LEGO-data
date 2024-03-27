WITH sets_reviews_themes_mapping AS (
    SELECT * FROM {{ ref('intermediate__lego_sets_reviews_themes_mapping') }}
),

reviews_per_set AS (
    SELECT
        set_num,
        set_name,
        release_year,
        theme_id,
        theme_name,
        parent_id,
        parent_theme_name,
        num_parts,
        COUNT(*) AS review_count,
        ROUND(AVG(rating_converted), 2) AS avg_rating,
        MIN(rating_converted) AS min_rating,
        MAX(rating_converted) AS max_rating,
        APPROX_QUANTILES(rating_converted, 2)[OFFSET(1)] AS median_rating
    FROM sets_reviews_themes_mapping
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

SELECT *
FROM reviews_per_set
