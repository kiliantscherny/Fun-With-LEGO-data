WITH sets_reviews_themes_mapping AS (
    SELECT * FROM {{ ref('intermediate__lego_sets_reviews_themes_mapping') }}
),

sets_pricing AS (SELECT * FROM {{ ref('intermediate__lego_sets_prices') }}),

reviews_per_set AS (
    SELECT
        set_num,
        -- Required for joining to some other data
        REGEXP_REPLACE(set_num, r'-.*', '') AS clean_set_num,
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
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)

SELECT
    reviews_per_set.*,
    sets_pricing.us_retail_price_eur,
    sets_pricing.uk_retail_price_eur,
    sets_pricing.ca_retail_price_eur,
    sets_pricing.pl_retail_price_eur,
    sets_pricing.de_retail_price_eur,
    sets_pricing.lowest_international_retail_price_eur,
    sets_pricing.highest_international_retail_price_eur
FROM reviews_per_set
LEFT JOIN sets_pricing ON reviews_per_set.clean_set_num = sets_pricing.set_num
