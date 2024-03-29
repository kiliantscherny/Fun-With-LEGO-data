WITH set_prices AS (SELECT * FROM {{ ref('stg_lego_raw__lego_aggregated_data') }}),

deduplicated_set_prices AS (
    SELECT
        *,
        {{ convert_currency_to_eur('us_retail_price_usd', 'USD') }} AS us_retail_price_eur,
        {{ convert_currency_to_eur('uk_retail_price_gbp', 'GBP') }} AS uk_retail_price_eur,
        {{ convert_currency_to_eur('ca_retail_price_cad', 'CAD') }} AS ca_retail_price_eur,
        {{ convert_currency_to_eur('pl_retail_price_pln', 'PLN') }} AS pl_retail_price_eur
    FROM set_prices
    QUALIFY ROW_NUMBER() OVER (PARTITION BY set_num ORDER BY last_updated_date) = 1
)

SELECT
    *,
    -- Workaround to get the highest and lowest prices
    IF(GREATEST(
        COALESCE(us_retail_price_eur, 0),
        COALESCE(uk_retail_price_eur, 0),
        COALESCE(ca_retail_price_eur, 0),
        COALESCE(pl_retail_price_eur, 0),
        COALESCE(de_retail_price_eur, 0)
    ) = 0, NULL, GREATEST(
        COALESCE(us_retail_price_eur, 0),
        COALESCE(uk_retail_price_eur, 0),
        COALESCE(ca_retail_price_eur, 0),
        COALESCE(pl_retail_price_eur, 0),
        COALESCE(de_retail_price_eur, 0)
    )) AS highest_international_retail_price_eur,
    IF(LEAST(
        COALESCE(us_retail_price_eur, 999999999),
        COALESCE(uk_retail_price_eur, 999999999),
        COALESCE(ca_retail_price_eur, 999999999),
        COALESCE(pl_retail_price_eur, 999999999),
        COALESCE(de_retail_price_eur, 999999999)
    ) = 999999999, NULL, LEAST(
        COALESCE(us_retail_price_eur, 999999999),
        COALESCE(uk_retail_price_eur, 999999999),
        COALESCE(ca_retail_price_eur, 999999999),
        COALESCE(pl_retail_price_eur, 999999999),
        COALESCE(de_retail_price_eur, 999999999)
    )) AS lowest_international_retail_price_eur
FROM deduplicated_set_prices
