WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'lego_final_data') }}

),

renamed AS (

    SELECT
        setid AS brickset_set_id,
        SAFE_CAST(number AS STRING) AS set_num,
        numbervariant AS number_variant,
        name AS set_name,
        year AS release_year,
        theme AS theme_name,
        themegroup AS theme_group,
        subtheme AS sub_theme,
        category,
        rating,
        reviewcount AS review_count,
        packagingtype AS packaging_type,
        availability,
        instructionscount AS instructions_count,
        tags,
        urlretailpricecheckpln AS retail_price_check_pln_url,
        us_retailprice AS us_retail_price_usd,
        uk_retailprice AS uk_retail_price_gbp,
        ca_retailprice AS ca_retail_price_cad,
        de_retailprice AS de_retail_price_eur,
        pl_retailprice AS pl_retail_price_pln,
        pricemonthpln AS price_month_pln,
        status AS set_status,
        urlretailpricehistorypln AS retail_price_pln_history_url,
        SAFE_CAST(pieces AS INT64) AS n_pieces,
        SAFE_CAST(minifigs AS INT64) AS n_minifigs,
        SAFE_CAST(ownedby AS INT64) AS n_users_owned_by,
        SAFE_CAST(wantedby AS INT64) AS n_users_wanted_by,
        SAFE_CAST(minage AS INT64) AS min_recommended_age,
        SAFE_CAST(maxage AS INT64) AS max_recommended_age,
        SAFE_CAST(released AS BOOL) AS is_released,
        SAFE_CAST(TIMESTAMP(lastupdated) AS DATE) AS last_updated_date,
        SAFE_CAST(TIMESTAMP(us_datefirstavailable) AS DATE)
            AS us_date_first_available,
        SAFE_CAST(TIMESTAMP(us_datelastavailable) AS DATE)
            AS us_date_last_available,
        SAFE_CAST(TIMESTAMP(uk_datefirstavailable) AS DATE)
            AS uk_date_first_available,
        SAFE_CAST(TIMESTAMP(uk_datelastavailable) AS DATE)
            AS uk_date_last_available,
        SAFE_CAST(TIMESTAMP(ca_datefirstavailable) AS DATE)
            AS ca_date_first_available,
        SAFE_CAST(TIMESTAMP(ca_datelastavailable) AS DATE)
            AS ca_date_last_available,
        SAFE_CAST(TIMESTAMP(de_datefirstavailable) AS DATE)
            AS de_date_first_available,
        SAFE_CAST(TIMESTAMP(de_datelastavailable) AS DATE)
            AS de_date_last_available,
        PARSE_DATE('%Y-%m', `date`) AS price_date

    FROM source

)

SELECT * FROM renamed
