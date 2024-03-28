WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'lego_final_data') }}

),

renamed AS (

    SELECT
        setid AS brickset_set_id,
        number AS set_num,
        numbervariant AS number_variant,
        name AS set_name,
        year AS release_year,
        theme AS theme_name,
        themegroup AS theme_group,
        subtheme AS sub_theme,
        category,
        pieces AS n_pieces,
        minifigs AS n_minifigs,
        ownedby AS n_users_owned_by,
        wantedby AS n_users_wanted_by,
        rating,
        reviewcount AS review_count,
        packagingtype AS packaging_type,
        availability,
        instructionscount AS instructions_count,
        minage AS min_recommended_age,
        maxage AS max_recommended_age,
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
        SAFE_CAST(released AS BOOL) AS is_released,
        SAFE_CAST(lastupdated AS TIMESTAMP) AS last_updated_at,
        SAFE_CAST(us_datefirstavailable AS DATE) AS us_date_first_available,
        SAFE_CAST(us_datelastavailable AS DATE) AS us_date_last_available,
        SAFE_CAST(uk_datefirstavailable AS DATE) AS uk_date_first_available,
        SAFE_CAST(uk_datelastavailable AS DATE) AS uk_date_last_available,
        SAFE_CAST(ca_datefirstavailable AS DATE) AS ca_date_first_available,
        SAFE_CAST(ca_datelastavailable AS DATE) AS ca_date_last_available,
        SAFE_CAST(de_datefirstavailable AS DATE) AS de_date_first_available,
        SAFE_CAST(de_datelastavailable AS DATE) AS de_date_last_available,
        SAFE_CAST(date AS DATE) AS price_date

    FROM source

)

SELECT * FROM renamed
