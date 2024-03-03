WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'colors') }}

),

renamed AS (

    SELECT
        id AS color_id,
        name AS color_name,
        rgb AS rgb_code,
        is_trans,
        inserted_at

    FROM source

)

SELECT * FROM renamed
