WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'colors') }}

),

renamed AS (

    SELECT
        id,
        name,
        rgb,
        is_trans,
        inserted_at

    FROM source

)

SELECT * FROM renamed
