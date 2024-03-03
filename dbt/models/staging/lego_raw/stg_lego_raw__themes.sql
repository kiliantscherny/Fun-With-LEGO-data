WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'themes') }}

),

renamed AS (

    SELECT
        id AS theme_id,
        name AS theme_name,
        parent_id,
        inserted_at

    FROM source

)

SELECT * FROM renamed
