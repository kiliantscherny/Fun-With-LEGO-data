WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'themes') }}

),

renamed AS (

    SELECT
        id,
        name,
        parent_id,
        inserted_at

    FROM source

)

SELECT * FROM renamed
