WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'part_categories') }}

),

renamed AS (

    SELECT
        id,
        name,
        inserted_at

    FROM source

)

SELECT * FROM renamed
