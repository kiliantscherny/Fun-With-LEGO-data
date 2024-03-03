WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'part_categories') }}

),

renamed AS (

    SELECT
        id AS part_category_id,
        name AS part_category_name,
        inserted_at

    FROM source

)

SELECT * FROM renamed
