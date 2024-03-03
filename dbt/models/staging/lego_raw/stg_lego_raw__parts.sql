WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'parts') }}

),

renamed AS (

    SELECT
        part_num,
        name AS part_name,
        part_cat_id,
        part_material,
        inserted_at

    FROM source

)

SELECT * FROM renamed
