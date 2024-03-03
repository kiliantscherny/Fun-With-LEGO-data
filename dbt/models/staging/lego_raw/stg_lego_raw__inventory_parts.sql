WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'inventory_parts') }}

),

renamed AS (

    SELECT
        inventory_id,
        part_num,
        color_id,
        quantity,
        is_spare,
        img_url,
        inserted_at

    FROM source

)

SELECT * FROM renamed
