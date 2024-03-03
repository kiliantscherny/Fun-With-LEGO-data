WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'elements') }}

),

renamed AS (

    SELECT
        element_id,
        part_num,
        color_id,
        design_id,
        inserted_at

    FROM source

)

SELECT * FROM renamed
