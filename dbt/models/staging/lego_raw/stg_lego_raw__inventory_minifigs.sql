WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'inventory_minifigs') }}

),

renamed AS (

    SELECT
        inventory_id,
        fig_num,
        quantity,
        inserted_at

    FROM source

)

SELECT * FROM renamed
