WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'inventory_sets') }}

),

renamed AS (

    SELECT
        inventory_id,
        set_num,
        quantity,
        inserted_at

    FROM source

)

SELECT * FROM renamed
