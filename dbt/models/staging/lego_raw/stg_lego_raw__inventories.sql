WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'inventories') }}

),

renamed AS (

    SELECT
        id AS inventory_id,
        version AS inventory_version,
        set_num,
        inserted_at

    FROM source

)

SELECT * FROM renamed
