WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'part_relationships') }}

),

renamed AS (

    SELECT
        rel_type,
        child_part_num,
        parent_part_num,
        inserted_at

    FROM source

)

SELECT * FROM renamed
