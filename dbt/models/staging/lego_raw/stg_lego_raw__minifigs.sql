WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'minifigs') }}

),

renamed AS (

    SELECT
        fig_num,
        name AS fig_name,
        num_parts,
        img_url,
        inserted_at

    FROM source

)

SELECT * FROM renamed
