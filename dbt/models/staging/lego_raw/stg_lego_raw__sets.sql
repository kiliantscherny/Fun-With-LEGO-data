WITH

source AS (

    SELECT * FROM {{ source('lego_raw', 'sets') }}

),

renamed AS (

    SELECT
        set_num,
        name,
        year,
        theme_id,
        num_parts,
        img_url,
        inserted_at

    FROM source

)

SELECT * FROM renamed
