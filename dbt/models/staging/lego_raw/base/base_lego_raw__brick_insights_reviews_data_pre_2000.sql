with 

source as (

    select * from {{ source('lego_raw', 'brick_insights_reviews_data_pre_2000') }}

),

renamed as (

    select
        set_num,
        review_url,
        snippet,
        review_amount,
        rating_original,
        rating_converted,
        author_name

    from source

)

select * from renamed
