with source as 
(
    select
        cast(businessentityid as integer) as person_id
        , cast(persontype as varchar) as person_type
        , cast(title as varchar) as title
        , cast((ifnull(firstname,'') || ' ' || ifnull(middlename,'') || ' ' || ifnull(lastname,'') || ' ' || ifnull(suffix,'')) as varchar) as name
        , cast(emailpromotion as integer) as email_promotion
    from {{ ref('src_person') }} 
)

select * from source