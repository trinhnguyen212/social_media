
    
    

select
    comment_id as unique_field,
    count(*) as n_records

from "social_media"."dbt_dev"."fct_comments"
where comment_id is not null
group by comment_id
having count(*) > 1


