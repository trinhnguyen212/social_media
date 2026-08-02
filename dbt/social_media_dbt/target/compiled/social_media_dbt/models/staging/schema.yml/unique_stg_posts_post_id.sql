
    
    

select
    post_id as unique_field,
    count(*) as n_records

from "social_media"."dbt_dev"."stg_posts"
where post_id is not null
group by post_id
having count(*) > 1


