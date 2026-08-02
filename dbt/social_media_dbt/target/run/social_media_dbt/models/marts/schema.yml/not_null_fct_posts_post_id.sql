
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select post_id
from "social_media"."dbt_dev"."fct_posts"
where post_id is null



  
  
      
    ) dbt_internal_test