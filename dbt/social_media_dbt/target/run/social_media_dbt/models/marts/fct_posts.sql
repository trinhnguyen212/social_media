
  create view "social_media"."dbt_dev"."fct_posts__dbt_tmp"
    
    
  as (
    select *
from "social_media"."dbt_dev"."stg_posts"
  );