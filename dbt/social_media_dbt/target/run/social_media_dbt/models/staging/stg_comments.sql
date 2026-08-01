
  create view "social_media"."dbt_dev"."stg_comments__dbt_tmp"
    
    
  as (
    select *
from "social_media"."public"."comments"
  );