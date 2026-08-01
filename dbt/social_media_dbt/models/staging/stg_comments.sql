select *
from {{ source('raw_social_media', 'comments') }}
