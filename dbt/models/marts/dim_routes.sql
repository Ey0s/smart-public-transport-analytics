select
  route_id,
  route_name
from {{ ref('stg_routes') }}

