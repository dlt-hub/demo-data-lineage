
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

select 
  invoice_id,
  city,
  unit_price,
  quantity,
  total,
  date,
  payment,
  info._dlt_id,
  info._dlt_load_id,
  loads.schema_name,
  loads.inserted_at
from {{source('store', 'sales_info')}} as info
left join {{source('store', '_dlt_loads')}} as loads 
on  info._dlt_load_id = loads.load_id

union all

select 
  name as invoice_id,
  billing_city,
  lineitem_price,
  lineitem_quantity,
  total,
  created_at,
  payment_method,
  info._dlt_id,
  info._dlt_load_id,
  loads.schema_name,
  loads.inserted_at
from {{source('shopify', 'sales_info')}} as info
left join {{source('shopify', '_dlt_loads')}} as loads
on  info._dlt_load_id = loads.load_id
where financial_status = 'paid'


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
