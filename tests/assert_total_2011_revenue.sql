select 
    sum(detail_value) as vendas_brutas_2011 
from {{ ref('fct_orders') }}
where year(order_date) = 2011
having 
vendas_brutas_2011 not between 12646112.15 and 12646112.17