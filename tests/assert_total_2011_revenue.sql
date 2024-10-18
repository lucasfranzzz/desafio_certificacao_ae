--vendas brutas no ano de 2011 foram de $12646112.16
--implementado com tolerância de +-$0.01

select 
    sum(detail_value) as vendas_brutas_2011 
from {{ ref('fct_orders') }}
where year(order_date) = 2011
having 
vendas_brutas_2011 not between 12646112.15 and 12646112.17