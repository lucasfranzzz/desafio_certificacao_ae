with base_sales_person as
(
    select
        sales_person_id
        , territory_id
        , sales_quota
        , bonus
        , commission_pct
        , sales_ytd
        , sales_last_year
    from {{ ref('stg_sales_person') }}
)

, base_employee as
(
    select
        employee_id
        , employee_job_title
        , employee_hire_date
        , employee_salaried_flag
        , employee_current_flag
        , employee_organization_node
    from {{ ref('stg_employee') }}
)

, base_person as
(
    select
        person_id
        , person_type
        , name
    from {{ ref('stg_person') }}
    where person_type = 'SP'
)

, joined as
(
    select
        base_sales_person.sales_person_id
        , base_person.name
        , base_employee.employee_job_title
        , base_employee.employee_hire_date
        , base_employee.employee_salaried_flag
        , base_employee.employee_current_flag
        , base_employee.employee_organization_node
        , base_sales_person.territory_id
        , base_sales_person.sales_quota
        , base_sales_person.bonus
        , base_sales_person.commission_pct
        , base_sales_person.sales_ytd
        , base_sales_person.sales_last_year
    from base_sales_person
    left join base_employee
        on base_sales_person.sales_person_id = base_employee.employee_id
    left join base_person
        on base_sales_person.sales_person_id = base_person.person_id
)

select * from joined