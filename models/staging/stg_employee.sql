with source as 
(
    select
        cast(businessentityid as integer) as employee_id
        , cast(nationalidnumber as integer) as employee_national_id_number
        , cast(jobtitle as varchar) as employee_job_title
        , cast(hiredate as date) as employee_hire_date
        , cast(salariedflag as boolean) as employee_salaried_flag
        , cast(currentflag as boolean) as employee_current_flag
        , cast(organizationnode as varchar) as employee_organization_node
    from {{ ref('src_employee') }}
)

select * from source