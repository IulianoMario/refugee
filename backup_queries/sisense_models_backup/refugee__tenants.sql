{{ config(
    alias="tenants"
) }}

with deals as (
Select
    pipeline_id,
    id,
    title,
    person_name,
    email.value as email,
    owner_name,
    cc_email,
    add_time,
    update_time,
    stage_change_time,
    status,
    close_time,
    won_time,
    first_won_time,
    lost_time,
    coalesce(lost_reason,"undefined") as lost_reason,
    date_diff(current_date(), date(won_time),day) as nights_from_won_date,
    case
            when _ll_maximum_rent_duration_possible_ in ('Less than one month','Unter einem Monat') then '<1 mo'
            when _ll_maximum_rent_duration_possible_ in ('One month','Einen Monat') then '1 mo'
            when _ll_maximum_rent_duration_possible_ in ('One to three months','Einen bis drei Monate') then '1-3 mo'
            when _ll_maximum_rent_duration_possible_ in ('Three to six months','Drei bis sechs Monate') then '3-6 mo'
            when _ll_maximum_rent_duration_possible_ in ('Longer than six months','Länger als sechs Monate') then '>6 mo'
            else 'undefined' end  as ll_max_rent_duration,

        case when _ll_rent_per_person_ in ('Kostenlos','I would rent it for free') then 'for_free'
            when _ll_rent_per_person_ in ('über 500€ pro Monat','More than 500 Euros/month') then '>500 mo'
            when _ll_rent_per_person_ in ("Ich bin mir nicht sicher","I'm unsure") then 'unsure'
            when _ll_rent_per_person_ in ("Under 250 Euros/month","Unter 250€ pro Monat") then '<250 mo'
            when _ll_rent_per_person_ in ("250 - 500€ pro Monat","Between 250 - 500 Euros/month") then '250-500 mo'
            else 'undefined' end as ll_rent_per_person,

          case when _ll_pets_possible_ in ('Vielleicht','Maybe') then 'maybe'
            when _ll_pets_possible_  in ('Nein','No') then 'no'
            when _ll_pets_possible_ in ('Ja','Yes') then 'yes'
            else 'undefined' end as ll_pets_allowed,

    te_move_in_date,
    te_priority,

        case when _ll_maximum_rent_duration_possible_ in ('Less than one month','Unter einem Monat') then 15
            when _ll_maximum_rent_duration_possible_ in ('One month','Einen Monat') then 30
            when _ll_maximum_rent_duration_possible_ in ('One to three months','Einen bis drei Monate') then 60
            when _ll_maximum_rent_duration_possible_ in ('Three to six months','Drei bis sechs Monate') then 120
            when _ll_maximum_rent_duration_possible_ in ('Longer than six months','Länger als sechs Monate') then 200
            else 0 end  as nights_per_rent_duration,

        case when _ll_maximum_rent_duration_possible_ in ('Less than one month','Unter einem Monat') then 1
            when _ll_maximum_rent_duration_possible_ in ('One month','Einen Monat') then 30
            when _ll_maximum_rent_duration_possible_ in ('One to three months','Einen bis drei Monate') then 31
            when _ll_maximum_rent_duration_possible_ in ('Three to six months','Drei bis sechs Monate') then 91
            when _ll_maximum_rent_duration_possible_ in ('Longer than six months','Länger als sechs Monate') then 181
            else 0 end  as min_nights_per_rent_duration,

       case when _ll_maximum_rent_duration_possible_ in ('Less than one month','Unter einem Monat') then 29
            when _ll_maximum_rent_duration_possible_ in ('One month','Einen Monat') then 30
            when _ll_maximum_rent_duration_possible_ in ('One to three months','Einen bis drei Monate') then 89
            when _ll_maximum_rent_duration_possible_ in ('Three to six months','Drei bis sechs Monate') then 179
            when _ll_maximum_rent_duration_possible_ in ('Longer than six months','Länger als sechs Monate') then 360
            else 0 end  as max_nights_per_rent_duration,

        case when _ll_pets_possible_  in ('Nein','No') then 0
            when _ll_pets_possible_ in ('Ja','Yes') then 1
            else 0 end as ll_pets_allowed_bool,

       CASE WHEN LOWER(te_pets_cleaned) LIKE '%no%' OR
           LOWER(te_pets_cleaned) LIKE '%without%' OR
           LOWER(te_pets_cleaned) LIKE '%don%' or
           LOWER(te_pets_cleaned) = ''
       THEN FALSE
       ELSE True END AS te_has_pets,
      CASE
      WHEN LOWER(te_disabled_help_needed_people_cleaned) LIKE '%no%'
      OR LOWER(te_disabled_help_needed_people_cleaned) LIKE '%without'
      OR LOWER(te_disabled_help_needed_people_cleaned) LIKE '%don%'
      OR LOWER(te_disabled_help_needed_people_cleaned) = ''
      THEN 0 ELSE 1 END AS te_has_disability,
    te_priority,
    if(status ='open',1,0) as open_deal,
    if(status ='lost',1,0) as lost_deal,
    if(status ='won',1,0) as won_deal ,
    _ll_beds_available_in_total_,
    ll_slots_available_edit_team_1,
    _ll_rent_per_person_,
    _ll_maximum_rent_duration_possible_,
    te_number_of_people_in_the_group_keep_updated,
    te_number_of_children_under_18_,
    string_agg(city) as city
    from{{ ref('stg_pipedrive_refugee__refugee_deals') }} a
left join unnest(a.person_id.email) as email
left join unnest (a.te_cities_cleaned) as city
with offset
where true
and email.primary = true
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38

) , pipedrive_te_raw as (
    Select *
    from deals
    where pipeline_id = 1
    and lost_reason not in ('Flawed/Fake Data Entry','Duplicate Data')

), product_data as (
Select users.email,
    te.tenant_id,
    te_lr.created_at,
    date_diff(te_lr.requested_to,te_lr.requested_from, day) as rent_duration,
    te_lr.listing_id,
    te_lr.listing_city,
    te_lr.listing_price,
    te_lr.deal_status,
    te_lr.adults,
    te_lr.children,
    te_lr.number_of_tenants,
    te_lr.tenant_has_pets,
    cast(NULL as String) as te_has_disability
from {{ ref('core_refugee__listing_requests') }} te_lr
left join {{ref('stg_backend__users')}} users
on te_lr.tenant_id = users.user_id
left join {{ ref('core_refugee__tenants') }} te
on te.tenant_id = te_lr.tenant_id
), pipedrive_data as(
    Select
    email,
    cast(null as string) as tenant_id,
    add_time as created_at,
    cast(NULL as INT64) as rent_duration,
    id,
    city,
    ll_rent_per_person as price,
    status,
    te_number_of_people_in_the_group_keep_updated as number_of_tenants,
    te_number_of_people_in_the_group_keep_updated - te_number_of_children_under_18_ as adults,
    te_number_of_children_under_18_,
    te_has_pets,
    te_has_disability
    from pipedrive_te_raw raw
), pipedrive_join_product as (
    Select 'Pipedrive' as data_surce,
    pd.email as email,
    case when pd.email is null and prd.email is not null then true end as in_product,
    case when pd.email is not null and prd.email is null then true end as in_pipedrive,
    case when pd.email is not null and prd.email is not null then true end as both_sources,
    cast(null as string) as tenant_id,
    cast(null as string) as listing_id,
    prd.created_at,
    cast(NULL as INT64) as rent_duration,
    id,
    city,
    pd.price,
    status as deal_status,
    pd.number_of_tenants,
    pd.adults,
    te_number_of_children_under_18_ as number_of_children,
    te_has_pets as te_has_pets,
    pd.te_has_disability
    from pipedrive_data pd
    left join product_data prd
    on pd.email = prd.email
), product_join_pipedrive as (
    Select 'Product' as data_source,
    prd.email,
    case when pd.email is null and prd.email is not null then true end as in_product,
    case when prd.email is null and pd.email is not null then true end as in_pipedrive,
    case when pd.email is not null and prd.email is not null then true end as both_sources,
    prd.tenant_id,
    prd.listing_id,
    prd.created_at,
    prd.rent_duration,
    cast(null as INT64)as id,
    prd.listing_city as city,
    cast(prd.listing_price as string) as price,
    prd.deal_status as deal_status,
    prd.number_of_tenants,
    prd.adults,
    prd.children as number_of_children,
    prd.tenant_has_pets as te_has_pets,
    cast(null as INT64) as te_has_disability
    from pipedrive_data pd
    right join product_data prd
    on pd.email = prd.email
), unioned as (
    Select *
    from pipedrive_join_product a
    UNION ALL
    Select *
    from product_join_pipedrive b
    where b.both_sources is null
)
Select * ,
case when in_product is true then 'in_product_only'
     when in_pipedrive is true then 'in_pipedrive_only'
     when both_sources is true then 'both_sources'
     end as source_flag
from unioned

