{{ config(
    alias="refugee_ll"
) }}

with deals as (
Select
    pipeline_id,
    id,
    title,
    person_name,
    email.value as email,
    city,
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

        case when te_pets_cleaned in ('%no %', '%without','None%') then 0
             else 1 end as te_has_pets,

        case when te_disabled_help_needed_people_cleaned in ('%no %', '%without','None%') then 0
             else 1 end as te_has_disability,

    te_priority,
    if(status ='open',1,0) as open_deal,
    if(status ='lost',1,0) as lost_deal,
    if(status ='won',1,0) as won_deal ,
    _ll_beds_available_in_total_,
    ll_slots_available_edit_team_1,
    _ll_rent_per_person_,
    _ll_maximum_rent_duration_possible_,
    te_number_of_people_in_the_group_keep_updated,
    te_number_of_children_under_18_
    from {{ ref('stg_pipedrive_refugee__refugee_deals')}} a
left join unnest(a.person_id.email) as email
left join unnest (a.ll_cities_cleaned) as city
with offset
where true
and email.primary = true
) , pipedrive as (
Select *
from deals
where true
and pipeline_id = 2
and lost_reason not in ('Flawed/Fake Data Entry','Duplicate Data')
), product_data as (
Select ll.landlord_id,
ll.freshworks_email,
lower(ls.city)as prd_city,
ls.listing_id,
ls.is_bookable,
ls.is_online,
ls.is_published,
ls.is_deleted,
ls.created_at,
date(ls.available_from) as available_from,
date(ls.available_to) as available_to,
ls.beds,
ls.price,
ls.rooms,
ls.city,
ls.next_available_date,
ls.has_requests
from {{ ref('core_refugee__landlords' ) }} ll
right join {{ ref('core_refugee__listings')}} ls
on ll.landlord_id = ls.landlord_id
where true
),pipedrive_join_product as (
    Select
    'Pipedrive' as data_source,
    pd_data.email as email,
    case when pd_data.email is null and prd_data.freshworks_email is not null then true
    end as in_product,
    case when freshworks_email is null and pd_data.email is not null then true end as in_pipedrive,
    case when freshworks_email is not null and pd_data.email is not null then true end as both_sources,
    pd_data.city,
    cast(NULL as string) as prd_listing_id,
    cast(NULL as BOOL) as prd_is_bookable,
    cast(NULL as BOOL) as prd_is_online,
    cast(NULL as BOOL) as prd_is_published,
    cast(NULL as BOOL) as prd_is_deleted,
    pd_data._ll_beds_available_in_total_  as total_beds,
    pd_data.ll_slots_available_edit_team_1  as pd_total_beds_edit_team1,
    pd_data.ll_pets_allowed_bool as pd_pets_allowed,
    pd_data.ll_max_rent_duration as pd_max_rent_duration,
    pd_data.ll_rent_per_person as pd_rent_per_person,
    pd_data.nights_from_won_date as pd_nights_from_won_date,
    pd_data.open_deal as pd_open_deal,
    pd_data.won_deal as pd_won_deal,
    pd_data.lost_deal as pd_lost_deal
    from pipedrive pd_data
    left join product_data prd_data
    on pd_data.email = prd_data.freshworks_email
), product_join_pipedrive as (
    Select
    'Product' as data_source,
    prd_data.freshworks_email as email,
    case when pd_data.email is null and prd_data.freshworks_email is not null then true
    end as in_product,
    case when freshworks_email is null and pd_data.email is not null then true end as in_pipedrive,
    case when pd_data.email is not null and prd_data.freshworks_email is not null then true
    end as both_sources,
    prd_data.prd_city as city,
    prd_data.listing_id as prd_listing_id,
    prd_data.is_bookable as prd_is_bookable,
    prd_data.is_online as prd_is_online,
    prd_data.is_published as prd_is_published,
    prd_data.is_deleted as prd_is_deleted,
    prd_data.beds as total_beds,
    cast(NULL as INT64) as pd_total_beds_edit_team1,
    cast(NULL as INT64) as pd_pets_allowed,
    cast(NULL as string) as pd_max_rent_duration,
    cast(prd_data.price as string) as pd_rent_per_person,
    cast(NULL as INT64) as pd_nights_from_won_date,
    cast(NULL as INT64)  as pd_opendeal,
   cast(NULL as INT64) as pd_won_dea_l,
    cast(NULL as INT64)  as pd_lost_deal
    from product_data prd_data
    left join pipedrive pd_data
    on prd_data.freshworks_email = pd_data.email
    )
Select *,
case when in_product is true then 'in_product_only'
               when in_pipedrive is true then 'in_pipedrive_only'
               when both_sources is true then 'both_sources'
            end as source_flag
from (
Select *
from pipedrive_join_product
UNION ALL
Select *
from product_join_pipedrive b
where b.both_sources is null
)
