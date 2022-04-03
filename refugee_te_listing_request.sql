-- Unnested cities field to avoid repetition -> possibly cast the cleaned te preferred cities? --

with deals as (
Select
    pipeline_id,
    cast(id as string)as id, 
    title,
    person_name, 
    email.value as email,
    city as listing_city,
    te_cities_cleaned as preferred_cities,
    owner_name,
    cc_email,
    date(add_time) as add_time,
    update_time,
    stage_change_time,
    status,
    close_time, 
    date(won_time) as won_time,
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

        case when _ll_rent_per_person_ in ('Kostenlos','I would rent it for free') then 0
            when _ll_rent_per_person_ in ('über 500€ pro Monat','More than 500 Euros/month') then 1000
            when _ll_rent_per_person_ in ("Ich bin mir nicht sicher","I'm unsure") then null
            when _ll_rent_per_person_ in ("Under 250 Euros/month","Unter 250€ pro Monat") then 250 
            when _ll_rent_per_person_ in ("250 - 500€ pro Monat","Between 250 - 500 Euros/month") then 500
            else 0 end as ll_rent_per_person_numeric,

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

    te_priority as deal_priority,   
    if(status ='open',1,0) as open_deal, 
    if(status ='lost',1,0) as lost_deal, 
    if(status ='won',1,0) as won_deal ,
    _ll_beds_available_in_total_,
    ll_slots_available_edit_team_1,
    _ll_rent_per_person_,
    _ll_maximum_rent_duration_possible_,
    te_number_of_people_in_the_group_keep_updated, 
    te_number_of_children_under_18_
    from `data-warehouse-229515.staging.pipedrive_refugee_deals` a
left join unnest(a.person_id.email) as email
left join unnest (a.ll_cities_cleaned) as city
with offset 
where true 
and pipeline_id = 1 
and email.primary = true

) , pipedrive_te_raw as (
    Select * 
    from deals 
    where lost_reason not in ('Flawed/Fake Data Entry','Duplicate Data')
), product_data as (
Select 
    te_lr.listing_request_id,
    te_lr.listing_id, 
    te.tenant_id, 
    users.email, 
    date(te_lr.created_at) as created_at, 
    te_lr.requested_from,
    te_lr.requested_to,
    date_diff(te_lr.requested_to,te_lr.requested_from, day) as prd_nights_rent_duration,
    --date_diff(current_date(),date(te_lr.requested_from), day) as nights_spent,
    date_diff(current_date(), date(te_lr.created_at), day) as prd_nights_from_won_date,
    if(date_diff(current_date(),date(te_lr.requested_from), day) < 0, 0 ,date_diff(current_date(),date(te_lr.requested_from), day)) as nights_spent,
    te_lr.listing_city,
    te_lr.listing_price,
    ls.beds,
    ls.area, 
    ls.rooms, 
    ls.accommodates,
    te_lr.deal_status,
    te_lr.status_reason,
    te_lr.adults,
    te_lr.children,
    te_lr.number_of_tenants,
    te_lr.tenant_has_pets,
    te_lr.pets_allowed,
    cast(NULL as String) as te_has_disability,
    ls.is_bookable, 
    ls.is_online, 
    ls.is_deleted
from `data-warehouse-229515.wunderflats_core_refugee.listing_requests` te_lr 
left join `data-warehouse-229515.dbt_staging.stg_backend__users` users
on te_lr.tenant_id = users.user_id
left join `data-warehouse-229515.wunderflats_core_refugee.tenants` te
on te.tenant_id = te_lr.tenant_id
left join `data-warehouse-229515.wunderflats_core_refugee.listings` ls 
on ls.listing_id = te_lr.listing_id
), full_join_tab as (
    Select 
    pd.id, 
    prd.listing_request_id,
    case when pd.id is not null and prd.listing_request_id is null then pd.id 
         when pd.id is null and prd.listing_request_id is not null then prd.listing_request_id
         when pd.id is not null and prd.listing_request_id is not null then prd.listing_request_id
         end as listing_requests_ids_unified,
    case when pd.email is not null and prd.email is null then pd.email 
         when pd.email is null and prd.email is not null then prd.email
         when pd.email is not null and prd.email is not null then prd.email
         end as email,
    case when pd.id is not null and prd.listing_request_id is null then pd.preferred_cities 
         else preferred_cities
         end as preferred_cities,
    -- pd.preferred_cities,
    case when pd.listing_city is not null and prd.listing_city is null then pd.listing_city 
         when pd.listing_city is null and prd.listing_city is not null then prd.listing_city
         when pd.listing_city is not null and prd.listing_city is not null then prd.listing_city
         end as listing_city,
    -- pd.listing_city,  
    -- prd.listing_city, 
    case when date(pd.won_time) is not null and prd.created_at is null then date(pd.won_time)
         when date(pd.won_time) is null and prd.created_at is not null then prd.created_at
         when date(pd.won_time) is not null and prd.created_at is not null then prd.created_at
         end as created_at,
    -- pd.add_time, 
    -- prd.created_at, 
    pd.nights_per_rent_duration, 
    pd.max_nights_per_rent_duration, 
    pd.min_nights_per_rent_duration,
    case when ll_max_rent_duration is null and prd_nights_rent_duration < 30 then "<1 mo"
             when ll_max_rent_duration is null and prd_nights_rent_duration = 30 then "1 mo"
             when ll_max_rent_duration is null and (prd_nights_rent_duration >30 and prd_nights_rent_duration < 90 ) then "1-3 mo"
             when ll_max_rent_duration is null and (prd_nights_rent_duration > 90 and prd_nights_rent_duration < 180) then "3-6 mo"
             when ll_max_rent_duration is null and prd_nights_rent_duration >= 180 then ">6 mo"
             when ll_max_rent_duration is not null then ll_max_rent_duration end as pd_max_rent_duration,
    case when pd.ll_max_rent_duration is not null then pd.ll_max_rent_duration end as ll_max_rent_duration,
    pd.ll_max_rent_duration, 
    case when prd_nights_rent_duration is null then prd_nights_rent_duration else prd_nights_rent_duration end as prd_nights_rent_duration,
--    prd_nights_rent_duration, 
    case when prd.requested_from is null then prd.requested_from else prd.requested_from end as requested_from,
    -- prd.requested_from, 
    case when prd.requested_to is null then prd.requested_to else prd.requested_to end as requested_to,
    -- prd.requested_to, 
    lower(case when pd.status is not null and prd.deal_status is null then pd.status 
         when pd.status is null and prd.deal_status is not null then prd.deal_status
         when pd.status is not null and prd.deal_status is not null then prd.deal_status
         end) as deal_status,
    -- pd.status, 
    -- prd.deal_status,
     case when max_nights_per_rent_duration is null then prd_nights_rent_duration
             when max_nights_per_rent_duration is not null then max_nights_per_rent_duration end max_nights_per_rent_duration,
        case when min_nights_per_rent_duration is null then prd_nights_rent_duration
             when min_nights_per_rent_duration is not null then min_nights_per_rent_duration end min_nights_per_rent_duration,
        case when nights_per_rent_duration is null then prd_nights_rent_duration
             when nights_per_rent_duration is not null then nights_per_rent_duration end nights_per_rent_duration,
        case when nights_per_rent_duration is null then prd_nights_rent_duration end as prd_nights_rent_duration,
    case when pd.won_time is null then pd.won_time else pd.won_time end as won_time,
    -- pd.won_time, 
    case when pd.lost_reason is not null and prd.status_reason is null then pd.lost_reason 
         when pd.lost_reason is null and prd.status_reason is not null then prd.status_reason
         when pd.lost_reason is not null and prd.status_reason is not null then prd.status_reason
         end as status_reason,
    -- prd.status_reason,
    pd.lost_reason, 
    pd.nights_from_won_date, 
    pd.ll_rent_per_person_numeric as pd_price, 
    prd.listing_price, 
    pd._ll_beds_available_in_total_ as pd_beds, 
    pd.ll_slots_available_edit_team_1 as pd_beds_edited_team1, 
    prd.rooms, 
    prd.accommodates, 
    prd.area,
    pd.deal_priority, 
    pd.ll_pets_allowed_bool as pd_pets_allowed, 
    prd.pets_allowed,
    pd.te_has_pets as pd_te_has_pets, 
    prd.tenant_has_pets , 
    pd.te_number_of_people_in_the_group_keep_updated as number_of_tenants,
    prd.number_of_tenants, 
    prd.adults, 
    prd.children,
    pd.te_number_of_children_under_18_ as pd_children,
    pd.te_has_disability,
    pd.open_deal, 
    pd.lost_deal, 
    pd.won_deal, 
    case when pd.id is not null and prd.listing_request_id is not null then 'both_sources'
         when pd.id is null and prd.listing_request_id is not null then 'in_product_only'
         when pd.id is not null and prd.listing_request_id is null then 'in_pipedrive_only'
         end as source_flag  
    from product_data prd 
    full join pipedrive_te_raw pd 
    on prd.email = pd.email 
)
Select *
from full_join_tab
