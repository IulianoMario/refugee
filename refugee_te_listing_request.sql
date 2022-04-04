-- Unnested cities field to avoid repetition -> possibly cast the cleaned te preferred cities? --

with deals as (
Select
    pipeline_id,
    cast(id as string)as id, 
    title,
    person_name, 
    email.value as email,
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

        case when _ll_pets_possible_  in ('Nein','No') then False
            when _ll_pets_possible_ in ('Ja','Yes') then True
            else null end as ll_pets_allowed_bool,

        case when te_pets_cleaned in ('%no %', '%without','None%') then False 
             else True end as te_has_pets,
        
        case when te_disabled_help_needed_people_cleaned in ('%no %', '%without','None%') then 0 
             else 1 end as te_has_disability,

    te_priority as deal_priority,   
    if(status ='open',1,0) as open_deal, 
    if(status ='lost',1,0) as lost_deal, 
    if(status ='won',1,0) as won_deal ,
    _ll_beds_available_in_total_ as beds,
    ll_slots_available_edit_team_1,
    _ll_rent_per_person_,
    _ll_maximum_rent_duration_possible_,
    te_number_of_people_in_the_group_keep_updated,  
    te_number_of_children_under_18_, 
    string_agg(city) as listing_city
    from `data-warehouse-229515.staging.pipedrive_refugee_deals` a
left join unnest(a.person_id.email) as email
left join unnest (a.te_cities_cleaned) as city
with offset 
where true 
and pipeline_id = 1 
and email.primary = true
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39

) , pipedrive_te_raw as (
    Select * 
    from deals 
    where lost_reason not in ('Flawed/Fake Data Entry','Duplicate Data')
), product_data as (
Select 
    te_lr.listing_request_id,
    te_lr.listing_id, 
    te_lr.tenant_id,
    users.email as te_email, 
    date(te_lr.created_at) as lr_created_at,
    te_lr.requested_from,
    te_lr.requested_to,
    date_diff(te_lr.requested_to,te_lr.requested_from, day) as nights_per_rent_duration,
    te_lr.listing_city as city,
    ls.is_bookable, 
    ls.is_online, 
    ls.is_deleted,
    date(ls.available_from) as available_from, 
    date(ls.available_to) as available_to, 
    date_diff(current_date(), date(te_lr.created_at), day) as prd_nights_from_won_date,
    if(date_diff(current_date(),date(te_lr.requested_from), day) < 0, 0 ,date_diff(current_date(),date(te_lr.requested_from), day)) as nights_spent,
    ls.beds,
    te_lr.listing_price,
    ls.rooms, 
    ls.accommodates,
    ls.area, 
    te_lr.deal_status,
    te_lr.status_reason,
    te_lr.adults,
    te_lr.children,
    te_lr.number_of_tenants,
    te_lr.tenant_has_pets,
    te_lr.pets_allowed,
    cast(NULL as String) as te_has_disability
from `data-warehouse-229515.wunderflats_core_refugee.listing_requests` te_lr 
left join `data-warehouse-229515.dbt_staging.stg_backend__users` users
on te_lr.tenant_id = users.user_id
left join `data-warehouse-229515.wunderflats_core_refugee.tenants` te
on te.tenant_id = te_lr.tenant_id
left join `data-warehouse-229515.wunderflats_core_refugee.listings` ls 
on ls.listing_id = te_lr.listing_id
), full_join_tab as (
    Select 
    case when pd.id is not null and prd.listing_request_id is not null then 'both_sources'
             when pd.id is not null and prd.listing_request_id is null then 'in_pipedrive_only'
             when pd.id is null and prd.listing_request_id is not null then 'in_product_only'
        end as source_flag,
    pd.id as pd_id, 
    prd.listing_request_id as prd_listing_request_id,
    case when pd.id is not null and prd.listing_request_id is null then pd.id 
         when pd.id is null and prd.listing_request_id is not null then prd.listing_request_id
         when pd.id is not null and prd.listing_request_id is not null then prd.listing_request_id
         end as listing_requests_ids_unified,
    
    pd.email as pd_email,
    prd.te_email as prd_email,
    case when pd.email is not null and prd.te_email is null then pd.email 
         when pd.email is null and prd.te_email is not null then prd.te_email
         when pd.email is not null and prd.te_email is not null then prd.te_email
         end as email,
    
    prd.tenant_id as prd_tenant_id,
    case when pd.id is null and prd.listing_request_id is not null then prd.tenant_id else prd.tenant_id end as tenant_id,
    pd.won_time as pd_created_at, 
    prd.lr_created_at as prd_created_at,
    case when date(pd.won_time) is not null and prd.lr_created_at is null then date(pd.won_time)
         when date(pd.won_time) is null and prd.lr_created_at is not null then prd.lr_created_at
         when date(pd.won_time) is not null and prd.lr_created_at is not null then prd.lr_created_at
         end as created_at,
prd.city as prd_city, 
pd.listing_city as pd_preferred_cities,
case when pd.listing_city is null and prd.city is not null then prd.city
     when pd.listing_city is not null and prd.city is not null then prd.city
     when pd.listing_city is not null and prd.city is null then pd.listing_city 
     end as city, 


    case when pd.id is not null and prd.listing_request_id is not null then prd.requested_from else prd.requested_from end as requested_from, 
    case when pd.id is not null and prd.listing_request_id is not null then prd.requested_to else prd.requested_to end as requested_to,    

    case when pd.max_nights_per_rent_duration is null then prd.nights_per_rent_duration
         when pd.max_nights_per_rent_duration is not null then pd.max_nights_per_rent_duration end max_nights_per_rent_duration,

    case when pd.min_nights_per_rent_duration is null then prd.nights_per_rent_duration
         when pd.min_nights_per_rent_duration is not null then pd.min_nights_per_rent_duration end min_nights_per_rent_duration,

    case when pd.nights_per_rent_duration is null then prd.nights_per_rent_duration
         when pd.nights_per_rent_duration is not null then pd.nights_per_rent_duration end nights_per_rent_duration,
        
    case when pd.nights_per_rent_duration is null then prd.nights_per_rent_duration end as prd_nights_rent_duration,
        
    ll_max_rent_duration as pd_max_rent_duration,
    prd.nights_per_rent_duration as prd_nights_per_rent_duration,
    case when ll_max_rent_duration is null and prd.nights_per_rent_duration < 30 then "<1 mo"
         when ll_max_rent_duration is null and prd.nights_per_rent_duration = 30 then "1 mo"
         when ll_max_rent_duration is null and (prd.nights_per_rent_duration >30 and prd.nights_per_rent_duration < 90 ) then "1-3 mo"
         when ll_max_rent_duration is null and (prd.nights_per_rent_duration > 90 and prd.nights_per_rent_duration < 180) then "3-6 mo"
         when ll_max_rent_duration is null and prd.nights_per_rent_duration >= 180 then ">6 mo"
         when ll_max_rent_duration is not null then ll_max_rent_duration end as max_rent_duration_categorical,

    pd.status as pd_status, 
    prd.deal_status as prd_status,
    lower(case when pd.status is not null and prd.deal_status is null then pd.status 
         when pd.status is null and prd.deal_status is not null then prd.deal_status
         when pd.status is not null and prd.deal_status is not null then prd.deal_status
         end) as deal_status,
    
    prd.status_reason as pd_status_reason,
    lower(pd.lost_reason) as prd_lost_reason, 
    case when pd.lost_reason is not null and prd.status_reason is null then pd.lost_reason 
         when pd.lost_reason is null and prd.status_reason is not null then prd.status_reason
         when pd.lost_reason is not null and prd.status_reason is not null then prd.status_reason
         end as status_reason,
    
    pd.nights_from_won_date as pd_nights_spent, 
    prd.nights_spent as prd_nights_spent,
    case when pd.nights_from_won_date is not null and prd.nights_spent is not null then prd.nights_spent
         when pd.nights_from_won_date is null and prd.nights_spent is not null then prd.nights_spent 
         when pd.nights_from_won_date is not null and prd.nights_spent is null then pd.nights_from_won_date
         end as nights_spent,


    case when pd.ll_slots_available_edit_team_1 is not null and prd.beds is not null then prd.beds  
            when pd.ll_slots_available_edit_team_1 is null and prd.beds  is not null then prd.beds  
            when pd.ll_slots_available_edit_team_1 is not null and prd.beds  is null then pd.ll_slots_available_edit_team_1 
            end as pd_beds_edited_team1,
    
    case when pd.beds is not null and prd.beds is not null then prd.beds 
            when pd.beds is null and prd.beds is not null then prd.beds 
            when pd.beds is not null and prd.beds is null then pd.beds 
            end as beds,
pd.ll_rent_per_person_numeric as pd_ll_rent_per_person_numeric ,
prd.listing_price as prd_listing_price,
    case when pd.ll_rent_per_person_numeric is not null and prd.listing_price is not null then prd.listing_price 
             when pd.ll_rent_per_person_numeric is null and prd.listing_price is not null then prd.listing_price 
             when pd.ll_rent_per_person_numeric is not null and prd.listing_price is null then pd.ll_rent_per_person_numeric 
             end as price,

        pd.ll_rent_per_person as pd_ll_rent_per_person,
        case when prd.listing_price = 0 then 'for_free'
             when prd.listing_price < 250 then '<250 mo' 
             when prd.listing_price >=250 and prd.listing_price < 500 then '250-500 mo' 
             when prd.listing_price >=500 then '>500 mo'
             else null end as prd_rent_per_person_categorical,

    case when pd.email is not null and prd.te_email is not null then prd.rooms
             when pd.email is null and prd.te_email is not null then prd.rooms 
             end as rooms, 

prd.accommodates as prd_accommodates,
case when pd.id is null and prd.listing_request_id is not null then prd.accommodates else prd.accommodates end as accommodates,
case when cast(pd.ll_pets_allowed_bool as bool) is not null and cast(prd.pets_allowed as bool) is not null then cast(prd.pets_allowed as bool)
            when cast(pd.ll_pets_allowed_bool as bool) is null and cast(prd.pets_allowed as bool) is not null then cast(prd.pets_allowed as bool) 
            when cast(pd.ll_pets_allowed_bool as bool) is not null and cast(prd.pets_allowed as bool) is null then pd.ll_pets_allowed_bool 
            end as pets_allowed,
    
        case when pd.id is not null or pd.id is null then prd.is_bookable end as is_bookable, 
        case when pd.id is not null or pd.id is null then prd.is_online end as is_online, 
        case when pd.id is not null or pd.id is null then prd.is_deleted end as is_deleted, 
   
    pd.deal_priority, 
    pd.te_has_pets as pd_te_has_pets,
    cast(prd.tenant_has_pets as bool) as prd_te_has_pets,
    case when pd.te_has_pets is null and cast(prd.tenant_has_pets as bool) is not null then cast(prd.tenant_has_pets as bool)
         when pd.te_has_pets is not null and cast(prd.tenant_has_pets as bool) is not null then cast(prd.tenant_has_pets as bool)
         when pd.te_has_pets is not null and cast(prd.tenant_has_pets as bool) is null then pd.te_has_pets
    end  as te_has_pets, 

    pd.te_number_of_people_in_the_group_keep_updated as pd_number_of_tenants,
    prd.number_of_tenants as prd_number_of_tenants, 
    case when pd.id is null and prd.listing_request_id is not null then prd.number_of_tenants
         when pd.id is not null and prd.listing_request_id is not null then prd.number_of_tenants 
         when pd.id is not null and prd.listing_request_id is null then pd.te_number_of_people_in_the_group_keep_updated
         end as number_of_tenants,
prd.adults as prd_adults, 
case when pd.id is null and prd.listing_request_id is not null then prd.adults else prd.adults end as adults,

prd.children as prd_children,
pd.te_number_of_children_under_18_ as pd_children,
case when pd.id is null and prd.listing_request_id is not null then prd.children
         when pd.id is not null and prd.listing_request_id is not null then prd.children 
         when pd.id is not null and prd.listing_request_id is null then pd.te_number_of_children_under_18_
         end as children,
pd.te_has_disability as pd_te_has_disability,
case when pd.id is not null and prd.listing_request_id is null then pd.te_has_disability else pd.te_has_disability end as te_has_disability,
pd.open_deal as pd_open_deal, 
pd.lost_deal as pd_lost_deal, 
pd.won_deal as pd_won_deal
    from product_data prd   
    full join pipedrive_te_raw pd 
    on prd.te_email = pd.email 
), final as (
    Select 
    source_flag,
    pd_id,
    prd_listing_request_id,
    listing_requests_ids_unified,
    pd_email,
    prd_email,
    email,
    prd_tenant_id,
    tenant_id,
    pd_created_at,
    prd_created_at,
    created_at,
    prd_city,
    pd_preferred_cities,
    city,
    requested_from,
    requested_to,
    max_nights_per_rent_duration,
    min_nights_per_rent_duration,
    nights_per_rent_duration,
    prd_nights_rent_duration,
    pd_max_rent_duration,
    prd_nights_per_rent_duration,
    max_rent_duration_categorical,
    pd_status,
    prd_status,
    deal_status, 
    pd_status_reason, 
    prd_lost_reason,
    status_reason,
    pd_nights_spent,
    prd_nights_spent,
    nights_spent,
    pd_beds_edited_team1,
    beds,
    pd_ll_rent_per_person_numeric,
    prd_listing_price,
    price,
    pd_ll_rent_per_person,
    prd_rent_per_person_categorical,
    case when pd_id is not null and prd_listing_request_id is null then pd_ll_rent_per_person
         when pd_id is null and prd_listing_request_id  is not null then prd_rent_per_person_categorical
         when pd_id is not null and prd_listing_request_id  is not null then prd_rent_per_person_categorical
         end as rent_per_person_categorical,
     rooms,
     prd_accommodates,
     accommodates,
     pets_allowed,
     deal_priority,
     pd_te_has_pets,
     prd_te_has_pets,
     te_has_pets,
     pd_number_of_tenants,
     prd_number_of_tenants,
     number_of_tenants,
     prd_adults,
     adults,
     pd_children,
     prd_children,
     children,
     pd_te_has_disability,
     te_has_disability,
     pd_open_deal, 
     pd_lost_deal,
     pd_won_deal,
     is_bookable,
     is_online,
     is_deleted
    from full_join_tab
)
Select *
from final
