{{ config(
    alias="refugee_ll_bookings"
) }}


with deals as (
Select
    pipeline_id,
    cast(id as string)as id, 
    title,
    person_name, 
    email.value as email,
    ll_cities as city,
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

    te_priority,   
    if(status ='open',1,0) as open_deal, 
    if(status ='lost',1,0) as lost_deal, 
    if(status ='won',1,0) as won_deal ,
    _ll_amount_rental_units_,
    _ll_beds_available_in_total_,
    ll_slots_available_edit_team_1,
    _ll_rent_per_person_,
    _ll_maximum_rent_duration_possible_,
    te_number_of_people_in_the_group_keep_updated, 
    te_number_of_children_under_18_
    from {{ ref('stg_pipedrive_refugee__refugee_deals') }} a
left join unnest(a.person_id.email) as email
with offset 
where true 
and email.primary = true
) , won_pipedrive_deals as (
Select *
from deals
where true 
and pipeline_id = 2
and lost_reason not in ('Flawed/Fake Data Entry','Duplicate Data')
and status = 'won'
) , bookings_core as (
Select
lower(core_ls.city)as prd_city,
rfg_bk.booking_id,
rfg_bk.booked_from, 
rfg_bk.booked_to,
date_diff(rfg_bk.booked_to,rfg_bk.booked_from,day) as prd_nights_rent_duration,
if(date_diff(current_date(),date(rfg_bk.booked_from), day) < 0, 0 ,date_diff(current_date(),date(rfg_bk.booked_from), day)) as nights_spent,
core_ls.listing_id, 
core_ls.is_bookable, 
core_ls.is_online, 
core_ls.is_published,
core_ls.is_deleted,
core_ls.created_at, 
date_diff(current_date(), date(core_ls.created_at), day) as prd_nights_from_won_date,
date(core_ls.available_from) as available_from, 
date(core_ls.available_to) as available_to,
core_ls.beds, 
core_ls.price, 
core_ls.rooms, 
core_ls.city,
core_ls.accommodates, 
core_ls.area,
core_ls.next_available_date,
core_ls.has_requests
from {{ ref('core_refugee__bookings') }} rfg_bk
left join {{ ref('core__listings') }}  core_ls
on rfg_bk.listing_id = core_ls.listing_id 
), rfg_listing_request as (
Select distinct listing_id, pets_allowed 
from {{ ref('core_refugee__listing_requests') }} 
), bookings_refugee as (
    Select rfg_bk.landlord_id as prd_landlord_id, 
    rfg_ll.freshworks_email as prd_email,
    rfg_bk.booking_id as bookingid, 
    case when  core_bk.listing_id is null then rfg_bk.listing_id  else core_bk.listing_id end as prd_listing_id, 
    case when core_bk.city is null then rfg_lis.city else core_bk.city end as prd_city, 
    rfg_bk.booking_id as prd_bookingid, 
    case when core_bk.is_bookable is null then rfg_lis.is_bookable else core_bk.is_bookable end as prd_is_bookable, 
    case when core_bk.is_online is null then rfg_lis.is_online else core_bk.is_online end as prd_is_online, 
    case when core_bk.is_published  is null then rfg_lis.is_published else core_bk.is_published end as prd_is_published, 
    case when core_bk.is_deleted  is null then rfg_lis.is_deleted else core_bk.is_deleted end as prd_is_deleted, 
    case when core_bk.created_at  is null then rfg_lis.created_at else core_bk.created_at end as prd_created_at, 
    case when core_bk.available_from  is null then date(rfg_lis.available_from) else core_bk.available_from end as prd_available_from,
    case when core_bk.available_to  is null then date(rfg_lis.available_to) else core_bk.available_to end as prd_available_to,
    date_diff(rfg_bk.booked_to,rfg_bk.booked_from,day) as prd_nights_rent_duration,
    rfg_bk.booked_from, 
    rfg_bk.booked_to,
    case when core_bk.nights_spent is null then date_diff(current_date(),date(rfg_bk.booked_from), day)
    else core_bk.nights_spent end as nights_spent,
    case when core_bk.beds  is null then rfg_lis.beds else core_bk.beds end as prd_beds,
    case when core_bk.price  is null then rfg_lis.price else core_bk.price end as prd_price,
    case when core_bk.rooms  is null then rfg_lis.rooms else core_bk.rooms end as prd_rooms,
    case when core_bk.city  is null then rfg_lis.city else core_bk.city end as prd_city_,
    case when core_bk.accommodates  is null then rfg_lis.accommodates else core_bk.accommodates end as prd_accommodates,
    case when core_bk.area  is null then rfg_lis.area else core_bk.area end as prd_area,
    case when core_bk.next_available_date  is null then rfg_lis.next_available_date else core_bk.next_available_date end as prd_next_available_date,
    case when core_bk.has_requests  is null then rfg_lis.has_requests else core_bk.has_requests end as prd_has_requests,
    rfg_lr.pets_allowed as prd_pets_allowed
    from bookings_core core_bk
    left join {{ ref('core_refugee__bookings') }} rfg_bk
    on rfg_bk.booking_id = core_bk.booking_id
    left join {{ ref('core_refugee__listings') }} rfg_lis
    on rfg_lis.listing_id = rfg_bk.listing_id
    left join rfg_listing_request rfg_lr
    on rfg_lis.listing_id = rfg_lr.listing_id
    left join {{ ref('core_refugee__landlords') }}  rfg_ll
    on rfg_ll.landlord_id = rfg_bk.landlord_id
) 
 , pipedrive_bookings as ( 
     Select email, 
     id, 
     city, 
     add_time, 
     won_time,
     nights_from_won_date,
     ll_max_rent_duration,
     _ll_beds_available_in_total_ as beds,
     ll_slots_available_edit_team_1 as beds_edited_team1, 
     _ll_amount_rental_units_ as total_rental_units,
     ll_rent_per_person as price_cat,
     ll_rent_per_person_numeric as price, 
     ll_pets_allowed, 
     nights_per_rent_duration, 
     min_nights_per_rent_duration, 
     max_nights_per_rent_duration
     from won_pipedrive_deals 
 ), final as (
Select pd.id,
prd.prd_bookingid, 
pd.email as pd_email, 
prd.prd_email as prd_email, 
prd.prd_landlord_id, 
pd.city as pd_city, 
prd.prd_city_ as prd_city, 
pd.add_time as pd_created_at, 
pd.won_time,
prd.booked_from, 
prd.booked_to,
pd.nights_from_won_date as pd_nights_spent, 
prd.nights_spent as prd_nights_spent,
prd_nights_rent_duration,
pd.ll_max_rent_duration,
prd.prd_created_at as prd_created_at, 
pd.total_rental_units,
pd.beds as pd_beds, 
prd.prd_beds as prd_beds, 
pd.beds_edited_team1, 
prd.prd_accommodates as prd_accommodates,
pd.price as pd_price, 
pd.price_cat as price_cat, 
prd.prd_price as prd_price, 
pd.ll_pets_allowed as pd_pets_allowed, 
prd.prd_pets_allowed as prd_pets_allowed, 
nights_per_rent_duration, 
min_nights_per_rent_duration, 
max_nights_per_rent_duration,
prd.prd_rooms, 
prd_is_bookable, 
prd_is_online,
case when pd.email is null and prd.prd_email is not null then 'in_product_only'
     when pd.email is not null and prd.prd_email is not null then 'both_sources'
     when pd.email is not null and prd.prd_email is null then 'in_pipedrive_only'
     end as source_flag
from pipedrive_bookings pd
full join bookings_refugee prd
on pd.email =prd.prd_email
  ) 
Select 
id as pd_id, 
prd_bookingid, 
case when id is not null and prd_bookingid is not null then prd_bookingid
     when id is null and prd_email is not null then prd_bookingid 
     when id is not null and prd_email is null then id
            end as distinct_ids, 

case when pd_email is not null and prd_email is not null then prd_email
            when pd_email is null and prd_email is not null then prd_email 
            when pd_email is not null and prd_email is null then pd_email
            end as email, 
        prd_landlord_id, 
        case when pd_city is not null and prd_city is not null then prd_city 
             when pd_city is null and prd_email is not null then prd_city 
             when pd_email is not null and prd_email is null then pd_city
        end as city, 
        case when  date(won_time) is not null and  date(prd_created_at) is not null then date(prd_created_at) 
             when  date(won_time) is null and  date(prd_created_at) is not null then date(prd_created_at) 
             when  date(won_time) is not null and  date(prd_created_at) is null then won_time
        end as created_at, 
        booked_from as prd_booked_from, 
        booked_to as prd_booked_to,
        case when max_nights_per_rent_duration is null then prd_nights_rent_duration
             when max_nights_per_rent_duration is not null then max_nights_per_rent_duration end max_nights_per_rent_duration,
        case when min_nights_per_rent_duration is null then prd_nights_rent_duration
             when min_nights_per_rent_duration is not null then min_nights_per_rent_duration end min_nights_per_rent_duration,
        case when nights_per_rent_duration is null then prd_nights_rent_duration
             when nights_per_rent_duration is not null then nights_per_rent_duration end nights_per_rent_duration,
        case when nights_per_rent_duration is null then prd_nights_rent_duration end as prd_nights_rent_duration,

        case when ll_max_rent_duration is null and prd_nights_rent_duration < 30 then "<1 mo"
             when ll_max_rent_duration is null and prd_nights_rent_duration = 30 then "1 mo"
             when ll_max_rent_duration is null and (prd_nights_rent_duration >30 and prd_nights_rent_duration < 90 ) then "1-3 mo"
             when ll_max_rent_duration is null and (prd_nights_rent_duration > 90 and prd_nights_rent_duration < 180) then "3-6 mo"
             when ll_max_rent_duration is null and prd_nights_rent_duration >= 180 then ">6 mo"
             when ll_max_rent_duration is not null then ll_max_rent_duration end as pd_max_rent_duration,
        ll_max_rent_duration as pd_max_rent_duration_raw,
        case when pd_nights_spent is not null and prd_nights_spent is not null then prd_nights_spent
             when pd_nights_spent is null and prd_nights_spent is not null then prd_nights_spent 
             when pd_nights_spent is not null and prd_nights_spent is null then pd_nights_spent
             end as nights_spent,
       case when beds_edited_team1 is not null and prd_beds is not null then prd_beds 
            when beds_edited_team1 is null and prd_beds is not null then prd_beds 
            when beds_edited_team1 is not null and prd_beds is null then beds_edited_team1 
            end as pd_beds_edited_team1,
       case when pd_beds is not null and prd_beds is not null then prd_beds 
            when pd_beds is null and prd_beds is not null then prd_beds 
            when pd_beds is not null and prd_beds is null then pd_beds 
            end as beds,
        case when pd_price is not null and prd_price is not null then prd_price 
            when pd_price is null and prd_price is not null then prd_price 
            when pd_price is not null and prd_price is null then pd_price 
            end as price,
        price_cat as pd_price_range,
        case when pd_pets_allowed is not null and prd_pets_allowed is not null then prd_pets_allowed 
            when pd_pets_allowed is null and prd_pets_allowed is not null then prd_pets_allowed 
            when pd_pets_allowed is not null and prd_pets_allowed is null then pd_pets_allowed 
            end as pets_allowed,
        total_rental_units as pd_rental_units,
        case when pd_email is not null and prd_email is not null then prd_rooms
             when pd_email is null and prd_email is not null then prd_rooms 
             end as prd_rooms, 
        case when source_flag = 'in_product_only' or source_flag ='both_sources' then prd_accommodates end as prd_accommodates,
        case when source_flag = 'in_product_only' or source_flag ='both_sources' then prd_is_bookable end as is_bookable,
        case when source_flag = 'in_product_only' or source_flag ='both_sources' then prd_is_online end as is_online,
        source_flag
from final   
