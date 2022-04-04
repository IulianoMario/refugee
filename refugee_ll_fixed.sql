with deals as (
Select
    pipeline_id,
    CAST(id AS string)AS id,
    title,
    person_name,
    email.value AS email,
    ll_cities AS city,
    deleted,
    owner_name,
    cc_email,
    DATE(add_time) AS add_time,
    update_time,
    stage_change_time,
    status,
    close_time,
    DATE(won_time) AS won_time,
    first_won_time,
    lost_time,
    coalesce(lost_reason,"undefined") AS lost_reason,
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
            when _ll_rent_per_person_ in ("Ich bin mir nicht sicher","I'm unsure") then null
            when _ll_rent_per_person_ in ("Under 250 Euros/month","Unter 250€ pro Monat") then '<250 mo'
            when _ll_rent_per_person_ in ("250 - 500€ pro Monat","Between 250 - 500 Euros/month") then '250-500 mo'
            else 'undefined' end as ll_rent_per_person,

        CASE
            WHEN _ll_rent_per_person_ IN ('Kostenlos', 'I would rent it for free') THEN 0
            WHEN _ll_rent_per_person_ IN ('über 500€ pro Monat','More than 500 Euros/month') THEN 1000
            WHEN _ll_rent_per_person_ IN ("Ich bin mir nicht sicher", "I'm unsure") THEN NULL
            WHEN _ll_rent_per_person_ IN ("Under 250 Euros/month","Unter 250€ pro Monat") THEN 250
            WHEN _ll_rent_per_person_ IN ("250 - 500€ pro Monat", "Between 250 - 500 Euros/month") THEN 500
            ELSE 0 END AS ll_rent_per_person_numeric,

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
    from `data-warehouse-229515.staging.pipedrive_refugee_deals` a
left join unnest(a.person_id.email) as email
left join unnest (a.ll_cities_cleaned) as city
with offset
where true
and pipeline_id = 2
and email.primary = true
) , pipedrive as (
    Select id, 
    email, 
    add_time as created_at, 
    _ll_beds_available_in_total_ as beds,
    ll_slots_available_edit_team_1 as beds_edited_team1,
    ll_rent_per_person as rent_per_person_categorical,
    ll_rent_per_person_numeric as price,
    city, 
    ll_max_rent_duration, 
    nights_per_rent_duration,
    min_nights_per_rent_duration,
    max_nights_per_rent_duration,
    nights_from_won_date,
    nights_from_won_date as nights_spent,
    ll_pets_allowed_bool as pets_allowed, 
    open_deal, 
    lost_deal,
    won_deal,
    case when won_deal = 1 then True else False end as ls_is_occupied,
    case when open_deal = 1 or lost_deal =1 then True else False end as ls_has_requests
    from deals
    where true
    and lost_reason not in ('Flawed/Fake Data Entry','Duplicate Data')
), 
    product_data as (
    Select ll.landlord_id,
    ll.freshworks_email,
    lower(ls.city)as prd_city,
    ls.listing_id,
    ls.created_at,
    date(ls.available_from) as available_from,
    date(ls.available_to) as available_to,
    ls.beds,
    ls.price,
    case when ls.price = 0 then 'for_free'
         when ls.price < 250 then '<250 mo' 
         when ls.price >=250 and ls.price < 500 then '250-500 mo' 
         when ls.price >=500 then '>500 mo'
         else null end as prd_rent_per_person_categorical,
    ls.rooms,
    ls.accommodates,
    ls.area,
    ls.city,    
    ls.next_available_date,
    ls.has_requests, 
    ls.is_bookable,
    ls.is_online,
    ls.is_published,
    ls.is_deleted
    from `data-warehouse-229515.wunderflats_core_refugee.listings` ls 
    left join `data-warehouse-229515.wunderflats_core_refugee.landlords` ll 
    on ll.landlord_id = ls.landlord_id
    where true
), 
    listing_requests as (
        Select distinct listing_id, 
        pets_allowed
        from `data-warehouse-229515.wunderflats_core_refugee.listing_requests` lr 
), 
    product_data_final as (
        Select prd.*, 
        pets_allowed 
        from product_data prd 
        left join  listing_requests lr 
        on prd.listing_id= lr.listing_id
    ), full_join_tab as (
        Select 
        case when pd.id is not null and prd.listing_id is not null then 'both_sources'
             when pd.id is not null and prd.listing_id is null then 'in_pipedrive_only'
             when pd.id is null and prd.listing_id is not null then 'in_product_only'
        end as source_flag,
        case when pd.id is not null and prd.listing_id is not null then prd.listing_id
             when pd.id is null and prd.listing_id is not null then prd.listing_id 
             when pd.id is not null and prd.listing_id is null then pd.id
             end as distinct_ls_id, 
        pd.id as pd_ls_id, 
        prd.listing_id as prd_ls_id,
        case when pd.email is not null and prd.freshworks_email  is not null then prd.freshworks_email 
             when pd.email is null and prd.freshworks_email  is not null then prd.freshworks_email  
             when pd.email is not null and prd.freshworks_email  is null then pd.email
             end as email, 
        pd.email as pd_email, 
        prd.freshworks_email as prd_email,
       case when  pd.created_at is not null and  date(prd.created_at) is not null then date(prd.created_at) 
            when  pd.created_at is null and  date(prd.created_at) is not null then date(prd.created_at) 
            when  pd.created_at is not null and  date(prd.created_at) is null then pd.created_at
        end as created_at, 
       pd.created_at as pd_created_at, 
       prd.created_at as prd_created_at,
        case when pd.city is not null and prd.city is not null then prd.city 
             when pd.city is null and prd.city  is not null then prd.city 
             when pd.city is not null and prd.city  is null then pd.city
        end as city, 
        pd.city as pd_city, 
        prd_city as prd_city,
        pd.nights_per_rent_duration as pd_nights_per_rent_duration, 
        pd.min_nights_per_rent_duration as  pd_min_nights_per_rent_duration, 
        pd.max_nights_per_rent_duration as pd_max_nights_per_rent_duration, 
        pd.ll_max_rent_duration as pd_ll_max_rent_duration, 
        pd.nights_spent as pd_nights_spent,
        case when pd.beds_edited_team1 is not null and prd.beds is not null then prd.beds  
            when pd.beds_edited_team1 is null and prd.beds  is not null then prd.beds  
            when pd.beds_edited_team1 is not null and prd.beds  is null then pd.beds_edited_team1 
            end as pd_beds_edited_team1,
        case when pd.beds is not null and prd.beds is not null then prd.beds 
            when pd.beds is null and prd.beds is not null then prd.beds 
            when pd.beds is not null and prd.beds is null then pd.beds 
            end as beds,
        pd.price as pd_price, 
        prd.price as prd_price,
        case when pd.price is not null and prd.price is not null then prd.price 
             when pd.price is null and prd.price is not null then prd.price 
             when pd.price is not null and prd.price is null then pd.price 
             end as price,
        rent_per_person_categorical as pd_rent_per_person_categorical,
        prd_rent_per_person_categorical, 
        case when rent_per_person_categorical is null and prd_rent_per_person_categorical is not null then prd_rent_per_person_categorical
                 when rent_per_person_categorical is not null and prd_rent_per_person_categorical is null then rent_per_person_categorical
                 when rent_per_person_categorical is not null and prd_rent_per_person_categorical is not null then prd_rent_per_person_categorical 
                 end as rent_per_person_categorical,
        case when pd.id is not null or pd.id is null then prd.accommodates end as accommodates, 
        case when pd.id is not null or pd.id is null then prd.is_bookable end as is_bookable, 
        case when pd.id is not null or pd.id is null then prd.is_online end as is_online, 
        case when pd.id is not null or pd.id is null then prd.is_deleted end as is_deleted
        from product_data_final as prd
        full join pipedrive pd 
        on prd.freshworks_email = pd.email
    )
    Select * 
    from full_join_tab 
