{{ config(
    alias="refugee_ll_bookings"
) }}

WITH
  deals AS (
  SELECT
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
    coalesce(lost_reason,
      "undefined") AS lost_reason,
    DATE_DIFF(CURRENT_DATE(), DATE(won_time),day) AS nights_from_won_date,
    CASE
      WHEN _ll_maximum_rent_duration_possible_ IN ('Less than one month', 'Unter einem Monat') THEN '<1 mo'
      WHEN _ll_maximum_rent_duration_possible_ IN ('One month',
      'Einen Monat') THEN '1 mo'
      WHEN _ll_maximum_rent_duration_possible_ IN ('One to three months', 'Einen bis drei Monate') THEN '1-3 mo'
      WHEN _ll_maximum_rent_duration_possible_ IN ('Three to six months',
      'Drei bis sechs Monate') THEN '3-6 mo'
      WHEN _ll_maximum_rent_duration_possible_ IN ('Longer than six months', 'Länger als sechs Monate') THEN '>6 mo'
    ELSE
    'undefined'
  END
    AS ll_max_rent_duration,
    CASE
      WHEN _ll_rent_per_person_ IN ('Kostenlos', 'I would rent it for free') THEN 'for_free'
      WHEN _ll_rent_per_person_ IN ('über 500€ pro Monat',
      'More than 500 Euros/month') THEN '>500 mo'
      WHEN _ll_rent_per_person_ IN ("Ich bin mir nicht sicher", "I'm unsure") THEN 'unsure'
      WHEN _ll_rent_per_person_ IN ("Under 250 Euros/month",
      "Unter 250€ pro Monat") THEN '<250 mo'
      WHEN _ll_rent_per_person_ IN ("250 - 500€ pro Monat", "Between 250 - 500 Euros/month") THEN '250-500 mo'
    ELSE
    'undefined'
  END
    AS ll_rent_per_person,
    CASE
      WHEN _ll_rent_per_person_ IN ('Kostenlos', 'I would rent it for free') THEN 0
      WHEN _ll_rent_per_person_ IN ('über 500€ pro Monat',
      'More than 500 Euros/month') THEN 1000
      WHEN _ll_rent_per_person_ IN ("Ich bin mir nicht sicher", "I'm unsure") THEN NULL
      WHEN _ll_rent_per_person_ IN ("Under 250 Euros/month",
      "Unter 250€ pro Monat") THEN 250
      WHEN _ll_rent_per_person_ IN ("250 - 500€ pro Monat", "Between 250 - 500 Euros/month") THEN 500
    ELSE
    0
  END
    AS ll_rent_per_person_numeric,
    CASE
      WHEN _ll_pets_possible_ IN ('Vielleicht', 'Maybe') THEN 'maybe'
      WHEN _ll_pets_possible_ IN ('Nein',
      'No') THEN 'no'
      WHEN _ll_pets_possible_ IN ('Ja', 'Yes') THEN 'yes'
    ELSE
    'undefined'
  END
    AS ll_pets_allowed,
    te_move_in_date,
    te_priority,
    CASE
      WHEN _ll_maximum_rent_duration_possible_ IN ('Less than one month', 'Unter einem Monat') THEN 15
      WHEN _ll_maximum_rent_duration_possible_ IN ('One month',
      'Einen Monat') THEN 30
      WHEN _ll_maximum_rent_duration_possible_ IN ('One to three months', 'Einen bis drei Monate') THEN 60
      WHEN _ll_maximum_rent_duration_possible_ IN ('Three to six months',
      'Drei bis sechs Monate') THEN 120
      WHEN _ll_maximum_rent_duration_possible_ IN ('Longer than six months', 'Länger als sechs Monate') THEN 200
    ELSE
    0
  END
    AS nights_per_rent_duration,
    CASE
      WHEN _ll_maximum_rent_duration_possible_ IN ('Less than one month', 'Unter einem Monat') THEN 1
      WHEN _ll_maximum_rent_duration_possible_ IN ('One month',
      'Einen Monat') THEN 30
      WHEN _ll_maximum_rent_duration_possible_ IN ('One to three months', 'Einen bis drei Monate') THEN 31
      WHEN _ll_maximum_rent_duration_possible_ IN ('Three to six months',
      'Drei bis sechs Monate') THEN 91
      WHEN _ll_maximum_rent_duration_possible_ IN ('Longer than six months', 'Länger als sechs Monate') THEN 181
    ELSE
    0
  END
    AS min_nights_per_rent_duration,
    CASE
      WHEN _ll_maximum_rent_duration_possible_ IN ('Less than one month', 'Unter einem Monat') THEN 29
      WHEN _ll_maximum_rent_duration_possible_ IN ('One month',
      'Einen Monat') THEN 30
      WHEN _ll_maximum_rent_duration_possible_ IN ('One to three months', 'Einen bis drei Monate') THEN 89
      WHEN _ll_maximum_rent_duration_possible_ IN ('Three to six months',
      'Drei bis sechs Monate') THEN 179
      WHEN _ll_maximum_rent_duration_possible_ IN ('Longer than six months', 'Länger als sechs Monate') THEN 360
    ELSE
    0
  END
    AS max_nights_per_rent_duration,
    CASE
      WHEN _ll_pets_possible_ IN ('Nein', 'No') THEN False
      WHEN _ll_pets_possible_ IN ('Ja',
      'Yes') THEN True
  END
    AS ll_pets_allowed_bool,
    CASE
      WHEN te_pets_cleaned IN ('%no %', '%without', 'None%') THEN False
    ELSE
    True
  END
    AS te_has_pets,
    CASE
      WHEN te_disabled_help_needed_people_cleaned IN ('%no %', '%without', 'None%') THEN 0
    ELSE
    1
  END
    AS te_has_disability,
    te_priority,
  IF
    (status ='open',
      1,
      0) AS open_deal,
  IF
    (status ='lost',
      1,
      0) AS lost_deal,
  IF
    (status ='won',
      1,
      0) AS won_deal,
    _ll_amount_rental_units_,
    _ll_beds_available_in_total_,
    ll_slots_available_edit_team_1,
    _ll_rent_per_person_,
    _ll_maximum_rent_duration_possible_,
    te_number_of_people_in_the_group_keep_updated,
    te_number_of_children_under_18_
  from {{ ref('stg_pipedrive_refugee__refugee_deals') }} a
  LEFT JOIN
    UNNEST(a.person_id.email) AS email
  WITH
  OFFSET
  WHERE
    TRUE
    AND email.primary = TRUE ),
  won_pipedrive_deals AS (
  SELECT
    *
  FROM
    deals
  WHERE
    TRUE
    AND pipeline_id = 2
    AND lost_reason NOT IN ('Flawed/Fake Data Entry',
      'Duplicate Data')
    AND status = 'won' ),
  bookings_core AS (
  SELECT
    rfg_bk.booking_id,
    rfg_bk.actual_booking_created_at,
    rfg_bk.booked_from,
    rfg_bk.booked_to,
    DATE_DIFF(rfg_bk.booked_to,rfg_bk.booked_from,day) AS prd_nights_rent_duration,
    LOWER(core_ls.city)AS prd_city,
    core_ls.listing_id,
    core_ls.is_bookable,
    core_ls.is_online,
    core_ls.is_published,
    core_ls.is_deleted,
    core_ls.created_at,
    DATE(core_ls.available_from) AS available_from,
    DATE(core_ls.available_to) AS available_to,
    core_ls.beds,
    core_ls.price,
    core_ls.rooms,
    core_ls.city,
    core_ls.accommodates,
    core_ls.area,
    core_ls.next_available_date
  from {{ ref('core_refugee__bookings') }} rfg_bk
  left join {{ ref('core__listings') }}  core_ls
  ON rfg_bk.listing_id = core_ls.listing_id
  WHERE core_ls.listing_id IS NOT NULL 
  ),rfg_listing_request AS (
  SELECT
    DISTINCT listing_id,
    pets_allowed
  from {{ ref('core_refugee__listing_requests') }} 
),
    bookings_refugee AS (
    SELECT
        rfg_bk.booking_id,
        rfg_ll.freshworks_email as email,
        rfg_ll.landlord_id, 
        rfg_bk.actual_booking_created_at as created_at, 
        rfg_bk.booked_from, 
        rfg_bk.booked_to, 
        rfg_bk.is_canceled,
        rfg_bk.listing_id,
        IF(rfg_lis.is_online is null, core_bk.is_online, rfg_lis.is_online) as is_online,
        IF(rfg_lis.is_bookable is null, core_bk.is_bookable, rfg_lis.is_bookable) as is_bookable,
        IF(rfg_lis.is_published is null, core_bk.is_published, rfg_lis.is_published) as is_published,
        IF(rfg_lis.is_deleted is null, core_bk.is_deleted, rfg_lis.is_deleted) as is_deleted,
        IF(rfg_lis.available_from is null, date(core_bk.available_from), date(rfg_lis.available_from)) as listing_available_from,
        IF(rfg_lis.available_to is null, date(core_bk.available_to), date(rfg_lis.available_to)) as listing_available_to,
        date_diff(rfg_bk.booked_to,rfg_bk.booked_from, day ) as nights_per_rent_duration, 
        IF(DATE_DIFF(CURRENT_DATE(),DATE(rfg_bk.booked_from), day) < 0,0,DATE_DIFF(CURRENT_DATE(),DATE(rfg_bk.booked_from), day)) AS nights_spent,
        IF(rfg_lis.price IS NULL, core_bk.price, rfg_lis.price)as price,
        IF(rfg_lis.beds IS NULL, core_bk.beds, rfg_lis.beds) AS beds,
        IF(rfg_lis.city is null, core_bk.city, rfg_lis.city) as city, 
        IF(rfg_lis.accommodates is null, core_bk.accommodates, rfg_lis.accommodates) as accommodates, 
        IF(rfg_lis.rooms is null, core_bk.rooms, rfg_lis.rooms) as rooms, 
        IF(rfg_lis.area is null, core_bk.area, rfg_lis.area) as area, 
        IF(rfg_lis.next_available_date is null, core_bk.next_available_date, rfg_lis.next_available_date) as next_available_date,
        cast(rfg_lr.pets_allowed as BOOL) as pets_allowed
    FROM {{ ref('core_refugee__bookings') }} rfg_bk
    LEFT JOIN bookings_core core_bk
    ON rfg_bk.booking_id = core_bk.booking_id
    LEFT JOIN {{ ref('core_refugee__listings') }} rfg_lis
    ON rfg_lis.listing_id = rfg_bk.listing_id
    LEFT JOIN rfg_listing_request rfg_lr
    ON rfg_lis.listing_id = rfg_lr.listing_id
    left join {{ ref('core_refugee__landlords') }}  rfg_ll
    on rfg_ll.landlord_id = rfg_bk.landlord_id
), 
    pipedrive_bookings as (
        Select 
        id, 
        email, 
        won_time as created_at,  
        deleted as is_deleted,
        nights_per_rent_duration,
        min_nights_per_rent_duration,
        max_nights_per_rent_duration, 
        ll_max_rent_duration, 
        nights_from_won_date as nights_spent,
        ll_rent_per_person_numeric as price,
        ll_rent_per_person,  
        _ll_beds_available_in_total_ as beds,
        ll_slots_available_edit_team_1 as beds_edited_team1, 
        city,
        ll_pets_allowed_bool as pets_allowed
        from won_pipedrive_deals
    ), full_join_tab as (
        Select 
        case when pd.id is not null and prd.booking_id is not null then 'both_sources'
             when pd.id is not null and prd.booking_id is null then 'in_pipedrive_only'
             when pd.id is null and prd.booking_id is not null then 'in_product_only'
        end as source_flag,
        case when pd.id is not null and prd.booking_id is not null then prd.booking_id
             when pd.id is null and prd.booking_id is not null then prd.booking_id 
             when pd.id is not null and prd.booking_id is null then pd.id
             end as distinct_bk_id, 
        pd.id as pd_bk_id, 
        prd.booking_id as prd_bk_id,
        case when pd.email is not null and prd.email  is not null then prd.email 
            when pd.email is null and prd.email  is not null then prd.email  
            when pd.email is not null and prd.email  is null then pd.email
            end as email, 
        pd.email as pd_email, 
        prd.email as prd_email,
        prd.landlord_id,
        case when  pd.created_at is not null and  date(prd.created_at) is not null then date(prd.created_at) 
             when  pd.created_at is null and  date(prd.created_at) is not null then date(prd.created_at) 
             when  pd.created_at is not null and  date(prd.created_at) is null then pd.created_at
        end as created_at, 
        pd.created_at as pd_created_at, 
        prd.created_at as prd_created_at,
        case when pd.id is not null and prd.booking_id is not null then prd.booked_from else prd.booked_from end as booked_from, 
        case when pd.id is not null and prd.booking_id is not null then prd.booked_to else prd.booked_from end as booked_to, 
        case when pd.city is not null and prd.city is not null then prd.city 
             when pd.city is null and prd.city  is not null then prd.city 
             when pd.city is not null and prd.city  is null then pd.city
        end as city, 
        case when pd.max_nights_per_rent_duration is null then prd.nights_per_rent_duration
             when pd.max_nights_per_rent_duration is not null then pd.max_nights_per_rent_duration end max_nights_per_rent_duration,
        case when pd.min_nights_per_rent_duration is null then prd.nights_per_rent_duration
             when pd.min_nights_per_rent_duration is not null then pd.min_nights_per_rent_duration end min_nights_per_rent_duration,
        case when pd.nights_per_rent_duration is null then prd.nights_per_rent_duration
             when pd.nights_per_rent_duration is not null then pd.nights_per_rent_duration end nights_per_rent_duration,
        case when pd.nights_per_rent_duration is null then prd.nights_per_rent_duration end as prd_nights_rent_duration,
        ll_max_rent_duration as pd_max_rent_duration_raw,
        case when ll_max_rent_duration is null and prd.nights_per_rent_duration < 30 then "<1 mo"
             when ll_max_rent_duration is null and prd.nights_per_rent_duration = 30 then "1 mo"
             when ll_max_rent_duration is null and (prd.nights_per_rent_duration >30 and prd.nights_per_rent_duration < 90 ) then "1-3 mo"
             when ll_max_rent_duration is null and (prd.nights_per_rent_duration > 90 and prd.nights_per_rent_duration < 180) then "3-6 mo"
             when ll_max_rent_duration is null and prd.nights_per_rent_duration >= 180 then ">6 mo"
             when ll_max_rent_duration is not null then ll_max_rent_duration end as max_rent_duration_categorical,
        case when pd.nights_spent is not null and prd.nights_spent is not null then prd.nights_spent
             when pd.nights_spent is null and prd.nights_spent is not null then prd.nights_spent 
             when pd.nights_spent is not null and prd.nights_spent is null then pd.nights_spent
             end as nights_spent,
        case when pd.beds_edited_team1 is not null and prd.beds is not null then prd.beds  
            when pd.beds_edited_team1 is null and prd.beds  is not null then prd.beds  
            when pd.beds_edited_team1 is not null and prd.beds  is null then pd.beds_edited_team1 
            end as pd_beds_edited_team1,
        case when pd.beds is not null and prd.beds is not null then prd.beds 
            when pd.beds is null and prd.beds is not null then prd.beds 
            when pd.beds is not null and prd.beds is null then pd.beds 
            end as beds,
        case when pd.price is not null and prd.price is not null then prd.price 
             when pd.price is null and prd.price is not null then prd.price 
             when pd.price is not null and prd.price is null then pd.price 
             end as price,
        pd.ll_rent_per_person as pd_ll_rent_per_person,
        case when prd.price = 0 then 'for_free'
             when prd.price < 250 then '<250 mo' 
             when prd.price >=250 and prd.price < 500 then '250-500 mo' 
             when prd.price >=500 then '>500 mo'
             else null end as prd_rent_per_person_categorical,
        case when pd.email is not null and prd.email is not null then prd.rooms
             when pd.email is null and prd.email is not null then prd.rooms 
             end as rooms, 
        case when pd.pets_allowed is not null and prd.pets_allowed is not null then prd.pets_allowed 
            when pd.pets_allowed is null and prd.pets_allowed is not null then prd.pets_allowed 
            when pd.pets_allowed is not null and prd.pets_allowed is null then pd.pets_allowed 
            end as pets_allowed,
        case when pd.id is not null or pd.id is null then prd.accommodates end as accommodates, 
        case when pd.id is not null or pd.id is null then prd.is_bookable end as is_bookable, 
        case when pd.id is not null or pd.id is null then prd.is_online end as is_online, 
        case when pd.id is not null or pd.id is null then prd.is_canceled end as is_canceled, 
        case when pd.id is not null or pd.id is null then prd.is_deleted end as is_deleted
        from pipedrive_bookings pd 
        full join bookings_refugee prd 
        on pd.email = prd.email
    ), 
        final as (
            Select 
            source_flag, 
            distinct_bk_id, 
            pd_bk_id as pd_id, 
            prd_bk_id as prd_bookingid, 
            email, 
            pd_email, 
            prd_email, 
            landlord_id,
            created_at,
            pd_created_at, 
            prd_created_at, 
            booked_from as prd_booked_from,
            booked_to as  prd_booked_to,
            city, 
            max_nights_per_rent_duration, 
            min_nights_per_rent_duration, 
            nights_per_rent_duration, 
            prd_nights_rent_duration, 
            pd_max_rent_duration_raw, 
            max_rent_duration_categorical, 
            nights_spent, 
            pd_beds_edited_team1, 
            beds, 
            price, 
            case when pd_ll_rent_per_person is null and prd_rent_per_person_categorical is not null then prd_rent_per_person_categorical
                 when pd_ll_rent_per_person is not null and prd_rent_per_person_categorical is null then pd_ll_rent_per_person
                 when pd_ll_rent_per_person is not null and prd_rent_per_person_categorical is not null then prd_rent_per_person_categorical 
                 end as rent_per_person_categorical,
            pd_ll_rent_per_person, 
            prd_rent_per_person_categorical, 
            rooms, 
            accommodates, 
            pets_allowed,
            is_bookable, 
            is_online, 
            is_canceled, 
            is_deleted
            from full_join_tab 
        )
Select * 
from final
