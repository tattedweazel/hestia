WITH non_renewals_dnf as (
    SELECT user_key,
           signup_date,
           avg(vod_time_watched_min) as avg_vod_min
    FROM warehouse.daily_user_non_renewal_cohorts_v2
    WHERE total_sub_days <= 31
      AND days_into_sub <= 31
      AND sub_type = 'Returning Paid'
      AND signup_date >= '2021-04-01'
    GROUP BY 1, 2
), renewals_dnf as (
    SELECT user_key,
           signup_date,
           avg(vod_time_watched_min) as avg_vod_min
    FROM warehouse.daily_user_renewal_cohorts_v2
    WHERE total_sub_days > 31
      AND days_into_sub <= 30
      AND sub_type = 'Returning Paid'
      AND signup_date >= '2021-04-01'
    GROUP BY 1, 2
), do_not_fly as (
    SELECT distinct user_key
    FROM non_renewals_dnf
    WHERE avg_vod_min < 1 or avg_vod_min > 300
    UNION
    SELECT distinct user_key
    FROM renewals_dnf
    WHERE avg_vod_min < 1 or avg_vod_min > 300
), renewal_platform_case as (
    SELECT user_key,
           signup_date,
           total_sub_days,
           days_into_sub,
           (CASE
                WHEN platform like 'lr_%' THEN 'living_room'
                ELSE platform
            END) as platform,
           vod_time_watched_min
    FROM warehouse.daily_user_renewal_cohorts_v2
    WHERE sub_type = 'Returning Paid'
          AND user_key not in (SELECT user_key FROM do_not_fly)
          AND signup_date >= '2021-04-01'
), non_renewal_platform_case as (
    SELECT user_key,
           signup_date,
           total_sub_days,
           days_into_sub,
           (CASE
                WHEN platform like 'lr_%' THEN 'living_room'
                ELSE platform
            END) as platform,
           vod_time_watched_min
    FROM warehouse.daily_user_non_renewal_cohorts_v2
    WHERE sub_type = 'Returning Paid'
          AND user_key not in (SELECT user_key FROM do_not_fly)
          AND signup_date >= '2021-04-01'
), first_month_renewal_platforms as (
    SELECT user_key,
           signup_date,
           listagg(distinct platform, '-') within group ( order by platform, user_key ) as platforms,
           sum((CASE
               WHEN platform = 'web' THEN vod_time_watched_min
               ELSE 0
            END)) as vod_min_web,
           sum((CASE
               WHEN platform = 'ios' THEN vod_time_watched_min
               ELSE 0
            END)) as vod_min_ios,
           sum((CASE
               WHEN platform = 'android' THEN vod_time_watched_min
               ELSE 0
            END)) as vod_min_android,
           sum((CASE
               WHEN platform = 'living_room' THEN vod_time_watched_min
               ELSE 0
            END)) as vod_min_living_room
    FROM renewal_platform_case
    WHERE total_sub_days > 31
      and days_into_sub <= 30
    GROUP BY 1, 2
), first_month_non_renewal_platforms as (
    SELECT user_key,
           signup_date,
           listagg(distinct platform, '-') within group ( order by platform, user_key ) as platforms,
           sum((CASE
               WHEN platform = 'web' THEN vod_time_watched_min
               ELSE 0
            END)) as vod_min_web,
           sum((CASE
               WHEN platform = 'ios' THEN vod_time_watched_min
               ELSE 0
            END)) as vod_min_ios,
           sum((CASE
               WHEN platform = 'android' THEN vod_time_watched_min
               ELSE 0
            END)) as vod_min_android,
           sum((CASE
               WHEN platform = 'living_room' THEN vod_time_watched_min
               ELSE 0
            END)) as vod_min_living_room
    FROM non_renewal_platform_case
    WHERE total_sub_days <= 31
      and days_into_sub <= 31
    GROUP BY 1, 2
), renewal_counts as (
    SELECT platforms,
           count(*) as membership_renewals,
           sum(vod_min_web) as vod_min_web,
           sum(vod_min_ios) as vod_min_ios,
           sum(vod_min_android) as vod_min_android,
           sum(vod_min_living_room) as vod_min_living_room
    FROM first_month_renewal_platforms
    GROUP BY 1
), non_renewal_counts as (
       SELECT platforms,
           count(*) as membership_renewals,
           sum(vod_min_web) as vod_min_web,
           sum(vod_min_ios) as vod_min_ios,
           sum(vod_min_android) as vod_min_android,
           sum(vod_min_living_room) as vod_min_living_room
    FROM first_month_non_renewal_platforms
    GROUP BY 1
), renewal_totals as (
    SELECT count(*) as total_renewals
    FROM first_month_renewal_platforms
), non_renewal_totals as (
    SELECT count(*) as total_non_renewals
    FROM first_month_non_renewal_platforms
)
SELECT rc.platforms,
       rc.membership_renewals,
       nrc.membership_renewals,
       (rc.membership_renewals + nrc.membership_renewals) as cohort_total,
       ((SELECT * FROM renewal_totals) + (SELECT * FROM non_renewal_totals)) as membership_total,
       round(((rc.membership_renewals + nrc.membership_renewals)*1.0) / ((SELECT * FROM renewal_totals) + (SELECT * FROM non_renewal_totals)), 3) as membership_pct,
       round((rc.membership_renewals*1.0) / (rc.membership_renewals + nrc.membership_renewals), 3) as retentation_rate,
       round((rc.vod_min_web / 60.0), 0) as r_vod_hr_web,
       round((nrc.vod_min_web / 60.0), 0) as n_vod_hr_web,
       round(((rc.vod_min_ios) / 60.0), 0) as r_vod_hr_ios,
       round(((nrc.vod_min_ios) / 60.0), 0) as n_vod_hr_ios,
       round(((rc.vod_min_android) / 60.0), 0) as r_vod_hr_android,
       round(((nrc.vod_min_android) / 60.0), 0) as n_vod_hr_android,
       round(((rc.vod_min_living_room) / 60.0), 0) as r_vod_hr_living_room,
       round(((nrc.vod_min_living_room) / 60.0), 0) as n_vod_hr_living_room
FROM renewal_counts rc
FULL OUTER JOIN non_renewal_counts nrc on rc.platforms = nrc.platforms
ORDER BY membership_pct desc nulls last;