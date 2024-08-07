import pandas as pd
from base.etl_jobv3 import EtlJobV3


class PremiumAttributionsV3Job(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='premium_attributions_v3')
        """ first viewership attribution as a premium user"""
        self.run_date = target_date
        self.next_day_subscription_threshold = 1
        self.previous_viewership_threshold = -1
        self.following_viewership_threshold = 5
        self.yt_members = []
        self.campaign_users = []
        self.attributed_user_viewership = []
        self.existing_users = []
        self.final_dataframe = None


    def get_yt_members(self):
        self.loggerv3.info('Getting YouTube members that connected account')
        query = f"""
        SELECT user_key
        FROM warehouse.youtube_rt_members
        WHERE sponsorship_starts_at >= '{self.target_date}'
              AND sponsorship_starts_at < date_add('days', {self.next_day_subscription_threshold}, '{self.target_date}');
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.yt_members.append(result[0])


    def get_campaign_users(self):
        self.loggerv3.info('Getting campaign users')
        query = f"""
        SELECT user_key
        FROM warehouse.signup_flow_campaign_attributions
        WHERE subscription_start >= '{self.target_date}'
              AND subscription_start < date_add('days', {self.next_day_subscription_threshold}, '{self.target_date}');
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.campaign_users.append(result[0])


    def get_attributions(self):
        self.loggerv3.info('Getting attributions')
        for subscription_event_type in ['Returning Paid', 'New Paid']:
            self.get_attributions_by_type(subscription_event_type)
        self.get_ftp_attributions()


    # Returning Paid & New Paid
    def get_attributions_by_type(self, subscription_event_type):
        self.loggerv3.info(f"Getting {subscription_event_type} attributions")
        query = f"""
        WITH members as (
            SELECT sub.user_key,
                   sub.start_timestamp as start,
                   sub.membership_plan
            FROM warehouse.subscription sub
            WHERE sub.subscription_event_type = '{subscription_event_type}'
              AND sub.event_timestamp >= '{self.target_date}'
              AND sub.event_timestamp < date_add('days', {self.next_day_subscription_threshold}, '{self.target_date}')
            GROUP BY 1, 2, 3
        ), following_vod_viewership as (
            SELECT
                vv.user_key,
                vv.start_timestamp,
                de.episode_key,
                'vod_viewership' as source_val
            FROM warehouse.vod_viewership vv
            INNER JOIN warehouse.dim_segment_episode de ON vv.episode_key = de.episode_key
            INNER JOIN members m ON m.user_key = vv.user_key AND vv.start_timestamp >= m.start
            WHERE vv.start_timestamp < date_add('days', {self.following_viewership_threshold}, '{self.target_date}')
        ), previous_vod_viewership as (
            SELECT
                vv.user_key,
                vv.start_timestamp,
                de.episode_key,
                'vod_viewership' as source_val
            FROM warehouse.vod_viewership vv
            INNER JOIN warehouse.dim_segment_episode de ON vv.episode_key = de.episode_key
            INNER JOIN members m ON m.user_key = vv.user_key AND vv.start_timestamp < m.start
            WHERE vv.start_timestamp > date_add('days', {self.previous_viewership_threshold}, '{self.target_date}')
        ), following_live_viewership as (
            SELECT
                du.user_key,
                ls.start_time as start_timestamp,
                ls.event_key as episode_key,
                'live_viewership' as source_val
            FROM warehouse.livestream_heartbeat lh
            INNER JOIN warehouse.dim_user du on du.user_id = lh.user_id
            INNER JOIN warehouse.livestream_schedule_v2 ls on lh.event_timestamp BETWEEN ls.start_time AND ls.end_time
            INNER JOIN members m ON m.user_key = du.user_key AND ls.start_time >= m.start
            WHERE
                ls.start_time < date_add('days', {self.following_viewership_threshold}, '{self.target_date}')
                AND ls.category in ('Streams', 'live')
            GROUP BY 1, 2, 3, 4
        ), previous_live_viewership as (
            SELECT
                du.user_key,
                ls.start_time as start_timestamp,
                ls.event_key as episode_key,
                'live_viewership' as source_val
            FROM warehouse.livestream_heartbeat lh
            INNER JOIN warehouse.dim_user du on du.user_id = lh.user_id
            INNER JOIN warehouse.livestream_schedule_v2 ls on lh.event_timestamp BETWEEN ls.start_time AND ls.end_time
            INNER JOIN members m ON m.user_key = du.user_key AND ls.start_time < m.start
            WHERE
                ls.start_time > date_add('days', {self.previous_viewership_threshold}, '{self.target_date}')
                AND ls.category in ('Streams', 'live')
            GROUP BY 1, 2, 3, 4
        ), following_viewership as (
            SELECT *
            FROM following_vod_viewership
            UNION 
            SELECT *
            FROM following_live_viewership
        ), previous_viewership as (
            SELECT *
            FROM previous_vod_viewership
            UNION 
            SELECT *
            FROM previous_live_viewership
        ), rank as (
            SELECT
                m.user_key,
                m.start as subscription_start,
                m.membership_plan,
                f.start_timestamp as following_viewership_start,
                f.episode_key as following_episode_key,
                f.source_val as following_source,
                p.start_timestamp as previous_viewership_start,
                p.episode_key as previous_episode_key,
                p.source_val as previous_source,
                row_number() OVER (PARTITION BY m.user_key ORDER BY f.start_timestamp ASC, p.start_timestamp DESC) AS row_number
            FROM members m
            LEFT JOIN following_viewership f ON f.user_key = m.user_key
            LEFT JOIN previous_viewership p ON p.user_key = m.user_key
            WHERE (p.start_timestamp is NOT NULL OR f.start_timestamp is NOT NULL)
        )
        SELECT
            user_key,
            subscription_start,
            coalesce(following_viewership_start, previous_viewership_start) as viewership_start,
            coalesce(following_episode_key, previous_episode_key) as episode_key,
            membership_plan,
            coalesce(following_source, previous_source)
        FROM rank
        where row_number = 1;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            user_key = result[0]
            if user_key not in self.existing_users:
                conversion = {
                    'subscription_event_type': subscription_event_type,
                    'user_key': result[0],
                    'subscription_start': result[1],
                    'viewership_start': result[2],
                    'episode_key': result[3],
                    'membership_plan': result[4],
                    'source': result[5]
                }
                self.attributed_user_viewership.append(conversion)


    def get_ftp_attributions(self):
        self.loggerv3.info("Getting FTP attributions")
        query = f"""
        WITH ftp_members as (
            SELECT
                user_key,
                start_timestamp as start
            FROM warehouse.subscription
            WHERE subscription_event_type = 'FTP Conversion'
                AND event_timestamp >= '{self.target_date}'
                AND event_timestamp < date_add('days', {self.next_day_subscription_threshold}, '{self.target_date}')
            GROUP BY 1, 2
        ), trial_members as (
            SELECT
                s.user_key,
                s.start_timestamp as start,
                s.membership_plan
            FROM warehouse.subscription s
            INNER JOIN ftp_members m on m.user_key = s.user_key and date_add('days', 10, s.start_timestamp) > m.start
            WHERE s.subscription_event_type = 'Trial'
        ), following_vod_viewership as (
            SELECT
                vv.user_key,
                vv.start_timestamp,
                de.episode_key,
                'vod_viewership' as source_val
            FROM warehouse.vod_viewership vv
            INNER JOIN warehouse.dim_segment_episode de ON vv.episode_key = de.episode_key
            INNER JOIN trial_members m ON m.user_key = vv.user_key AND vv.start_timestamp >= m.start
            WHERE vv.start_timestamp < date_add('days', {self.following_viewership_threshold}, '{self.target_date}')
        ),  previous_vod_viewership as (
            SELECT
                vv.user_key,
                vv.start_timestamp,
                de.episode_key,
                'vod_viewership' as source_val
            FROM warehouse.vod_viewership vv
            INNER JOIN warehouse.dim_segment_episode de ON vv.episode_key = de.episode_key
            INNER JOIN trial_members m ON m.user_key = vv.user_key AND vv.start_timestamp < m.start
            WHERE vv.start_timestamp > date_add('days', {self.previous_viewership_threshold}, '{self.target_date}')
        ), following_live_viewership as (
            SELECT
                du.user_key,
                ls.start_time as start_timestamp,
                ls.event_key as episode_key,
                'live_viewership' as source_val
            FROM warehouse.livestream_heartbeat lh
            INNER JOIN warehouse.dim_user du on du.user_id = lh.user_id
            INNER JOIN warehouse.livestream_schedule_v2 ls on lh.event_timestamp BETWEEN ls.start_time AND ls.end_time
            INNER JOIN trial_members m ON m.user_key = du.user_key AND ls.start_time >= m.start
            WHERE
                ls.start_time < date_add('days', {self.following_viewership_threshold}, '{self.target_date}')
                AND ls.category in ('Streams', 'live')
            GROUP BY 1, 2, 3, 4
        ), previous_live_viewership as (
            SELECT
                du.user_key,
                ls.start_time as start_timestamp,
                ls.event_key as episode_key,
                'live_viewership' as source_val
            FROM warehouse.livestream_heartbeat lh
            INNER JOIN warehouse.dim_user du on du.user_id = lh.user_id
            INNER JOIN warehouse.livestream_schedule_v2 ls on lh.event_timestamp BETWEEN ls.start_time AND ls.end_time
            INNER JOIN trial_members m ON m.user_key = du.user_key AND ls.start_time < m.start
            WHERE
                ls.start_time > date_add('days', {self.previous_viewership_threshold}, '{self.target_date}')
                AND ls.category in ('Streams', 'live')
            GROUP BY 1, 2, 3, 4
        ), following_viewership as (
            SELECT *
            FROM following_vod_viewership
            UNION 
            SELECT *
            FROM following_live_viewership
        ), previous_viewership as (
            SELECT *
            FROM previous_vod_viewership
            UNION 
            SELECT *
            FROM previous_live_viewership
        ), rank as (
            SELECT 
                m.user_key,
                m.start as subscription_start,
                m.membership_plan,
                f.start_timestamp as following_viewership_start,
                f.episode_key as following_episode_key,
                f.source_val as following_source,
                p.start_timestamp as previous_viewership_start,
                p.episode_key as previous_episode_key,
                p.source_val as previous_source,
                row_number() OVER (PARTITION BY m.user_key ORDER BY f.start_timestamp ASC, p.start_timestamp DESC) AS row_number
            FROM trial_members m
            INNER JOIN following_viewership f ON f.user_key = m.user_key
            LEFT JOIN previous_viewership p ON p.user_key = m.user_key
            WHERE (p.start_timestamp is NOT NULL OR f.start_timestamp is NOT NULL)
        )
        SELECT
            user_key,
            subscription_start,
            coalesce(following_viewership_start, previous_viewership_start) as viewership_start,
            coalesce(following_episode_key, previous_episode_key) as episode_key,
            membership_plan,
            coalesce(following_source, previous_source)
        FROM rank
        where row_number = 1;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            user_key = result[0]
            if user_key not in self.existing_users:
                conversion = {
                    'subscription_event_type': 'FTP Conversion',
                    'user_key': result[0],
                    'subscription_start': result[1],
                    'viewership_start': result[2],
                    'episode_key': result[3],
                    'membership_plan': result[4],
                    'source': result[5]
                }
                self.attributed_user_viewership.append(conversion)


    def build_final_dataframe(self):
        self.loggerv3.info("Building final dataframe")
        final_attributions = []
        for conversion in self.attributed_user_viewership:
            if conversion['user_key'] not in self.campaign_users and conversion['user_key'] not in self.yt_members:
                final_attributions.append(conversion)

        self.final_dataframe = pd.DataFrame(final_attributions)
        self.final_dataframe['run_date'] = self.run_date


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing results to Redshift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.info(f"Running Premium Attributions v3 for {self.target_date}")
        self.get_yt_members()
        self.get_campaign_users()
        self.get_attributions()
        self.build_final_dataframe()
        self.write_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
