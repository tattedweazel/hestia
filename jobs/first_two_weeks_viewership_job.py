import pandas as pd
from base.etl_jobv3 import EtlJobV3


class FirstTwoWeeksViewershipJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector=None, file_location=''):
        super().__init__(target_date = target_date, jobname =  __name__, db_connector = db_connector, table_name = 'first_two_weeks_viewership')
        self.target_date = target_date
        self.user_viewership = []
        self.final_dataframe = None


    def populate_user_viewership(self):
        self.loggerv3.info('Getting new users')
        results = self.db_connector.read_redshift(f""" 
                                                    WITH user_subs as (
                                                        SELECT user_key,
                                                               start_timestamp as signup_date,
                                                               payment_provider,
                                                               listagg(subscription_event_type, '-') within group ( order by user_key ) as subs
                                                        FROM warehouse.subscription
                                                        WHERE
                                                              start_timestamp >= date_add('day', -14, '{self.target_date}') AND
                                                              start_timestamp < date_add('day', -13, '{self.target_date}') AND
                                                              subscription_event_type in ('FTP Conversion', 'New Paid', 'Returning Paid') AND
                                                              membership_plan = '1month'
                                                        GROUP BY 1, 2, 3
                                                    ), signups as (
                                                        SELECT
                                                            user_key,
                                                            (CASE
                                                                WHEN subs like '%%Returning Paid%%' THEN 'Returning Paid'
                                                                WHEN subs like '%%FTP Conversion%%' THEN 'FTP Conversion'
                                                                WHEN subs like '%%New Paid%%' THEN 'New Paid'
                                                            END) as sub_type,
                                                            signup_date,
                                                            payment_provider
                                                        FROM user_subs
                                                        WHERE sub_type != 'Returning Paid'
                                                    ), excluded_signups as (
                                                        SELECT vv.user_key
                                                        FROM warehouse.vod_viewership vv
                                                            LEFT JOIN signups s on s.user_key = vv.user_key
                                                        WHERE vv.start_timestamp < s.signup_date
                                                          AND vv.user_key in (SELECT user_key FROM signups WHERE payment_provider = 'roku')
                                                          AND vv.user_key is not null
                                                          AND vv.user_tier = 'premium'
                                                          AND vv.max_position > 0
                                                        GROUP BY 1
                                                    ), vod_viewership_n as (
                                                        SELECT vv.user_key,
                                                               s.sub_type,
                                                               cast(s.signup_date as varchar(10))      as signup_date,
                                                               cast(vv.start_timestamp as varchar(10)) as viewership_date,
                                                               dse.series_id,
                                                               dse.season_id,
                                                               dse.episode_key
                                                        FROM warehouse.vod_viewership vv
                                                             INNER JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
                                                             LEFT JOIN signups s on s.user_key = vv.user_key
                                                        WHERE vv.start_timestamp >= date_add('day', -14, '{self.target_date}')
                                                          AND vv.start_timestamp < '{self.target_date}'
                                                          AND vv.user_key in (SELECT user_key FROM signups)
                                                          AND vv.user_key not in (SELECT user_key FROM excluded_signups)
                                                          AND vv.user_key is not null
                                                          AND vv.user_tier = 'premium'
                                                          AND vv.max_position > 0
                                                        GROUP BY 1, 2, 3, 4, 5, 6, 7
                                                        ORDER BY viewership_date
                                                    ), live_viewership_n as (
                                                        SELECT du.user_key,
                                                               s.sub_type,
                                                               cast(s.signup_date as varchar(10))      as signup_date,
                                                               cast(lv.start_timestamp as varchar(10)) as viewership_date,
                                                               'livestream' as series_id,
                                                               null as season_id,
                                                               'livestream' as episode_key
                                                        FROM warehouse.livestream_viewership lv
                                                            INNER JOIN warehouse.dim_user du on lv.user_id = du.user_id
                                                            LEFT JOIN signups s on s.user_key = du.user_key
                                                        WHERE lv.start_timestamp >= date_add('day', -14, '{self.target_date}')
                                                          AND lv.start_timestamp < '{self.target_date}'
                                                          AND du.user_key in (SELECT user_key FROM signups)
                                                          AND du.user_key not in (SELECT user_key FROM excluded_signups)
                                                          AND du.user_key is not null
                                                          AND lv.user_tier = 'premium'
                                                        GROUP BY 1, 2, 3, 4, 5, 6, 7
                                                    )
                                                    SELECT *
                                                    FROM vod_viewership_n
                                                    UNION ALL
                                                    SELECT *
                                                    FROM live_viewership_n;
                                                    """)

        for result in results:
            self.user_viewership.append({
                'user_key': result[0],
                'sub_type': result[1],
                'signup_date': result[2],
                'viewership_date': result[3],
                'series_id': result[4],
                'season_id': result[5],
                'episode_key': result[6]
            })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.user_viewership)


    def write_to_redshift(self):
        self.loggerv3.info('Writing to redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start(f"Running First Two Week Viewership for {self.target_date}")
        self.populate_user_viewership()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
