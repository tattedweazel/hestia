import pandas as pd
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3



class UserNonRenewalAttribution:

    def __init__(self, target_date):
        self.target_date = target_date
        self.db_connector = DatabaseConnector('')
        self.loggerv3 = Loggerv3(name=__name__, file_location='', local_mode=1)
        self.table_name = 'user_non_renewal_attribution'
        self.ENDED_SUB_DAYS = 30
        self.non_renewal_subs = []
        self.non_renewal_users = []
        self.viewership_30day = []
        self.viewership_90day = []
        self.join_30day_df = None
        self.join_90day_df = None
        self.final_dataframe = None

        self.loggerv3.alert = False


    def get_ended_subs(self):
        self.loggerv3.info('Getting ended subs')
        results = self.db_connector.read_redshift(f"""
                                                    SELECT 
                                                        user_key, 
                                                        max(event_timestamp) as event_timestamp
                                                    FROM warehouse.subscription
                                                    WHERE
                                                     event_timestamp >= '{self.target_date}' AND 
                                                     event_timestamp <= dateadd(day, 1, '{self.target_date}') AND 
                                                     subscription_event_type in ('Paid Churn', 'Paid Terminated')
                                                    GROUP BY 1; 
                                                """
                                                   )
        for result in results:
            self.non_renewal_users.append(f"'{result[0]}'")
            self.non_renewal_subs.append({
                'user_key': result[0],
                'end_timestamp': result[1]
            })


    def get_30day_viewership(self):
        self.loggerv3.info('Getting 30 day viewership')
        results = self.db_connector.read_redshift(f"""
                                                        SELECT
                                                            vv.user_key,
                                                            vv.episode_key,
                                                            dse.episode_title,
                                                            dse.series_id,
                                                            dse.series_title,
                                                            dse.season_number,
                                                            dse.season_id,
                                                            sum(vv.active_seconds) / 60.0 as time_watched_min
                                                        FROM warehouse.vod_viewership vv
                                                        LEFT JOIN warehouse.dim_segment_episode dse on vv.episode_key = dse.episode_key
                                                        WHERE
                                                              vv.user_key in ({','.join(self.non_renewal_users)}) AND
                                                              vv.user_tier = 'premium' AND
                                                              vv.start_timestamp >= dateadd(day, -31, '{self.target_date}') AND 
                                                              vv.start_timestamp < '{self.target_date}'
                                                        GROUP BY 1, 2, 3, 4, 5, 6, 7;
                                                """
                                                   )

        for result in results:
            self.viewership_30day.append({
                'user_key': result[0],
                'episode_key': result[1],
                'episode_title': result[2],
                'series_id': result[3],
                'series_title': result[4],
                'season_number': result[5],
                'season_id': result[6],
                'time_watched_min': result[7],
                'viewing_cohort': 'last_30_days'
            })


    def get_90day_viewership(self):
        self.loggerv3.info('Getting 90 day viewership')
        results = self.db_connector.read_redshift(f"""
                                                        SELECT
                                                            vv.user_key,
                                                            vv.episode_key,
                                                            dse.episode_title,
                                                            dse.series_id,
                                                            dse.series_title,
                                                            dse.season_number,
                                                            dse.season_id,
                                                            sum(vv.active_seconds) / 60 as time_watched_min
                                                        FROM warehouse.vod_viewership vv
                                                        LEFT JOIN warehouse.dim_segment_episode dse on vv.episode_key = dse.episode_key
                                                        WHERE
                                                              vv.user_key in ({','.join(self.non_renewal_users)}) AND
                                                              vv.user_tier = 'premium' AND
                                                              vv.start_timestamp >= dateadd(day, -91, '{self.target_date}') AND 
                                                              vv.start_timestamp < dateadd(day, -31, '{self.target_date}')
                                                        GROUP BY 1, 2, 3, 4, 5, 6, 7;
                                                """
                                                   )

        for result in results:
            self.viewership_90day.append({
                'user_key': result[0],
                'episode_key': result[1],
                'episode_title': result[2],
                'series_id': result[3],
                'series_title': result[4],
                'season_number': result[5],
                'season_id': result[6],
                'time_watched_min': result[7],
                'viewing_cohort': 'last_31_90_days'
            })


    def join_non_renewals_30day_viewership(self):
        self.loggerv3.info('Joining non renewals with 30day viewership')
        non_renewal_subs_df = pd.DataFrame(self.non_renewal_subs)
        viewership_30day_df = pd.DataFrame(self.viewership_30day)
        self.join_30day_df = pd.merge(non_renewal_subs_df, viewership_30day_df, on='user_key', how='outer')
        self.join_30day_df['viewing_cohort'].fillna('last_30_days', inplace=True)


    def join_non_renewals_90day_viewership(self):
        self.loggerv3.info('Joining non renewals with 90day viewership')
        non_renewal_subs_df = pd.DataFrame(self.non_renewal_subs)
        viewership_90day_df = pd.DataFrame(self.viewership_90day)
        self.join_90day_df = pd.merge(non_renewal_subs_df, viewership_90day_df, on='user_key', how='outer')
        self.join_90day_df['viewing_cohort'].fillna('last_31_90_days', inplace=True)


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.concat([self.join_30day_df, self.join_90day_df])


    def write_to_redshift(self):
        self.loggerv3.info('Writing to redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')


    def execute(self):
        self.loggerv3.start(f'Starting User Non Renewal Attribution for {self.target_date}')
        self.get_ended_subs()
        self.get_30day_viewership()
        self.get_90day_viewership()
        self.join_non_renewals_30day_viewership()
        self.join_non_renewals_90day_viewership()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.db_connector.update_redshift_table_permissions(self.table_name)
        self.loggerv3.success("All Processing Complete!")
