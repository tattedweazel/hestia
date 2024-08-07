import pandas as pd
from datetime import datetime, timedelta
from tools.modules.user_renewal_analysis.user_signups_and_renewals import UserSignupsAndRenewals
from utils.components.batcher import Batcher
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class DailyUserNonRenewalCohortsV2:

    def __init__(self, start_month):
        self.start_month = start_month
        self.db_connector = DatabaseConnector('')
        self.loggerv3 = Loggerv3(name=__name__, file_location='', local_mode=1)
        self.table_name = 'daily_user_non_renewal_cohorts_v2'
        self.user_signups_and_renewals = UserSignupsAndRenewals(start_month=self.start_month)
        self.batcher = Batcher()
        self.user_signups = {}
        self.batch_limit = 2000
        self.MAX_DAYS_BETWEEN_RENEWALS = 31
        self.user_renewals = {}
        self.users = {}
        self.non_renewal_users = {}
        self.viewership_batches = []
        self.vod_viewership = []
        self.live_viewership = []
        self.viewership = []
        self.final_data_structure = []
        self.final_dataframe = None
        self.loggerv3.alert = False


    def populate_user_signups_and_renewals(self):
        self.user_signups_and_renewals.get_signup_ids()
        self.user_signups_and_renewals.batch_signup_ids()
        self.user_signups_and_renewals.get_renewals()
        self.user_signups_and_renewals.build_user_renewals()
        self.user_signups_and_renewals.convert_user_renewals_to_sorted_list()
        self.user_signups_and_renewals.define_consecutive_renewals()

        self.user_signups = self.user_signups_and_renewals.user_signups
        self.user_renewals = self.user_signups_and_renewals.user_renewals


    def identify_non_renewals(self):
        """
        Get "last renewal date. If none, then +31. Otherwise, get last renewal date and +31 that.
        Subset those users to either (signup, signup+31) or (last_renewal_date, last_renewal_date+31).
        """
        self.loggerv3.info('Identifying non renewals')
        for user in self.user_signups:
            signup = self.user_signups[user]['signup']
            if user in self.user_renewals:
                months_renewed = len(self.user_renewals[user]['consecutive_renewals'])
            else:
                months_renewed = 0

            if months_renewed == 0:
                last_renewal_date = signup
            else:
                last_renewal_date = max(self.user_renewals[user]['consecutive_renewals'])
            final_sub_date = last_renewal_date + timedelta(days=self.MAX_DAYS_BETWEEN_RENEWALS)

            self.non_renewal_users[user] = {
                'sub_type': self.user_signups[user]['sub_type'],
                'signup': signup,
                'last_renewal_date': last_renewal_date,
                'final_sub_date': final_sub_date
            }


    def batch_viewership(self):
        self.loggerv3.info('Batching viewership')
        self.viewership_batches = self.batcher.list_to_list_batch(batch_limit=self.batch_limit, iterator=self.non_renewal_users)


    def get_vod_viewership_by_batch(self, batch_ids):
        vod_viewership = []
        results = self.db_connector.read_redshift(f"""
                                                    SELECT
                                                           vv.user_key,
                                                           cast(vv.start_timestamp as varchar(10)),
                                                           lower(vv.platform),
                                                           sum(vv.active_seconds) / 60.0 as vod_time_watched_min,
                                                           count(distinct dse.series_id) as series_watched,
                                                           count(distinct dse.episode_key) as episodes_watched
                                                    FROM warehouse.vod_viewership vv
                                                    INNER JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
                                                    WHERE 
                                                        vv.start_timestamp >= '{self.start_month}' AND
                                                        vv.user_key in ({','.join(batch_ids)}) AND
                                                        vv.user_key is not null AND 
                                                        vv.max_position > 0 AND 
                                                        vv.active_seconds <= (dse.length_in_seconds*2)
                                                    GROUP BY 1, 2, 3;
                                                """
                                                )

        for result in results:
            vod_viewership.append({
                'user_key': result[0],
                'viewership_date': datetime.strptime(result[1] + ' 23:59:59', '%Y-%m-%d %H:%M:%S'),
                'platform': result[2],
                'vod_time_watched_min': result[3],
                'series_watched': result[4],
                'episodes_watched': result[5]
            })
        return vod_viewership


    def populate_vod_viewership(self):
        self.loggerv3.info('Getting VOD viewership')
        for batch in self.viewership_batches:
            self.vod_viewership.extend(self.get_vod_viewership_by_batch(batch))


    def get_live_viewership_by_batch(self, batch_ids):
        live_viewership = []
        results = self.db_connector.read_redshift(f"""
                                                     SELECT
                                                            du.user_key,
                                                            cast(lv.start_timestamp as varchar(10)),
                                                            lower(lv.platform),
                                                            sum(lv.active_seconds) / 60.0 as live_time_watched_min
                                                     FROM warehouse.livestream_viewership lv
                                                     INNER JOIN warehouse.dim_user du on lv.user_id = du.user_id
                                                     WHERE 
                                                         lv.start_timestamp >= '{self.start_month}' AND
                                                         du.user_key in ({','.join(batch_ids)})
                                                     GROUP BY 1, 2, 3;
                                                 """
                                                   )

        for result in results:
            live_viewership.append({
                'user_key': result[0],
                'viewership_date': datetime.strptime(result[1] + ' 23:59:59', '%Y-%m-%d %H:%M:%S'),
                'platform': result[2],
                'live_time_watched_min': result[3]
            })
        return live_viewership


    def populate_live_viewership(self):
        self.loggerv3.info('Getting Live viewership')
        for batch in self.viewership_batches:
            self.live_viewership.extend(self.get_live_viewership_by_batch(batch))


    def join_vod_live_viewership(self):
        self.loggerv3.info('Joining VOD and Live viewership')
        vod_viewership_df = pd.DataFrame(self.vod_viewership)
        live_viewership_df = pd.DataFrame(self.live_viewership)
        viewership_df = pd.merge(vod_viewership_df, live_viewership_df, on=['user_key', 'viewership_date', 'platform'], how='outer')
        viewership_df.update(viewership_df[['live_time_watched_min', 'vod_time_watched_min', 'series_watched', 'episodes_watched']].fillna(0))
        self.viewership = viewership_df.to_dict('records')


    def populate_final_data_structure(self):
        self.loggerv3.info('Building final dataframe')
        for user_view in self.viewership:
            user = str(user_view['user_key'])
            signup = self.non_renewal_users[user]['signup']
            last_renewal_date = self.non_renewal_users[user]['last_renewal_date']
            final_sub_date = self.non_renewal_users[user]['final_sub_date']
            if last_renewal_date <= user_view['viewership_date'] <= final_sub_date:
                self.final_data_structure.append({
                    'sub_type': self.user_signups[user]['sub_type'],
                    'platform': user_view['platform'],
                    'user_key': user,
                    'signup_date': signup,
                    'days_into_sub': (user_view['viewership_date'] - signup).days,
                    'vod_time_watched_min': user_view['vod_time_watched_min'],
                    'series_watched': user_view['series_watched'],
                    'episodes_watched': user_view['episodes_watched'],
                    'live_time_watched_min': user_view['live_time_watched_min'],
                    'total_sub_days': (final_sub_date - signup).days
                })

        self.final_dataframe = pd.DataFrame(self.final_data_structure)
        self.final_dataframe[['vod_time_watched_min', 'live_time_watched_min']] = self.final_dataframe[['vod_time_watched_min', 'live_time_watched_min']].apply(pd.to_numeric, errors='coerce')


    def write_to_redshift(self):
        self.loggerv3.info('Writing to redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')


    def execute(self):
        self.loggerv3.start(f"Running Daily User Non Renewal Cohorts for Month {self.start_month}")
        self.populate_user_signups_and_renewals()
        self.identify_non_renewals()
        self.batch_viewership()
        self.populate_vod_viewership()
        self.populate_live_viewership()
        self.join_vod_live_viewership()
        self.populate_final_data_structure()
        self.write_to_redshift()
        self.db_connector.update_redshift_table_permissions(self.table_name)
        self.loggerv3.success("All Processing Complete!")
