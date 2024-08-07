import decimal
import pandas as pd
from datetime import datetime, timedelta
from tools.modules.user_renewal_analysis.user_signups_and_renewals import UserSignupsAndRenewals
from utils.components.batcher import Batcher
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class UserSubViewership:

    def __init__(self, start_month):
        """This allows us to correctly identfy signups in a month. Correctly identifies a renewal. Correctly identifies a consecutive renewal."""
        self.start_month = start_month
        self.db_connector = DatabaseConnector('')
        self.loggerv3 = Loggerv3(name=__name__, file_location='', local_mode=1)
        self.table_name = 'user_sub_viewership'
        self.user_signups_and_renewals = UserSignupsAndRenewals(start_month=self.start_month)
        self.batcher = Batcher()
        self.user_signups = {}
        self.batch_limit = 2000
        self.MAX_DAYS_BETWEEN_RENEWALS = 31
        self.user_renewals = {}
        self.users = {}
        self.viewership_batches = []
        self.vod_viewership = []
        self.live_viewership = []
        self.final_datastructure = []
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


    def build_dataframe(self):
        self.loggerv3.info('Building initial dataframe')
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

            self.users[user] = {
                'user_key': user,
                'sub_type': self.user_signups[user]['sub_type'],
                'signup': signup,
                'months_renewed': months_renewed,
                'last_renewal_date': last_renewal_date,
                'final_sub_date': final_sub_date,
                'total_sub_days': (final_sub_date - signup).days,
                'total_vod_min_during_sub': decimal.Decimal(0.0),
                'latest_viewership': datetime.strptime('1900-01-01', '%Y-%m-%d'),
                'last_view_days_into_sub': None,
                'first_month_vod_min': decimal.Decimal(0.0),
                'total_live_min_during_sub': 0,
                'first_month_live_min': 0
            }


    def batch_viewership(self):
        self.loggerv3.info('Batching viewership')
        self.viewership_batches = self.batcher.list_to_list_batch(batch_limit=self.batch_limit, iterator=self.users)


    def get_vod_viewership_by_batch(self, batch_ids):
        vod_viewership = []
        results = self.db_connector.read_redshift(f"""
                                                    SELECT
                                                           vv.user_key,
                                                           cast(vv.start_timestamp as varchar(10)),
                                                           sum(vv.active_seconds) / 60.0 as vod_time_watched_min
                                                    FROM warehouse.vod_viewership vv
                                                    INNER JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
                                                    WHERE 
                                                        vv.start_timestamp >= '{self.start_month}' AND
                                                        vv.user_key in ({','.join(batch_ids)}) AND
                                                        vv.user_key is not null AND 
                                                        vv.max_position > 0 AND 
                                                        vv.active_seconds <= (dse.length_in_seconds*2)
                                                    GROUP BY 1, 2;
                                                """
                                                )

        for result in results:
            vod_viewership.append({
                'user_key': result[0],
                'viewership_date': datetime.strptime(result[1] + ' 23:59:59', '%Y-%m-%d %H:%M:%S'),
                'vod_time_watched_min': result[2]
            })
        return vod_viewership


    def get_vod_viewership(self):
        self.loggerv3.info('Getting VOD viewership')
        for batch in self.viewership_batches:
            self.vod_viewership.extend(self.get_vod_viewership_by_batch(batch))


    def aggregate_vod_viewership(self):
        self.loggerv3.info('Aggregating VOD viewership')
        for user_view in self.vod_viewership:
            user = self.users[str(user_view['user_key'])]
            signup = user['signup']
            final_sub_date = user['final_sub_date']
            first_month_date = signup + timedelta(days=self.MAX_DAYS_BETWEEN_RENEWALS)
            if signup <= user_view['viewership_date'] <= final_sub_date:
                user['total_vod_min_during_sub'] += user_view['vod_time_watched_min']
                user['latest_viewership'] = max(user['latest_viewership'], user_view['viewership_date'])
                user['last_view_days_into_sub'] = (user['latest_viewership'] - signup).days

            if signup <= user_view['viewership_date'] <= first_month_date:
                user['first_month_vod_min'] += user_view['vod_time_watched_min']


        for user in self.users:
            if self.users[user]['latest_viewership'] == datetime.strptime('1900-01-01', '%Y-%m-%d'):
                self.users[user]['latest_viewership'] = None
                self.users[user]['last_view_days_into_sub'] = None


    def get_live_viewership_by_batch(self, batch_ids):
        live_viewership = []
        results = self.db_connector.read_redshift(f"""
                                                    SELECT
                                                           du.user_key,
                                                           cast(lv.start_timestamp as varchar(10)),
                                                           sum(lv.active_seconds) / 60.0 as live_time_watched_min
                                                    FROM warehouse.livestream_viewership lv
                                                    INNER JOIN warehouse.dim_user du on lv.user_id = du.user_id
                                                    WHERE 
                                                        lv.start_timestamp >= '{self.start_month}' AND
                                                        du.user_key in ({','.join(batch_ids)})
                                                    GROUP BY 1, 2;
                                                """
                                                )

        for result in results:
            live_viewership.append({
                'user_key': result[0],
                'viewership_date': datetime.strptime(result[1] + ' 23:59:59', '%Y-%m-%d %H:%M:%S'),
                'live_time_watched_min': result[2]
            })
        return live_viewership


    def get_live_viewership(self):
        self.loggerv3.info('Getting Live viewership')
        for batch in self.viewership_batches:
            self.live_viewership.extend(self.get_live_viewership_by_batch(batch))


    def aggregate_live_viewership(self):
        self.loggerv3.info('Aggregating Live viewership')
        for user_view in self.live_viewership:
            user = self.users[str(user_view['user_key'])]
            signup = user['signup']
            final_sub_date = user['final_sub_date']
            first_month_date = signup + timedelta(days=self.MAX_DAYS_BETWEEN_RENEWALS)
            if signup <= user_view['viewership_date'] <= final_sub_date:
                user['total_live_min_during_sub'] += user_view['live_time_watched_min']

            if signup <= user_view['viewership_date'] <= first_month_date:
                user['first_month_live_min'] += user_view['live_time_watched_min']


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        for user, info in self.users.items():
            self.final_datastructure.append(info)
        self.final_dataframe = pd.DataFrame(self.final_datastructure)
        self.final_dataframe[['total_vod_min_during_sub', 'first_month_vod_min', 'total_live_min_during_sub', 'first_month_live_min']] = \
            self.final_dataframe[['total_vod_min_during_sub', 'first_month_vod_min', 'total_live_min_during_sub', 'first_month_live_min']].apply(pd.to_numeric, errors='coerce')


    def write_to_redshift(self):
        self.loggerv3.info('Writing to redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')


    def execute(self):
        self.loggerv3.start(f"Running User Sub Viewership for Month {self.start_month}")
        self.populate_user_signups_and_renewals()
        self.build_dataframe()
        self.batch_viewership()
        self.get_vod_viewership()
        self.aggregate_vod_viewership()
        self.get_live_viewership()
        self.aggregate_live_viewership()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.db_connector.update_redshift_table_permissions(self.table_name)
        self.loggerv3.success("All Processing Complete!")
