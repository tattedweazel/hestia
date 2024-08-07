import pandas as pd
from datetime import datetime, timedelta
from math import floor
from tools.modules.user_renewal_analysis.user_signups_and_renewals import UserSignupsAndRenewals
from utils.components.batcher import Batcher
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class NonRenewalViewership:

    def __init__(self, start_month):
        """Identifies all of the users who did not renew in their X month by sub type and what they watched in that month"""
        self.start_month = start_month
        self.db_connector = DatabaseConnector('')
        self.loggerv3 = Loggerv3(name=__name__, file_location='', local_mode=1)
        self.table_name = 'non_renewal_viewership'
        self.user_signups_and_renewals = UserSignupsAndRenewals(start_month=self.start_month)
        self.batcher = Batcher()
        self.user_signups = {}
        self.batch_limit = 2000
        self.MAX_DAYS_BETWEEN_RENEWALS = 31
        self.user_renewals = {}
        self.non_renewal_users = {}
        self.viewership_batches = []
        self.vod_viewership = []
        self.user_viewership = []
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
                                                           dse.series_id,
                                                           dse.series_title
                                                    FROM warehouse.vod_viewership vv
                                                    INNER JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
                                                    WHERE 
                                                        vv.start_timestamp >= '{self.start_month}' AND
                                                        vv.user_key in ({','.join(batch_ids)}) AND
                                                        vv.user_key is not null AND 
                                                        vv.active_seconds <= (dse.length_in_seconds*2)
                                                    GROUP BY 1, 2, 3, 4;
                                                """
                                                )

        for result in results:
            vod_viewership.append({
                'user_key': result[0],
                'viewership_date': datetime.strptime(result[1] + ' 23:59:59', '%Y-%m-%d %H:%M:%S'),
                'series_id': result[2],
                'series_title': result[3]
            })
        return vod_viewership


    def populate_vod_viewership(self):
        self.loggerv3.info('Getting VOD viewership')
        for batch in self.viewership_batches:
            self.vod_viewership.extend(self.get_vod_viewership_by_batch(batch))


    def populate_user_viewership(self):
        self.loggerv3.info('Populating user viewership')
        for user_view in self.vod_viewership:
            user = str(user_view['user_key'])
            signup = self.non_renewal_users[user]['signup']
            last_renewal_date = self.non_renewal_users[user]['last_renewal_date']
            final_sub_date = self.non_renewal_users[user]['final_sub_date']
            if last_renewal_date <= user_view['viewership_date'] <= final_sub_date:
                self.user_viewership.append({
                    'sub_type': self.user_signups[user]['sub_type'],
                    'user_key': user,
                    'signup_date': signup,
                    'days_into_sub': (user_view['viewership_date'] - signup).days,
                    'viewership_date': user_view['viewership_date'],
                    'sub_month': floor((user_view['viewership_date'] - signup).days / self.MAX_DAYS_BETWEEN_RENEWALS),
                    'series_id': user_view['series_id']
                })


    def populate_final_dataframe(self):
        self.loggerv3.info('Populating final dataframe')
        final_dataframe = pd.DataFrame(self.user_viewership)
        self.final_dataframe = final_dataframe.groupby(['sub_type', 'sub_month', 'series_id']).agg({'user_key': pd.Series.nunique}).reset_index()
        self.final_dataframe.rename(columns={'user_key': 'viewers'}, inplace=True)
        self.final_dataframe['cohort_month'] = self.start_month


    def write_to_redshift(self):
        self.loggerv3.info('Writing to redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')


    def execute(self):
        self.loggerv3.start(f"Running Non Renewal Viewership for {self.start_month}")
        self.populate_user_signups_and_renewals()
        self.identify_non_renewals()
        self.batch_viewership()
        self.populate_vod_viewership()
        self.populate_user_viewership()
        self.populate_final_dataframe()
        self.write_to_redshift()
        self.db_connector.update_redshift_table_permissions(self.table_name)
        self.loggerv3.success("All Processing Complete!")
