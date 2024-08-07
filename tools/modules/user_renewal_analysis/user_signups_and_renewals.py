from datetime import datetime
from dateutil.relativedelta import relativedelta
from utils.components.batcher import Batcher
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class UserSignupsAndRenewals:

    def __init__(self, start_month):
        """This allows us to correctly identify signups in a given month, a renewal, and a consecutive renewal."""
        self.start_month = start_month
        self.start_month_dt = datetime.strptime(start_month, '%Y-%m-%d')
        self.renewal_start_dt = self.start_month_dt + relativedelta(months=1)
        self.renewal_start = self.renewal_start_dt.strftime('%Y-%m-%d')
        self.db_connector = DatabaseConnector('')
        self.loggerv3 = Loggerv3(name=__name__, file_location='', local_mode=1)
        self.batcher = Batcher()
        self.user_signups = {}
        self.batch_limit = 2000
        self.MAX_DAYS_BETWEEN_RENEWALS = 31
        self.renewal_batches = []
        self.renewals = []
        self.user_renewals = {}
        self.loggerv3.alert = False


    def get_signup_ids(self):
        self.loggerv3.info('Getting signup ids')
        results = self.db_connector.read_redshift(f"""
                                                    WITH user_subs as (
                                                        SELECT user_key,
                                                               start_timestamp,
                                                               listagg(subscription_event_type, '-') within group ( order by user_key ) as subs
                                                        FROM warehouse.subscription
                                                        WHERE
                                                              start_timestamp >= '{self.start_month}' AND
                                                              start_timestamp < '{self.renewal_start}' AND
                                                              subscription_event_type in ('FTP Conversion', 'New Paid', 'Returning Paid') AND
                                                              membership_plan = '1month'
                                                        GROUP BY 1, 2
                                                    )
                                                    SELECT
                                                        user_key,
                                                        (CASE
                                                            WHEN subs like '%%Returning Paid%%' THEN 'Returning Paid'
                                                            WHEN subs like '%%FTP Conversion%%' THEN 'FTP Conversion'
                                                            WHEN subs like '%%New Paid%%' THEN 'New Paid'
                                                        END) as sub_type,
                                                        start_timestamp
                                                    FROM user_subs;
                                                    """)

        for result in results:
            self.user_signups[str(result[0])] = {
                'sub_type': result[1],
                'signup': result[2]
            }


    def batch_signup_ids(self):
        self.loggerv3.info('Batching signup ids')
        self.renewal_batches = self.batcher.list_to_list_batch(batch_limit=self.batch_limit, iterator=self.user_signups)


    def get_renewals_for_batch(self, batch_ids):
        batch_renewals = []
        results = self.db_connector.read_redshift(f"""
                                                       SELECT
                                                           user_key,
                                                           cast(start_timestamp as varchar(7)) as month,
                                                           min(start_timestamp)
                                                       FROM warehouse.subscription
                                                       WHERE
                                                           start_timestamp >= '{self.renewal_start}' AND
                                                           subscription_event_type = 'Paid Renewal' AND
                                                           user_key in ({','.join(batch_ids)})
                                                       GROUP BY 1, 2
                                                       ORDER BY 1, 2;
                                                   """
                                                   )

        for result in results:
            record = {
                'user_key': result[0],
                'renewal_date': result[2]

            }
            if record not in batch_renewals:
                batch_renewals.append(record)

        return batch_renewals


    def get_renewals(self):
        self.loggerv3.info('Getting renewals')
        for batch in self.renewal_batches:
            self.renewals.extend(self.get_renewals_for_batch(batch))


    def build_user_renewals(self):
        self.loggerv3.info('Building user renewals')
        for renewal in self.renewals:
            user = str(renewal['user_key'])
            signup = self.user_signups[user]['signup']
            if user not in self.user_renewals:
                self.user_renewals[user] = {
                    'signup': signup,
                    'renewals': {renewal['renewal_date']}
                }
            else:
                self.user_renewals[user]['renewals'].add(renewal['renewal_date'])


    def convert_user_renewals_to_sorted_list(self):
        self.loggerv3.info('Converting user renewals to list')
        for user in self.user_renewals:
            self.user_renewals[user]['renewals'] = sorted(self.user_renewals[user]['renewals'])


    def define_consecutive_renewals(self):
        self.loggerv3.info('Defining consecutive renewals')

        for user in self.user_renewals:
            self.user_renewals[user]['consecutive_renewals'] = []
            signup = self.user_renewals[user]['signup']
            renewals = self.user_renewals[user]['renewals']
            if (renewals[0] - signup).days <= self.MAX_DAYS_BETWEEN_RENEWALS:
                self.user_renewals[user]['consecutive_renewals'].append(renewals[0])
                for i in range(1, len(renewals)):
                    curr = renewals[i]
                    prev = renewals[i - 1]
                    if (curr - prev).days <= self.MAX_DAYS_BETWEEN_RENEWALS:
                        self.user_renewals[user]['consecutive_renewals'].append(renewals[i])
                    else:
                        break
        self.loggerv3.close()
