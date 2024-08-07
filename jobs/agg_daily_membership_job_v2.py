import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class AggDailyMembershipJobV2(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='agg_daily_membership_v2')
        self.schema = 'warehouse'
        self.Dater = DateHandler()
        self.target_day = self.target_date.replace('-', '')
        self.target = self.Dater.format_date(self.target_day)
        self.next_day = self.Dater.format_date(self.Dater.find_next_day(self.target_day))
        self.six_ago = self.Dater.format_date(self.Dater.find_x_days_ago(self.target_day, 6))
        self.seven_ago = self.Dater.format_date(self.Dater.find_x_days_ago(self.target_day, 7))
        self.event_data = {
            'Paid Renewal': 0,
            'Returning Paid': 0,
            'FTP Conversion': 0,
            'New Paid': 0,
            'Trial Cancel Requested': 0,
            'Paid Cancel Requested': 0,
            'Trial Churn': 0,
            'Paid Churn': 0,
            'Paid Terminated': 0,
            'Trials': 0,
            'Free Signups': 0,
            'Conversion Rate': 0
        }
        self.final_dataframe = None
        self.paid_events = ('New Paid', 'Returning Paid', 'FTP Conversion', 'Paid Renewal')
        self.churn_events = ('Trial Cancel Requested', 'Paid Cancel Requested', 'Trial Churn', 'Paid Churn', 'Paid Terminated')


    def get_paid_events(self):
        self.loggerv3.info('Getting paid events')
        query = f"""
            WITH user_subs as (
                SELECT
                    user_key,
                    subscription_id,
                    listagg(subscription_event_type, '-') within group ( order by user_key ) as subs
                FROM
                     warehouse.subscription
                WHERE
                    subscription_event_type in {self.paid_events} AND
                    event_timestamp >= '{self.target}' AND
                    event_timestamp < '{self.next_day}'
                GROUP BY 1, 2
            ), coalesce_subs as (
                SELECT
                    user_key,
                    subscription_id,
                    (CASE
                        WHEN subs like '%%Paid Renewal%%' THEN 'Paid Renewal'
                        WHEN subs like '%%Returning Paid%%' THEN 'Returning Paid'
                        WHEN subs like '%%FTP Conversion%%' THEN 'FTP Conversion'
                        WHEN subs like '%%New Paid%%' THEN 'New Paid'
                    END) as sub_type
                FROM user_subs
            )
            SELECT
                sub_type,
                count(*)    
            FROM coalesce_subs
            GROUP BY 1;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.event_data[result[0]] = result[1]


    def get_churn_events(self):
        self.loggerv3.info('Getting churn events')
        query = f"""
            SELECT
                subscription_event_type, 
                count(*)
            FROM
                 warehouse.subscription
            WHERE
                subscription_event_type in {self.churn_events} AND
                event_timestamp >= '{self.target}' AND
                event_timestamp < '{self.next_day}'
            GROUP BY 1;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.event_data[result[0]] = result[1]


    def get_subscribers(self):
        query = f"""
            SELECT
                sub.user_key,
                du.user_id
            FROM
                warehouse.subscription sub
            LEFT JOIN
                warehouse.dim_user du
            ON du.user_key = sub.user_key
            WHERE
                sub.start_timestamp >= '{self.target_day}' AND
                sub.start_timestamp < '{self.next_day}'
        """
        results = self.db_connector.read_redshift(query)
        uuids = []
        for result in results:
            uuids.append(result[1])
        return uuids


    def get_free_signup_events(self):
        self.loggerv3.info('Getting free signup events')
        signup_uuids = []
        query = f"""
            SELECT 
                uuid
            FROM 
                users 
            WHERE 
                created_at >= '{self.target}' AND 
                created_at < '{self.next_day}'
        """
        results = self.db_connector.query_v2_db(query)
        for result in results:
            signup_uuids.append(result[0])

        subscription_event_uuids = self.get_subscribers()
        free_signup_uuids = []
        for uuid in signup_uuids:
            if uuid not in subscription_event_uuids and uuid not in free_signup_uuids:
                free_signup_uuids.append(uuid)

        self.event_data['Free Signups'] = len(free_signup_uuids)


    def get_trial_events(self):
        self.loggerv3.info('Getting trial events')
        query = f"""
            SELECT
                count(*)
            FROM
                warehouse.subscription sub
            WHERE
                sub.subscription_event_type = 'Trial' AND
                sub.start_timestamp >= '{self.target_day}' AND
                sub.start_timestamp < '{self.next_day}'
        """
        results = self.db_connector.read_redshift(query)

        for result in results:
            self.event_data['Trials'] = result[0]


    def get_trial_conversion_events(self):
        self.loggerv3.info('Getting trial conversion events')
        original_trials = 0
        query = f"""
		    SELECT
		        count(*)
		    FROM
		        warehouse.subscription sub
		    WHERE
		        sub.subscription_event_type = 'Trial' AND
		        sub.event_timestamp >= '{self.seven_ago}' AND
		        sub.event_timestamp < '{self.six_ago}'
		"""
        results = self.db_connector.read_redshift(query)

        for result in results:
            original_trials = result[0]

        if original_trials > 1:
            self.event_data['Conversion Rate'] = int((self.event_data['FTP Conversion'] / original_trials) * 100)


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame([{
            'date': self.target,
            'free_signups': self.event_data['Free Signups'],
            'trial_signups': self.event_data['Trials'],
            'new_paid_signups': self.event_data['New Paid'],
            'returning_paid_signups': self.event_data['Returning Paid'],
            'ftp_converts': self.event_data['FTP Conversion'],
            'trial_cancels': self.event_data['Trial Cancel Requested'],
            'paid_cancels': self.event_data['Paid Cancel Requested'],
            'trial_churn': self.event_data['Trial Churn'],
            'voluntary_paid_churn': self.event_data['Paid Churn'],
            'involuntary_paid_churn': self.event_data['Paid Terminated'],
            'paid_renewals': self.event_data['Paid Renewal'],
            'conversion_rate': self.event_data['Conversion Rate']
        }])


    def write_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, self.schema)


    def execute(self):
        self.loggerv3.start(f"Running Daily Membership for {self.target}")
        self.get_paid_events()
        self.get_churn_events()
        self.get_free_signup_events()
        self.get_trial_events()
        self.get_trial_conversion_events()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
