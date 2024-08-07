import pandas as pd
from base.etl_jobv3 import EtlJobV3


class SignupFlowCampaignAttributionsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='signup_flow_campaign_attributions')
        self.attributed_users = []
        self.final_dataframe = None


    def get_campaign_users(self):
        self.loggerv3.info('Getting campaign users')
        query = f"""
        WITH campaigns as (
            SELECT sfe.campaign,
                   du.user_key,
                   cast(event_timestamp as varchar(10)) as date,
                   event_timestamp
            FROM warehouse.signup_flow_event sfe
            INNER JOIN warehouse.dim_user du on du.user_id = sfe.user_id
            WHERE (
                    (sfe.state = 'entered' and sfe.step = 4 and user_tier = 'premium')
                    or (sfe.state = 'exited' and sfe.step = 3 and sfe.option_selected in ('credit', 'paypal'))
                )
              and sfe.campaign is NOT NULL
              and sfe.campaign != 'test_campaign'
        ), subs as (
            SELECT user_key,
                   cast(event_timestamp as varchar(10)) as date,
                   start_timestamp as subscription_start,
                   membership_plan
            FROM warehouse.subscription
            WHERE subscription_event_type in ('New Paid', 'FTP Conversion', 'Returning Paid')
        ), sub_campaigns as (
            SELECT
                c.campaign,
                c.user_key,
                c.event_timestamp,
                subs.subscription_start,
                subs.membership_plan
            FROM campaigns c
            INNER JOIN subs on c.user_key = subs.user_key and c.date = subs.date
        ), max_timestamp as (
            SELECT
                user_key,
                subscription_start,
                membership_plan,
                max(event_timestamp) as max_timestamp
            FROM sub_campaigns
            GROUP BY 1, 2, 3
        )
        SELECT
            sc.campaign,
            sc.user_key,
            cast(sc.subscription_start as varchar(10)),
            sc.membership_plan
        FROM sub_campaigns sc
        INNER JOIN max_timestamp as mt
            on mt.user_key = sc.user_key
            and mt.subscription_start = sc.subscription_start
            and mt.membership_plan = sc.membership_plan
            and mt.max_timestamp = sc.event_timestamp
        GROUP BY 1, 2, 3, 4;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            conversion = {
                'campaign': result[0],
                'user_key': result[1],
                'subscription_start': result[2],
                'membership_plan': result[3]
            }
            self.attributed_users.append(conversion)

        self.final_dataframe = pd.DataFrame(self.attributed_users)


    def truncate_table(self):
        self.loggerv3.info('Truncating table')
        self.db_connector.write_redshift(f"TRUNCATE TABLE warehouse.{self.table_name}")


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing results to Redshift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.info(f"Running Signup Flow Campaign Attributions Job")
        self.get_campaign_users()
        self.truncate_table()
        self.write_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
