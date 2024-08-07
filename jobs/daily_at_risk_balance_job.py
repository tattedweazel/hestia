import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tools.modules.user_renewal_analysis.daily_at_risk_members import DailyAtRiskMembers


class DailyAtRiskBalanceJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector=None, file_location=''):
        super().__init__(target_date = target_date, jobname =  __name__, db_connector = db_connector, table_name = 'daily_at_risk_balance')
        self.darm = DailyAtRiskMembers(target_date=target_date)
        self.target_date_dt = datetime.strptime(target_date, '%Y-%m-%d')
        self.signup_date_dt = self.target_date_dt - relativedelta(days=14)
        self.signup_date = self.signup_date_dt.strftime('%Y-%m-%d')
        self.final_dataframe = None


    def build_dataframe(self):
        self.loggerv3.info('Building dataframe')
        at_risk_members = self.darm.execute()

        if at_risk_members:
            final_data_structure = {
                'signup_date': self.signup_date,
                'signups': at_risk_members[0]['total_population'],
                'at_risk': len(at_risk_members)
            }
        else:
            final_data_structure = {
                'signup_date': self.signup_date,
                'signups': 0,
                'at_risk': 0
            }

        self.final_dataframe = pd.DataFrame([final_data_structure])


    def write_to_redshift(self):
        self.loggerv3.info('Writing to redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start(f"Running Daily At Risk Balance for Date {self.target_date}")
        self.build_dataframe()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
