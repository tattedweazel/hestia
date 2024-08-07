import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.connectors.supporting_cast_api_connector import SupportingCastApiConnector


class SupportingCastPlansJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, table_name = 'supporting_cast_plans')
        self.page = 1
        self.records_per_page = 400
        self.supporting_cast_api_connector = SupportingCastApiConnector(file_location=self.file_location)
        self.total_pages = None
        self.export_endpoint = 'plans'
        self.records = None
        self.cleaned_records = None
        self.final_dataframe = None
        self.loggerv3.alert = False


    def request_data_from_url(self):
        self.loggerv3.info('Requesting data from Supporting Cast')
        response = self.supporting_cast_api_connector.make_feeds_request(
            endpoint=self.export_endpoint,
            page=self.page,
            records_per_page=self.records_per_page
        )
        self.records = response['data']


    def select_records(self):
        self.loggerv3.info('Selecting records')
        self.cleaned_records = []
        for record in self.records:
            data = {
                'plan_id': record['id'],
                'plan_name': record['name'],
                'live': record['live'],
                'amount': record['amount'],
                'interval': record['interval'],
                'interval_count': record['interval_count'],
                'stripe_pricing_plan_id': record['stripe_pricing_plan_id']
            }
            self.cleaned_records.append(data)


    def build_dataframe(self):
        self.loggerv3.info('Building dataframe')
        self.final_dataframe = pd.DataFrame(self.cleaned_records, index=None)


    def truncate_table(self):
        self.loggerv3.info(f'Truncating {self.table_name}')
        self.db_connector.write_redshift(f"""TRUNCATE TABLE warehouse.{self.table_name};""")


    def write_to_redshift(self):
        self.loggerv3.info("Writing to Redshift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)

    def execute(self):
        self.loggerv3.start(f"Running Supporting Cast Plans Job")
        self.request_data_from_url()
        self.select_records()
        self.build_dataframe()
        # self.truncate_table()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
