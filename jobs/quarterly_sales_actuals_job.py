import pandas as pd
from base.etl_jobv3 import EtlJobV3



class QuarterlySalesActualsJob(EtlJobV3):

    def __init__(self, file_name, for_quarter_date, for_quarter_end_date, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='quarterly_sales_targets', local_mode=True)
        """
        file_name: name of the file that has series_title, for_quarter, and target columns
        for_quarter_date: start of quarter in YYYY-MM-DD format
        for_quarter_end_date: end of quarter, plus 1 day, in YYYY-MM-DD format
        """
        self.file_name = file_name
        self.for_quarter_date = for_quarter_date
        self.for_quarter_end_date = for_quarter_end_date
        self.file_path = '/'.join(['downloads/quarterly_sales', self.file_name])
        self.final_dataframe = None


    def read_actuals(self):
        self.loggerv3.info('Reading actuals')
        self.final_dataframe = pd.read_csv(self.file_path)
        self.final_dataframe['target'] = self.final_dataframe['target'].str.replace(',', '')
        self.final_dataframe['for_quarter_date'] = self.for_quarter_date
        self.final_dataframe['for_quarter_end_date'] = self.for_quarter_end_date
        self.final_dataframe = self.final_dataframe.astype({'target': int})


    def write_to_redshift(self):
        self.loggerv3.info('Writing to Redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start(f"Running Quarterly Sales Actuals Job")
        self.read_actuals()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
