import os
import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.connectors.s3_api_connector import S3ApiConnector


class YouTubeChannelRevenueJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='yt_weekly_channel_revenue')
        self.rt_bucket = 'rt-data-youtube'
        self.prefix = 'revenue/'
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket=self.rt_bucket, profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.downloads_directory = self.file_location + 'downloads/youtube/'
        self.file_name = 'weekly_channel_revenue.csv'
        self.records_df = None
        self.records = []

        self.loggerv3.disable_alerting()


    def clear_downloads_directory(self):
        self.loggerv3.info('Clearing downloads directory')
        if len(os.listdir(f'{self.downloads_directory}/')) > 1:
            os.system(f'rm {self.downloads_directory}/*.*')


    def download_files_from_s3(self):
        self.loggerv3.info('Downloading files from s3')
        key = self.prefix + self.file_name
        filename = '/'.join([self.downloads_directory, self.file_name])
        self.s3_api_connector.download_files_from_object(key=key, filename=filename)


    def read_records_from_files(self):
        self.loggerv3.info('Reading records from files')
        filename = '/'.join([self.downloads_directory, self.file_name])
        if self.records_df is None:
            self.records_df = pd.read_csv(filename)
        else:
            df = pd.read_csv(filename)
            self.records_df = pd.concat([self.records_df, df])


    def clean_dataframe(self):
        self.loggerv3.info('Cleaning dataframe')
        # Convert headers to lowercase and remove spaces
        self.records_df.columns = self.records_df.columns.str.lower().str.replace(' ', '_', regex=False)

        # Convert Week Ending to Date
        self.records_df['week_ending'] = pd.to_datetime(self.records_df['week_ending'], format='%m/%d/%y')

        # Revenue to a float
        self.records_df["revenue"] = self.records_df["revenue"].str.replace(',', '')
        self.records_df["revenue"] = self.records_df["revenue"].astype(float)

        # Convert RPM to a float
        self.records_df["rpm"] = self.records_df["rpm"].astype(float)


    def write_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.records_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start("Running YouTube Channel Revenue Job")
        self.clear_downloads_directory()
        self.download_files_from_s3()
        self.read_records_from_files()
        self.clean_dataframe()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
