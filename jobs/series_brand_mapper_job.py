import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.connectors.s3_api_connector import S3ApiConnector


class SeriesBrandMapperJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, table_name = 'series_brand_mappings')
        self.target_date = target_date
        self.downloads_directory = 'downloads/dsp'
        self.file_name = 'series_brand_mappings.csv'
        self.final_dataframe = None
        self.rt_bucket = 'rt-data-misc'
        self.prefix = 'series_brand_mappings/'
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket=self.rt_bucket, profile_name='roosterteeth')


    def download_files_from_s3(self):
        self.loggerv3.info('Downloading files from s3')
        key = self.prefix + self.file_name
        filename = '/'.join([self.downloads_directory, self.file_name])
        self.s3_api_connector.download_files_from_object(key=key, filename=filename)


    def read_mappings(self):
        self.loggerv3.info('Reading mappings')
        full_file_path = '/'.join([self.downloads_directory, self.file_name])
        self.final_dataframe = pd.read_csv(full_file_path)


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe['month'] = self.target_date
        self.final_dataframe['channel_title'] = self.final_dataframe['channel_title'].str.lower()
        self.final_dataframe['series_title'] = self.final_dataframe['series_title'].str.lower()
        self.final_dataframe['brand'] = self.final_dataframe['brand'].str.lower()
        self.final_dataframe.drop_duplicates(inplace=True)


    def write_to_redshift(self):
        self.loggerv3.info("Writing results to Redshift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start(f"Running Series Brand Mapper Job for {self.target_date}")
        self.download_files_from_s3()
        self.read_mappings()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
