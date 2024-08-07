import os
from base.etl_jobv3 import EtlJobV3
from datetime import datetime
from pandas import pandas as pd
from utils.connectors.s3_api_connector import S3ApiConnector
from utils.components.extract_from_zip import ExtractFromZip


class MegaphoneMetricsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='megaphone_metrics')
        self.schema = 'warehouse'
        self.file_location = file_location
        self.downloads_directory = self.file_location + 'downloads/megaphone'
        self.extract_directory = self.file_location + 'extract'
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket='rt-megaphone', profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.target_date = target_date
        self.records = []
        self.base_file = f"metrics-day-{self.target_date}.json.gz"
        self.metrics_df = None


    def clear_downloads_directory(self):
        self.loggerv3.info('Clearing downloads directory')
        if len(os.listdir(f'{self.downloads_directory}/')) > 1:
            os.system(f'rm {self.downloads_directory}/*.*')


    def download_files_from_s3(self):
        self.loggerv3.info('Downloading file from s3')
        filename = '/'.join([self.downloads_directory, self.base_file])
        self.s3_api_connector.download_files_from_object(key=self.base_file, filename=filename)


    def extract_files_from_zips(self):
        self.loggerv3.info('Extracting files from zips')
        full_file_path = '/'.join([self.downloads_directory, self.base_file])
        extractor = ExtractFromZip(full_file_path=full_file_path,
                                   directory=self.extract_directory,
                                   extracted_file_name='metrics.json'
                                   )
        self.records.extend(extractor.execute())


    def build_dataframe(self):
        self.loggerv3.info("Building dataframe")
        metrics = []

        for record in self.records:
            metrics.append({
                "run_date": datetime.strptime(self.target_date, '%Y-%m-%d'),
                "id": record['id'],
                "created_at": record['created_at'],
                "normalized_user_agent": record['normalized_user_agent'],
                "podcast_id": record['podcast_id'],
                "episode_id": record['episode_id'],
                "seconds_downloaded": record['seconds_downloaded'],
                "duration": record['duration'],
                "source": record['source'],
                "city": record['city'],
                "country": record['country'],
                "region": record['region']
            })

        self.metrics_df = pd.DataFrame(metrics)


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.metrics_df, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, schema=self.schema)


    def execute(self):
        self.loggerv3.start(f"Running Megaphone Metrics Job for {self.target_date}")
        self.clear_downloads_directory()
        self.download_files_from_s3()
        self.extract_files_from_zips()
        self.build_dataframe()
        self.write_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
