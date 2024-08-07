import os
from base.etl_jobv3 import EtlJobV3
from datetime import datetime
from pandas import pandas as pd
from utils.connectors.s3_api_connector import S3ApiConnector
from utils.components.backfill_by_dt import backfill_by_week


class PodcastTargetsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, table_name='podcast_targets')
        self.file_location = file_location
        self.target_date = target_date
        self.min_date = '2022-01-01'
        self.prefix = 'tubular-imports/targets/'
        self.s3_objects = None
        self.downloads_directory = self.file_location + 'downloads/tubular'
        self.prod_schema = 'warehouse'
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket='rt-data-youtube', profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.records_df = None
        self.start_weeks_df = None
        self.series_air_dates = []
        self.final_dataframe = None


    def clear_downloads_directory(self):
        self.loggerv3.info('Clearing downloads directory')
        if len(os.listdir(f'{self.downloads_directory}/')) > 1:
            os.system(f'rm {self.downloads_directory}/*.*')


    def get_filenames_from_s3(self):
        self.loggerv3.info('Getting filenames from s3')
        self.s3_objects = self.s3_api_connector.get_object_list_from_bucket(prefix=self.prefix)


    def download_files_from_s3(self):
        self.loggerv3.info('Downloading file from s3')
        for filename in self.s3_objects[1:]:
            key_no_prefix = filename.replace(self.prefix, '')
            full_file_path = '/'.join([self.downloads_directory, key_no_prefix])
            self.s3_api_connector.download_files_from_object(key=filename, filename=full_file_path)


    def quarter_dates_helper(self, quarter, year):
        if quarter == 'q1':
            start = '-'.join([year, '01', '01'])
            end = '-'.join([year, '03', '31'])
        elif quarter == 'q2':
            start = '-'.join([year, '04', '01'])
            end = '-'.join([year, '06', '30'])
        elif quarter == 'q3':
            start = '-'.join([year, '07', '01'])
            end = '-'.join([year, '09', '30'])
        elif quarter == 'q4':
            start = '-'.join([year, '10', '01'])
            end = '-'.join([year, '12', '31'])
        else:
            raise ValueError('File names in s3 are incorrectly formatted. Format should be qx-YYYY-targets.csv')

        return {
            'start': datetime.strptime(start, '%Y-%m-%d'),
            'end': datetime.strptime(end, '%Y-%m-%d'),
        }


    def read_records_from_files(self):
        self.loggerv3.info('Reading records from file')
        for filename in self.s3_objects[1:]:
            key_no_prefix = filename.replace(self.prefix, '')
            full_file_path = '/'.join([self.downloads_directory, key_no_prefix])
            quarter = key_no_prefix.split('-')[0]
            year = key_no_prefix.split('-')[1]
            start = self.quarter_dates_helper(quarter, year)['start']
            end = self.quarter_dates_helper(quarter, year)['end']
            if self.records_df is None:
                self.records_df = pd.read_csv(full_file_path, encoding='utf-8')
                self.records_df['quarter_start'] = start
                self.records_df['quarter_end'] = end
            else:
                df = pd.read_csv(full_file_path, encoding='utf-8')
                df['quarter_start'] = start
                df['quarter_end'] = end
                self.records_df = pd.concat([self.records_df, df])


    def identify_start_weeks(self):
        self.loggerv3.info('Identifying start weeks')
        start_weeks_list = backfill_by_week(latest_date=self.target_date, earliest_date=self.min_date, start_dow=1)
        self.start_weeks_df = pd.DataFrame(start_weeks_list, columns=['start_week'])


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = self.start_weeks_df.merge(self.records_df, how='cross')
        self.final_dataframe = self.final_dataframe[
            (self.final_dataframe['start_week'] >= self.final_dataframe['quarter_start']) & (self.final_dataframe['start_week'] <= self.final_dataframe['quarter_end'])
        ]
        self.final_dataframe = self.final_dataframe[['series_title', 'start_week', 'target']]


    def truncate_table(self):
        self.loggerv3.info('Truncating table')
        self.db_connector.write_redshift(f"TRUNCATE TABLE warehouse.{self.table_name}")


    def write_to_redshift(self):
        self.loggerv3.info('Writing to Redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start(f"Running Podcast Targets Job")
        self.clear_downloads_directory()
        self.get_filenames_from_s3()
        self.download_files_from_s3()
        self.read_records_from_files()
        self.identify_start_weeks()
        self.build_final_dataframe()
        self.truncate_table()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
