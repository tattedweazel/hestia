import os
import re
from base.etl_jobv3 import EtlJobV3
from pandas import pandas as pd
from utils.connectors.s3_api_connector import S3ApiConnector


class DailyTubularMetricsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='daily_tubular_metrics')
        """This job also updates dim_tubular_videos table"""
        self.run_date = target_date
        self.file_location = file_location
        self.rt_bucket = 'rt-data-youtube'
        self.prefix = 'tubular-imports/'
        self.prod_schema = 'warehouse'
        self.staging_schema = 'staging'
        self.downloads_directory = self.file_location + 'downloads/tubular'
        self.files = [f"report1-{self.run_date}.csv", f"report2-{self.run_date}.csv", f"report3-{self.run_date}.csv"]
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket=self.rt_bucket, profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.dim_columns = ['video_id', 'video_title', 'date_of_upload', 'upload_creator', 'run_date',  'duration_(secs)']
        self.dropped_columns = ['creator_subscriber/follower_count', 'url',  'platform']
        self.records_df = None
        self.records = []
        self.dim_df = None
        self.normalized_records = []
        self.final_dataframe = None


    def clear_downloads_directory(self):
        self.loggerv3.info('Clearing downloads directory')
        if len(os.listdir(f'{self.downloads_directory}/')) > 1:
            os.system(f'rm {self.downloads_directory}/*.*')


    def download_files_from_s3(self):
        self.loggerv3.info('Downloading files from s3')
        for file in self.files:
            key = self.prefix + file
            filename = '/'.join([self.downloads_directory, file])
            self.s3_api_connector.download_files_from_object(key=key, filename=filename)


    def read_records_from_files(self):
        self.loggerv3.info('Reading records from files')
        for file in self.files:
            filename = '/'.join([self.downloads_directory, file])
            if self.records_df is None:
                self.records_df = pd.read_csv(filename, skiprows=2)
            else:
                df = pd.read_csv(filename, skiprows=2)
                self.records_df = pd.concat([self.records_df, df])


    def clean_dataframe(self):
        self.loggerv3.info('Cleaning dataframe')
        # Convert headers to lowercase and remove spaces
        self.records_df.columns = self.records_df.columns.str.lower().str.replace(' ', '_', regex=False)
        # Add video_id
        self.records_df['video_id'] = self.records_df['url'].str.replace('https://www.youtube.com/watch?v=', '', regex=False)
        # Add run date
        self.records_df['run_date'] = self.run_date
        # Drop Columns
        self.records_df.drop(self.dropped_columns, axis=1, inplace=True)
        # Make index first columns
        self.records_df.set_index(self.dim_columns, inplace=True)
        self.records_df = self.records_df.reset_index()
        # Drop Duplicates
        self.records_df.drop_duplicates(inplace=True)
        # Rename columns
        self.records_df.rename(columns={'duration_(secs)': 'duration_seconds'}, inplace=True)


    def write_to_dim_redshift_staging(self):
        self.loggerv3.info('Writing to dim redshift staging')
        self.dim_df = self.records_df[['video_id', 'video_title', 'date_of_upload', 'upload_creator', 'duration_seconds']]
        self.dim_df = self.dim_df.drop_duplicates()
        self.db_connector.write_to_sql(self.dim_df, 'stage_dim_tubular_videos', self.db_connector.sv2_engine(), schema=self.staging_schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions('stage_dim_tubular_videos', self.staging_schema)


    def merge_dim_stage_to_prod(self):
        self.loggerv3.info('Merging dim staging to prod')
        query = f"""
               BEGIN TRANSACTION;

                   DELETE FROM {self.staging_schema}.stage_dim_tubular_videos
                   USING {self.prod_schema}.dim_tubular_videos prod
                   WHERE 
                       prod.video_id = stage_dim_tubular_videos.video_id;

                   INSERT INTO {self.prod_schema}.dim_tubular_videos
                   SELECT * FROM {self.staging_schema}.stage_dim_tubular_videos;

                   TRUNCATE {self.staging_schema}.stage_dim_tubular_videos;

                   COMMIT;

               END TRANSACTION;
           """
        self.db_connector.write_redshift(query)


    def clean_views_helper(self, value):
        if isinstance(value, int):
            return value
        elif value.strip() == '-':
            return 0
        else:
            return int(value)


    def normalize_dataframe(self):
        self.loggerv3.info('Normalizing dataframe')
        # Convert Dataframe to list of dicts
        self.records = self.records_df.to_dict('records')
        # Create normalized dataframe
        for record in self.records:
            new_record = {}
            for key, value in record.items():
                if key in self.dim_columns:
                    new_record[key] = value
                else:
                    if len(re.findall(r'\d+', key)) > 0:
                        if 'views' in key:
                            new_record['agg_type'] = 'cumulative'
                            new_record['date_value'] = int(re.findall(r'\d+', key)[0])
                            new_record['views'] = self.clean_views_helper(value)
                        else:
                            new_record['agg_type'] = 'daily'
                            new_record['date_value'] = int(re.findall(r'\d+', key)[0])
                            new_record['views'] = self.clean_views_helper(value)
                    else:
                        new_record['agg_type'] = 'all_time'
                        new_record['date_value'] = 0
                        new_record['views'] = self.clean_views_helper(value)
                    self.normalized_records.append(new_record.copy())


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.normalized_records)
        self.final_dataframe = self.final_dataframe[self.final_dataframe['views'] > 0]
        self.final_dataframe = self.final_dataframe[['video_id', 'run_date', 'agg_type', 'date_value', 'views']]
        self.final_dataframe.drop_duplicates(inplace=True)


    def write_to_redshift_staging(self):
        self.loggerv3.info('Writing to Redshift staging')
        self.db_connector.write_to_sql(self.final_dataframe, f'stage_{self.table_name}', self.db_connector.sv2_engine(), schema=self.staging_schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(f'stage_{self.table_name}', self.staging_schema)


    def merge_stage_to_prod(self):
        self.loggerv3.info('Merging staging to prod')
        query = f"""
               BEGIN TRANSACTION;

                   UPDATE {self.prod_schema}.{self.table_name}
                   SET 
                       views = staging.views,
                       run_date = staging.run_date
                   FROM {self.staging_schema}.stage_{self.table_name} staging
                   JOIN {self.prod_schema}.{self.table_name} prod
                       ON staging.video_id = prod.video_id
                       AND staging.agg_type = prod.agg_type
                       AND staging.date_value = prod.date_value
                       AND staging.run_date > prod.run_date
                       AND staging.views != prod.views;

                   DELETE FROM {self.staging_schema}.stage_{self.table_name}
                   USING {self.prod_schema}.{self.table_name} prod
                   WHERE 
                       prod.video_id = stage_{self.table_name}.video_id
                       AND prod.agg_type = stage_{self.table_name}.agg_type
                       AND prod.date_value = stage_{self.table_name}.date_value
                       AND prod.views = stage_{self.table_name}.views;

                   INSERT INTO {self.prod_schema}.{self.table_name}
                   SELECT * FROM {self.staging_schema}.stage_{self.table_name};

                   TRUNCATE {self.staging_schema}.stage_{self.table_name};

                   COMMIT;

               END TRANSACTION;
           """
        self.db_connector.write_redshift(query)


    def execute(self):
        self.loggerv3.start(f"Running Daily Tubular Metrics Job for {self.run_date}")
        self.clear_downloads_directory()
        self.download_files_from_s3()
        self.read_records_from_files()
        self.clean_dataframe()
        self.write_to_dim_redshift_staging()
        self.merge_dim_stage_to_prod()
        self.normalize_dataframe()
        self.build_final_dataframe()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
