import os
import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta
from utils.connectors.s3_api_connector import S3ApiConnector


class YouTubeWriteReportingJob(EtlJobV3):

    def __init__(self, backfill = False, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, target_date=target_date)
        self.target_date = target_date  # Report Create Time from yt_job_history
        self.prod_schema = 'warehouse'
        self.staging_schema = 'staging'
        self.prod_table = 'content_owner_combined_a2'
        self.staging_table = 'stage_content_owner_combined_a2'
        self.bucket_name = 'rt-datapipeline'
        self.s3_folder = 'raw/yt-reporting'
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket=self.bucket_name, profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.downloads_directory = self.file_location + 'downloads/youtube'
        self.file_names = []
        self.records = []
        self.start_date = None
        self.end_date = None
        self.final_dataframe = None
        self.DECIMAL_PRECISION = 4


    def set_dates(self):
        if self.target_date is None:
            self.start_date = datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d') + 'T00:00:00Z'
            self.end_date = datetime.strftime(datetime.now(), '%Y-%m-%d') + 'T00:00:00Z'
        else:
            target_date_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
            self.start_date = datetime.strftime(target_date_dt, '%Y-%m-%d') + 'T00:00:00Z'
            self.end_date = datetime.strftime(target_date_dt + timedelta(days=1), '%Y-%m-%d') + 'T00:00:00Z'
        self.loggerv3.info(f"Job run from {self.start_date} to {self.end_date}")


    def clean_up(self):
        self.loggerv3.info('Cleaning up files in local dir')
        if len(os.listdir(f'{self.downloads_directory}/')) > 0:
            os.system(f'rm {self.downloads_directory}/*.*')


    def get_yt_job_history(self):
        self.loggerv3.info('Getting YouTube job history')
        query = f"""
            SELECT
                job_name,
                report_create_time,
                object_path
            FROM {self.prod_schema}.yt_job_history
            WHERE 
                report_create_time >= '{self.start_date}' 
                AND report_create_time < '{self.end_date}' 
                AND content_owner_name = 'RT'
                AND job_name = 'content_owner_combined_a2' 
                AND job_status='STARTED'            
            GROUP BY 1, 2, 3;
        """

        results = self.db_connector.read_redshift(query)
        for result in results:
            record = {
                "job_name": result[0],
                "report_create_time": result[1],
                "object_path": result[2]
            }
            self.records.append(record)

        if len(self.records) == 0:
            raise ValueError(f'No jobs were found in yt_job_history for {self.start_date} to {self.end_date}')


    def download_from_s3(self):
        self.loggerv3.info('Downloading files from s3')
        for idx, record in enumerate(self.records):
            self.loggerv3.info(f"Downloading records: {idx + 1}/{len(self.records)}")
            key = record['object_path']
            file_name = key.split('/')[-1]
            full_file_name = '/'.join([self.downloads_directory, file_name])
            self.s3_api_connector.download_files_from_object(key, full_file_name)


    def read_downloaded_files(self):
        self.loggerv3.info('Read downloaded files to dataframe')
        for record in self.records:
            key = record['object_path']
            file_name = key.split('/')[-1]
            full_file_name = '/'.join([self.downloads_directory, file_name])
            df = pd.read_csv(full_file_name)
            if self.final_dataframe is None:
                self.final_dataframe = df
            else:
                self.final_dataframe = pd.concat([self.final_dataframe, df])


    def clean_dataframe(self):
        self.loggerv3.info('Cleaning dataframe')
        self.final_dataframe.rename({'date': 'start_date'}, axis=1, inplace=True)
        # The country code ZZ is used to report metrics for which YouTube could not identify the associated country
        self.final_dataframe['country_code'].fillna('ZZ', inplace=True)
        # Claimed status cannot be null. If null, we set as "unknown"
        self.final_dataframe['claimed_status'].fillna('unknown', inplace=True)
        # Round floats to not have numeric overflow error from Redshift
        self.final_dataframe['watch_time_minutes'] = self.final_dataframe['watch_time_minutes'].round(self.DECIMAL_PRECISION)
        self.final_dataframe['average_view_duration_seconds'] = self.final_dataframe['average_view_duration_seconds'].round(self.DECIMAL_PRECISION)
        self.final_dataframe['average_view_duration_percentage'] = self.final_dataframe['average_view_duration_percentage'].round(self.DECIMAL_PRECISION)
        self.final_dataframe['red_watch_time_minutes'] = self.final_dataframe['red_watch_time_minutes'].round(self.DECIMAL_PRECISION)
        # Drop Duplicates
        self.final_dataframe = self.final_dataframe.drop_duplicates()


    def write_to_redshift_staging(self):
        self.loggerv3.info('Writing to Redshift staging')
        self.db_connector.write_to_sql(self.final_dataframe, self.staging_table, self.db_connector.sv2_engine(), schema=self.staging_schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.staging_table, self.staging_schema)


    def update_yt_job_history(self):
        self.loggerv3.info('Updating YouTube job history')
        for record in self.records:
            query = f"""
                    UPDATE {self.prod_schema}.yt_job_history
                    SET job_status='COPIED TO RS STAGE'
                    WHERE job_name='{record['job_name']}' AND object_path='{record['object_path']}';
                """
            self.db_connector.write_redshift(query)


    def merge_stage_to_prod(self):
        self.loggerv3.info('Merging staging to prod')
        query = f"""
            BEGIN TRANSACTION;
            
                UPDATE {self.prod_schema}.{self.prod_table}
                SET 
                    views = staging.views,
                    watch_time_minutes = staging.watch_time_minutes,
                    average_view_duration_seconds = staging.average_view_duration_seconds,
                    average_view_duration_percentage = staging.average_view_duration_percentage,
                    red_views = staging.red_views,
                    red_watch_time_minutes = staging.red_watch_time_minutes
                FROM {self.staging_schema}.{self.staging_table} staging
                JOIN {self.prod_schema}.{self.prod_table} prod
                    ON staging.start_date = prod.start_date
                    AND staging.channel_id = prod.channel_id
                    AND staging.video_id = prod.video_id
                    AND staging.claimed_status = prod.claimed_status
                    AND staging.uploader_type = prod.uploader_type
                    AND staging.live_or_on_demand = prod.live_or_on_demand
                    AND staging.subscribed_status = prod.subscribed_status
                    AND staging.country_code = prod.country_code
                    AND staging.playback_location_type = prod.playback_location_type
                    AND staging.traffic_source_type = prod.traffic_source_type
                    AND staging.device_type = prod.device_type
                    AND staging.operating_system = prod.operating_system;
    
                DELETE FROM {self.staging_schema}.{self.staging_table}
                USING {self.prod_schema}.{self.prod_table} prod
                WHERE 
                    prod.start_date = {self.staging_table}.start_date
                    AND prod.channel_id = {self.staging_table}.channel_id
                    AND prod.video_id = {self.staging_table}.video_id
                    AND prod.claimed_status = {self.staging_table}.claimed_status
                    AND prod.uploader_type = {self.staging_table}.uploader_type
                    AND prod.live_or_on_demand = {self.staging_table}.live_or_on_demand
                    AND prod.subscribed_status = {self.staging_table}.subscribed_status
                    AND prod.country_code  = {self.staging_table}.country_code
                    AND prod.playback_location_type = {self.staging_table}.playback_location_type
                    AND prod.traffic_source_type = {self.staging_table}.traffic_source_type
                    AND prod.device_type = {self.staging_table}.device_type
                    AND prod.operating_system = {self.staging_table}.operating_system;
    
                INSERT INTO {self.prod_schema}.{self.prod_table}
                SELECT * FROM {self.staging_schema}.{self.staging_table};
    
                TRUNCATE {self.staging_schema}.{self.staging_table};
    
                COMMIT;
                
            END TRANSACTION;
        """
        self.db_connector.write_redshift(query)


    def execute(self):
        self.loggerv3.start(f"Running YouTube Write Reporting Job")
        self.set_dates()
        self.clean_up()
        self.get_yt_job_history()
        self.download_from_s3()
        self.read_downloaded_files()
        self.clean_dataframe()
        self.write_to_redshift_staging()
        self.update_yt_job_history()
        self.clean_up()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
