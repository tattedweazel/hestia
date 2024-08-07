import tempfile
import os
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta
from googleapiclient.http import MediaIoBaseDownload
from time import sleep
from utils.connectors.s3_api_connector import S3ApiConnector
from utils.connectors.youtube_api_connector import YouTubeApiConnector



class YouTubeDownloadReportingJob(EtlJobV3):

    def __init__(self, backfill = False, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector)
        self.schema = 'warehouse'
        self.backfill = False
        self.youtube_api_connector = YouTubeApiConnector(file_location=self.file_location, service='reporting')
        self.bucket_name = 'rt-datapipeline'
        self.s3_folder = 'raw/yt-reporting'
        self.export_format = 'csv'
        self.chunk_size: int = 10 * 1024 * 1024
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket=self.bucket_name, profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.reporting_api = None
        self.report_ids = ['content_owner_combined_a2']
        self.created_after = datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d') + 'T00:00:00Z'
        self.jobs = []
        self.reports = []
        self.reports_df = None
        self.SLEEP_TIME = 30


    def instatiate_reporting_api(self):
        self.loggerv3.info('Instatiating reporting API')
        self.reporting_api = self.youtube_api_connector.get_service_api()


    def get_current_jobs(self):
        self.loggerv3.info('Getting current jobs')
        for content_owner_name, content_id in self.youtube_api_connector.content_ids.items():
            responses = self.reporting_api.jobs().list(onBehalfOfContentOwner=content_id).execute()['jobs']
            for response in responses:
                if response['reportTypeId'] in self.report_ids:
                    self.jobs.append({
                        'job_id': response['id'],
                        'job_name': response['reportTypeId'],
                        'content_id': content_id,
                        'content_owner_name': content_owner_name
                    })


    def get_job_reports(self):
        self.loggerv3.info('Getting job reports')
        for job in self.jobs:
            if self.backfill is True:
                responses = self.reporting_api.jobs().reports().list(jobId=job['job_id'], onBehalfOfContentOwner=job['content_id']).execute()['reports']
            else:
                responses = self.reporting_api.jobs().reports().list(jobId=job['job_id'], onBehalfOfContentOwner=job['content_id'], createdAfter=self.created_after).execute()['reports']

            for response in responses:
                self.reports.append({
                    'job_id': job['job_id'],
                    'job_name': job['job_name'],
                    'content_owner_name': job['content_owner_name'],
                    'content_id': job['content_id'],
                    'report_id': response['id'],
                    'report_start_time': response['startTime'],
                    'report_end_time': response['endTime'],
                    'report_create_time': response['createTime'],
                    'download_url': response['downloadUrl']
                })


    def download_report_to_s3(self):
        self.loggerv3.info('Download reports to S3')
        for idx, report in enumerate(self.reports):
            report_start_date = report['report_start_time'][:report['report_start_time'].find('T')]
            report_end_date = report['report_end_time'][:report['report_end_time'].find('T')]
            report_date = datetime.strptime(report_start_date, '%Y-%m-%d')
            self.loggerv3.info(f"Report {idx+1} of {len(self.reports)}: {report['content_owner_name']} - {report_start_date}")
            report_url = self.reporting_api.jobs().reports().get(jobId=report['job_id'],
                                                                 reportId=report['report_id'],
                                                                 onBehalfOfContentOwner=report['content_id']
                                                                 ).execute()['downloadUrl']
            request = self.reporting_api.media().download(resourceName="")
            request.uri = report_url
            path = f"""{self.s3_folder.lower()}/{report['content_owner_name']}/{report_date.year}/{report_date.month}/{report_date.day}/{report['job_name'].lower()}_{report['report_create_time']}.{self.export_format.lower()}"""

            with tempfile.NamedTemporaryFile(prefix=report['job_name'], suffix='.' + self.export_format, delete=False) as temp_file:
                downloader = MediaIoBaseDownload(fd=temp_file, request=request, chunksize=self.chunk_size)
                download_finished = False
                while download_finished is False:
                    status, download_finished = downloader.next_chunk()

                temp_file.flush()
                self.s3_api_connector.put_object(file_object=open(temp_file.name, 'rb'), key=path)
                os.unlink(temp_file.name)

            insert_query = f"""
                INSERT INTO {self.schema}.yt_job_history (
                    job_id,
                    job_name,
                    report_start_date,
                    report_end_date,
                    report_create_time,
                    object_path, 
                    job_created_time, 
                    job_frequency, 
                    job_status,
                    content_owner_name
                ) VALUES (
                    '{report['job_id']}',
                    '{report['job_name']}',
                    '{report_start_date}',
                    '{report_end_date}',
                    '{report['report_create_time']}',
                    '{path}',
                    '{datetime.now()}',
                    'DAILY',
                    'STARTED',
                    '{report['content_owner_name']}'
                );
            """
            self.db_connector.write_redshift(insert_query)
            sleep(self.SLEEP_TIME)


    def execute(self):
        self.loggerv3.start(f'Running YouTube Download Reporting Job')
        self.instatiate_reporting_api()
        self.get_current_jobs()
        self.get_job_reports()
        self.download_report_to_s3()
        self.loggerv3.success("All Processing Complete!")
