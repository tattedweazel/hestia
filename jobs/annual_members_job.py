import json
from time import sleep
import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.batcher import Batcher
from utils.connectors.s3_api_connector import S3ApiConnector


class AnnualMembersJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='cs_annual_members')
        """DELETE DATA TABLE & S3 FOLDER WHEN DONE"""
        self.target_date = target_date
        self.start_date = '2023-11-20'
        self.output_filename = f'{self.target_date}.csv'
        self.output_directory = self.file_location + 'downloads/annual_members'
        self.key = f'customer_service_annual_members/{self.output_filename}'
        self.rt_bucket = 'rt-data-misc'
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket=self.rt_bucket, profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.existing_members = []
        self.member_subs = []
        self.member_emails = []
        self.batcher = Batcher()
        self.batches = None
        self.members_df = None


    def read_existing(self):
        self.loggerv3.info('Reading existing')
        query = f"""
            SELECT user_uuid
            FROM warehouse.cs_annual_members;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.existing_members.append(result[0])


    def get_subs(self):
        self.loggerv3.info('Getting annual member subs')
        query = f"""
            SELECT
                user_uuid,
                started_at
            FROM user_subscriptions
            WHERE
                plan_code = '1year' AND
                state = 'paying' AND
                current_period_started_at >= '{self.start_date}' AND
                current_period_started_at = started_at
        """
        results = self.db_connector.query_business_service_db_connection(query)
        for result in results:
            if result[0] not in self.existing_members:
                self.member_subs.append({
                    'user_uuid': result[0],
                    'started_at': result[1]
                })


    def get_emails(self):
        self.loggerv3.info('Getting annual member emails')
        user_uuids = [member['user_uuid'] for member in self.member_subs]
        self.batches = self.batcher.list_to_list_batch(batch_limit=50, iterator=user_uuids)
        for batch in self.batches:
            id_batch = ','.join([f"'{x}'" for x in batch])
            query = f"""
                SELECT
                    uuid as user_uuid,
                    email
                FROM users
                WHERE uuid in ({id_batch})
                    AND sponsor_type = 'recurly';
            """
            results = self.db_connector.query_v2_db(query)
            for result in results:
                self.member_emails.append({
                    'user_uuid': result[0],
                    'email': result[1]
                })


    def build_final_dataframes(self):
        self.loggerv3.info('Building final dataframes')
        member_subs_df = pd.DataFrame(self.member_subs)
        member_emails_df = pd.DataFrame(self.member_emails)
        self.members_df = member_subs_df.merge(member_emails_df, how='inner', on='user_uuid')
        self.members_df['run_date'] = self.target_date


    def write_to_redshift(self):
        self.loggerv3.info('Writing to redshift')
        self.db_connector.write_to_sql(self.members_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def upload_to_s3(self):
        self.loggerv3.info("Uploading to S3")
        full_path = '/'.join([self.output_directory, self.output_filename])
        self.members_df.to_csv(full_path, sep=',', encoding='utf-8')
        sleep(10)
        self.s3_api_connector.upload_file(full_path, self.key)


    def execute(self):
        self.loggerv3.start(f"Running Annual Members Job for {self.target_date}")
        self.read_existing()
        self.get_subs()
        self.get_emails()
        self.build_final_dataframes()
        self.write_to_redshift()
        self.upload_to_s3()
        self.loggerv3.success("All Processing Complete!")
