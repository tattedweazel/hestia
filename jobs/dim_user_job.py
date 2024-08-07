import pandas as pd
from base.etl_jobv3 import EtlJobV3
from io import BytesIO
from utils.connectors.s3_api_connector import S3ApiConnector


class DimUserJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, file_location=''):
        super().__init__(jobname = __name__, db_connector = db_connector, table_name = 'dim_user')
        self.users_df = None
        self.schema = 'warehouse'
        self.bucket = 'airflow-rt-data'
        self.prefix = 'processed/dim_user'
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket=self.bucket, profile_name='default', dry_run=self.db_connector.dry_run)


    def get_v2_users(self):
        self.loggerv3.info('Get V2 Users')
        users = []
        results = self.db_connector.query_v2_db(f"""
            SELECT
                id as user_key,
                uuid as user_id,
                created_at
            FROM users;
        """)

        for result in results:
            users.append({
                'user_key': result[0],
                'user_id': result[1],
                'created_at': result[2]
                })
        self.users_df = pd.DataFrame(users)


    def clear_dim_table(self):
        self.loggerv3.info('Truncating table')
        self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.table_name}")


    def write_all_results_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.users_df, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, schema=self.schema)


    def write_all_results_to_s3(self):
        """Must write as parquet file"""
        self.loggerv3.info('Writing results to s3 as parquet')
        out_buffer = BytesIO()
        self.users_df.to_parquet(out_buffer, index=False)
        key = '/'.join([self.prefix, 'dim_users.parquet'])
        self.s3_api_connector.put_object(out_buffer.getvalue(), key)


    def execute(self):
        self.loggerv3.start(f"Running Dim User Job")
        self.get_v2_users()
        self.clear_dim_table()
        self.write_all_results_to_redshift()
        self.write_all_results_to_s3()
        self.loggerv3.success("All Processing Complete!")
