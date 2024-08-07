import pandas as pd
from base.etl_jobv3 import EtlJobV3


class YouTubeRTMembersJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, file_location=''):
        super().__init__(jobname = __name__, db_connector = db_connector, table_name = 'youtube_rt_members')
        self.prod_schema = 'warehouse'
        self.staging_schema = 'staging'
        self.users_df = None


    def get_v2_users(self):
        self.loggerv3.info('Get V2 Users')
        users = []
        results = self.db_connector.query_v2_db(f"""
        SELECT id as user_key,
               sponsor_type,
               sponsorship_starts_at,
               sponsorship_ends_at
        FROM users
        WHERE sponsor_type in ('death_battle_youtube', 'funhaus_youtube');
        """)

        for result in results:
            users.append({
                'user_key': result[0],
                'sponsor_type': result[1],
                'sponsorship_starts_at': result[2],
                'sponsorship_ends_at': result[3]
                })
        self.users_df = pd.DataFrame(users)


    def write_to_redshift_staging(self):
        self.loggerv3.info('Writing to Redshift staging')
        self.db_connector.write_to_sql(self.users_df, f'stage_{self.table_name}', self.db_connector.sv2_engine(), schema=self.staging_schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(f'stage_{self.table_name}', self.staging_schema)


    def merge_stage_to_prod(self):
        self.loggerv3.info('Merging staging to prod')
        query = f"""
               BEGIN TRANSACTION;

                   DELETE FROM {self.staging_schema}.stage_{self.table_name}
                   USING {self.prod_schema}.{self.table_name} prod
                   WHERE 
                       prod.user_key = stage_{self.table_name}.user_key
                       AND prod.sponsor_type = stage_{self.table_name}.sponsor_type
                       AND prod.sponsorship_starts_at = stage_{self.table_name}.sponsorship_starts_at
                       AND prod.sponsorship_ends_at = stage_{self.table_name}.sponsorship_ends_at;

                   INSERT INTO {self.prod_schema}.{self.table_name}
                   SELECT * FROM {self.staging_schema}.stage_{self.table_name};

                   TRUNCATE {self.staging_schema}.stage_{self.table_name};

                   COMMIT;

               END TRANSACTION;
           """
        self.db_connector.write_redshift(query)


    def execute(self):
        self.loggerv3.start(f"YouTube RT Members Job")
        self.get_v2_users()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
