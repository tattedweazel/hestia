import pandas as pd
from base.etl_jobv3 import EtlJobV3


class DimContentBlocksJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, file_location=''):
        super().__init__(jobname = __name__, db_connector = db_connector, table_name = 'dim_content_blocks')
        self.records = []
        self.records_df = None
        self.staging_schema = 'staging'
        self.prod_schema = 'warehouse'


    def get_content_block_dim_data(self):
        self.loggerv3.info('Getting content block dim data')
        query = f"""
            SELECT
                uuid,
                (CASE
                    WHEN item_use_type is NOT NULL AND length(item_use_type) > 0 THEN item_use_type
                    WHEN title_2 is NOT NULL AND length(title_2) > 0 THEN title_2
                    ELSE name
                END) as title,
                lower((CASE
                    WHEN item_use_type is NOT NULL AND length(item_use_type) > 0 THEN item_use_type
                    WHEN title_2 is NOT NULL AND length(title_2) > 0 THEN title_2
                    ELSE name
                END)) as title_clean,
                position,
                created_at,
                updated_at
            FROM content_blocks;
        """
        results = self.db_connector.query_svod_be_db_connection(query)

        for result in results:
            self.records.append({
                'uuid': result[0],
                'title': result[1],
                'title_clean': result[2],
                'position': result[3],
                'created_at': result[4],
                'updated_at': result[5]
                })
        self.records_df = pd.DataFrame(self.records)


    def write_to_redshift_staging(self):
        self.loggerv3.info('Writing to Redshift staging')
        self.db_connector.write_to_sql(self.records_df, f'stage_{self.table_name}', self.db_connector.sv2_engine(), schema=self.staging_schema, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(f'stage_{self.table_name}', self.staging_schema)


    def merge_stage_to_prod(self):
        self.loggerv3.info('Merging staging to prod')
        query = f"""
                  BEGIN TRANSACTION;
                  
                      DELETE FROM {self.staging_schema}.stage_{self.table_name}
                      USING {self.prod_schema}.{self.table_name} prod
                      WHERE 
                           prod.uuid = stage_{self.table_name}.uuid
                           AND prod.created_at = stage_{self.table_name}.created_at
                           AND prod.position = stage_{self.table_name}.position;

                      INSERT INTO {self.prod_schema}.{self.table_name}
                      SELECT * FROM {self.staging_schema}.stage_{self.table_name};

                      TRUNCATE {self.staging_schema}.stage_{self.table_name};

                      COMMIT;

                  END TRANSACTION;
              """
        self.db_connector.write_redshift(query)


    def execute(self):
        self.loggerv3.start(f"Running Dim Content Blocks Job")
        self.get_content_block_dim_data()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
