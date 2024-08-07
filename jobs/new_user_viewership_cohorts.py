import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.batcher import Batcher
from utils.components.sql_helper import SqlHelper


class NewUserViewershipCohorts(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector=None, file_location=''):
        super().__init__(target_date = target_date, jobname =  __name__, db_connector = db_connector, table_name = 'new_user_viewership_cohorts_v2')
        self.target_date = target_date
        self.batcher = Batcher()
        self.sql_helper = SqlHelper()
        self.new_users = []
        self.new_user_batches = []
        self.series_viewership_df = None
        self.final_dataframe = None
        self.loggerv3.alert = False


    def get_new_users(self):
        self.loggerv3.info('Getting new users')
        results = self.db_connector.read_redshift(f"""
                                                    WITH last_14_days as (
                                                        SELECT distinct user_key
                                                        FROM warehouse.vod_viewership
                                                        WHERE user_key is not null
                                                          AND user_tier in ('premium', 'trial', 'free')
                                                          AND start_timestamp >= dateadd('days', -13, '{self.target_date}')
                                                          AND start_timestamp < dateadd('days', 1, '{self.target_date}')
                                                          AND max_position > 0
                                                    ), previous_viewers as (
                                                        SELECT distinct user_key
                                                        FROM warehouse.vod_viewership
                                                        WHERE user_key is not null
                                                          AND user_tier in ('premium', 'trial', 'free')
                                                          AND start_timestamp < dateadd('days', -13, '{self.target_date}')
                                                    ) SELECT
                                                        distinct user_key
                                                      FROM last_14_days
                                                      WHERE user_key not in (SELECT user_key FROM previous_viewers); 
                                                """
                                                   )
        for result in results:
            self.new_users.append(str(result[0]))


    def batch_new_users(self):
        self.loggerv3.info('Batching new users')
        self.new_user_batches = self.batcher.list_to_list_batch(batch_limit=500, iterator=self.new_users)


    def get_viewership_per_batch(self, batch):
        where_in = self.sql_helper.array_to_sql_list(batch)
        batch_series_viewership = []
        results = self.db_connector.read_redshift(f"""
                                                    SELECT 
                                                        dse.series_id,
                                                        dse.series_title,
                                                        count(distinct vv.user_key)
                                                    FROM warehouse.vod_viewership vv
                                                    INNER JOIN warehouse.dim_segment_episode dse on vv.episode_key = dse.episode_key
                                                    WHERE 
                                                        vv.user_key in ({where_in}) AND
                                                        vv.user_tier in ('trial', 'premium', 'free') AND 
                                                        vv.start_timestamp >= dateadd('days', -13, '{self.target_date}') AND 
                                                        vv.start_timestamp < dateadd('days', 1, '{self.target_date}') AND 
                                                        vv.max_position > 0
                                                    GROUP BY 1, 2;
                                                """
                                                   )

        for result in results:
            batch_series_viewership.append({
                'series_id': result[0],
                'series_title': result[1],
                'new_viewers': result[2]
            })

        return pd.DataFrame(batch_series_viewership)


    def get_viewership(self):
        self.loggerv3.info('Getting new user viewership')
        for batch in self.new_user_batches:
            batch_viewership_df = self.get_viewership_per_batch(batch)
            if self.series_viewership_df is None:
                self.series_viewership_df = batch_viewership_df
            else:
                self.series_viewership_df = pd.concat([self.series_viewership_df, batch_viewership_df], axis=0)


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = self.series_viewership_df.groupby(['series_id', 'series_title'])['new_viewers'].agg('sum').reset_index()
        self.final_dataframe['run_date'] = self.target_date


    def write_to_redshift(self):
        self.loggerv3.info('Writing to redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)



    def execute(self):
        self.loggerv3.start(f"Running New User Viewership Cohorts for {self.target_date}")
        self.get_new_users()
        self.batch_new_users()
        self.get_viewership()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
