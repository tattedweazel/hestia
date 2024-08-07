import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.batcher import Batcher
from utils.components.sql_helper import SqlHelper


class NewViewershipBySeasonSeriesJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector=None, file_location=''):
        super().__init__(target_date = target_date, jobname =  __name__, db_connector = db_connector, table_name = 'new_viewership_season_series')
        self.target_date = target_date
        self.batcher = Batcher()
        self.sql_helper = SqlHelper()
        self.BATCH_LIMIT = 2000
        self.series_ids = ['b6455f6a-ccff-4522-afc0-ca7903fdfd8c', 'e965a532-f473-49d4-a7ca-8454b4f78401', 'b229ba6d-0b3d-4414-a762-11496d438fff', '8a041287-e7ef-4388-9718-5288e62307fb', '8edb45ff-1cfc-4ef9-b451-9cec4dc372b5', '98342dec-562f-4ac2-afc6-509cffd635e4', '9e89b263-ddb6-48e8-bf2d-2942967749ba', 'a7d17604-f4e8-4b3b-b151-ab7eda53654a', '6b820054-2316-46d6-90e8-c389d2b8a532', '819bbd06-45f7-44de-bd75-56bbc08343bc', '2403115e-92ec-4b9c-965a-eccd41acb5ba', '014a9f01-de40-4c15-9b65-0e009bf90861', '7230ebba-58bf-4fd9-9866-e12d119c49f7', '1c1320ef-d12f-4797-bede-10c8dc9d7ecd', '8552d8d5-55a0-469f-94ca-d659031140ff', 'f58a23d8-7d0b-4e86-ada8-8b3f902184bd', 'd785786d-1b09-4e7e-bf34-bc4cdbb1b09a', '9409f352-23fd-456b-b532-bc956fba4b42', '2b8b0986-c3d1-42ea-8e61-9453305260b0', 'c9110758-e436-4da5-95a4-b44f11d1c86e']
        """Murder room (new season), Ah wrestling, Last laugh 2, Camp betrayal, Neon konbini, Red vs blue family shatters, Let's play gmod 2022 and 2021, Red web (2022 and 2021), Black box down (2022 and 2021), F**kface (2022 and 2021), Let's Play Minecraft (2022 and 2021), Let's Firt (2022 and 2021), Let's Play (2022 and 2021)"""
        self.new_users = []
        self.new_user_batches = []
        self.series_viewership = []
        self.final_dataframe = None


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
        self.new_user_batches = self.batcher.list_to_list_batch(batch_limit=self.BATCH_LIMIT, iterator=self.new_users)


    def get_viewership_per_batch(self, batch):
        where_in_users = self.sql_helper.array_to_sql_list(batch)
        where_in_series = self.sql_helper.array_to_sql_list(self.series_ids)
        batch_series_viewership = []
        results = self.db_connector.read_redshift(f"""
                                                        WITH max_air_dates as (
                                                            SELECT season_id,
                                                                   dateadd('days', 60, max(air_date)) as final_viewing_date
                                                            FROM warehouse.dim_segment_episode
                                                            WHERE 
                                                                season_id in ({where_in_series}) AND
                                                                episode_number != 0
                                                            GROUP BY 1
                                                        )
                                                        SELECT
                                                            dse.series_id,
                                                            dse.series_title,
                                                            dse.season_id,
                                                            dse.season_number,
                                                            vv.user_tier,
                                                            vv.user_key
                                                        FROM warehouse.vod_viewership vv
                                                        INNER JOIN warehouse.dim_segment_episode dse on vv.episode_key = dse.episode_key
                                                        LEFT JOIN max_air_dates mad on mad.season_id = dse.season_id
                                                        WHERE
                                                            vv.user_key in ({where_in_users}) AND
                                                            vv.user_tier in ('trial', 'premium', 'free') AND
                                                            vv.start_timestamp >= dateadd('days', -13, '{self.target_date}') AND
                                                            vv.start_timestamp < dateadd('days', 1, '{self.target_date}') AND
                                                            vv.max_position > 0 AND
                                                            dse.season_id in ({where_in_series}) AND
                                                            vv.start_timestamp <= mad.final_viewing_date AND
                                                            dse.episode_number != 0
                                                        GROUP BY 1, 2, 3, 4, 5, 6;
                                                """)

        for result in results:
            batch_series_viewership.append({
                'max_viewership_date': self.target_date,
                'series_id': result[0],
                'series_title': result[1],
                'season_id': result[2],
                'season_number': result[3],
                'user_tier': result[4],
                'user_key': result[5]
            })

        return batch_series_viewership


    def get_viewership(self):
        self.loggerv3.info('Getting new user viewership')
        self.series_viewership = []
        for batch in self.new_user_batches:
            self.series_viewership.extend(self.get_viewership_per_batch(batch))


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.series_viewership)


    def write_to_redshift(self):
        self.loggerv3.info('Writing to redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start(f"New Viewership By Season Series for {self.target_date}")
        self.get_new_users()
        self.batch_new_users()
        self.get_viewership()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
