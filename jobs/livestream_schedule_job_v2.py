import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.batcher import Batcher
from datetime import datetime, timedelta


class LivestreamScheduleJobV2(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='livestream_schedule_v2')
        self.target_date = target_date
        self.target_date_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
        self.next_date_dt = self.target_date_dt + timedelta(1)
        self.next_date = datetime.strftime(self.next_date_dt, '%Y-%m-%d')
        self.batcher = Batcher()
        self.batches = None
        self.livestreams = []
        self.episodes = []
        self.final_dataframe = None


    def get_livestream_schedules(self):
        self.loggerv3.info("Getting livestream schedules")
        query = f"""
        SELECT
            slug as event_key,
            start_time,
            end_time,
            title,
            length,
            container_type,
            container_uuid,
            category
        FROM livestream_scheduled_items
        WHERE 
            start_time >= '{self.target_date}'
            AND start_time < '{self.next_date}';
        """
        results = self.db_connector.query_svod_be_db_connection(query)
        for result in results:
            self.livestreams.append({
                'event_key': result[0],
                'start_time': result[1],
                'end_time': result[2],
                'title': result[3],
                'length': result[4],
                'container_type': result[5],
                'container_uuid': result[6],
                'category': result[7]
            })


    def get_channels(self):
        self.loggerv3.info('Getting Channels')
        uuids = [episode['container_uuid'] for episode in self.livestreams]
        self.batches = self.batcher.list_to_list_batch(batch_limit=500, iterator=uuids)
        for batch in self.batches:
            episodes = ','.join([f"'{x}'" for x in batch])
            query = f"""
                SELECT channel_title, episode_key
                FROM warehouse.dim_segment_episode
                WHERE episode_key in ({episodes});
            """
            results = self.db_connector.read_redshift(query)
            for result in results:
                self.episodes.append({
                    'channel': result[0],
                    'episode_key': result[1]
                })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        livestreams_df = pd.DataFrame(self.livestreams)
        episodes_df = pd.DataFrame(self.episodes)

        self.final_dataframe = livestreams_df.merge(episodes_df, how='left', left_on='container_uuid', right_on='episode_key')
        del self.final_dataframe['episode_key']


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start(f"Running Livestream Schedule Job v2 for {self.target_date}")
        self.get_livestream_schedules()
        if len(self.livestreams) > 0:
            self.get_channels()
            self.build_final_dataframe()
            self.write_results_to_redshift()
        else:
            self.loggerv3.info(f'No livestream_scheduled_items between {self.target_date} and {self.next_date}')
        self.loggerv3.success("All Processing Complete!")
