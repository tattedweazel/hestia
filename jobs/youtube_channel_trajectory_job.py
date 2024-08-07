import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime


class YoutubeChannelTrajectoryJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
        super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'youtube_channel_trajectory')

        self.target_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
        self.final_df = None


    def load_data(self):
        self.loggerv3.info("Running Query")
        query = f""" 
            WITH base_cte as (
                SELECT
                    dim.channel_title,
                    to_date(cast(coca2.start_date as varchar), 'YYYYMMDD') as start_timestamp,
                    sum(coca2.views) as views
                FROM warehouse.content_owner_combined_a2 coca2
                INNER JOIN warehouse.dim_yt_video_v2 dim on dim.video_id = coca2.video_id
                WHERE
                    to_date(cast(coca2.start_date as varchar), 'YYYYMMDD') >= date_add('days', -130, '{self.target_date}')
                    AND to_date(cast(coca2.start_date as varchar), 'YYYYMMDD') < date_add('days', 1, '{self.target_date}')
                    AND dim.duration_in_seconds > 60
                    AND channel_title in
                    ('Achievement Hunter', 'Funhaus', 'Rooster Teeth', 'Rooster Teeth Animation', 'LetsPlay', 'DEATH BATTLE!',
                    'Red Web', 'Face Jam', 'F**KFACE', 'Inside Gaming', 'Funhaus Too', 'All Good No Worries',
                    'Tales from the Stinky Dragon', 'Best Friends Today', 'Hypothetical Nonsense', 'Rooster Teeth Podcast', 'Dogbark')
            GROUP BY 1, 2
            ), max_viewership_cte as (
                SELECT
                    channel_title,
                    date_add('days', -1, max(start_timestamp)) as max_viewership
                FROM base_cte
                GROUP BY 1
            ), extended_base_cte as (
                SELECT
                    b.channel_title,
                    b.start_timestamp,
                    b.views,
                    mv.max_viewership
                FROM base_cte b
                LEFT JOIN max_viewership_cte mv on mv.channel_title = b.channel_title
            ), last_month as (
                SELECT
                    channel_title,
                    sum(views) as views
                FROM extended_base_cte
                WHERE start_timestamp >= date_add('days', -30, max_viewership) and start_timestamp < max_viewership
                GROUP BY 1
            ), two_months_ago as (
                SELECT
                    channel_title,
                    sum(views) as views
                FROM extended_base_cte
                WHERE start_timestamp >= date_add('days', -60, max_viewership) and start_timestamp < date_add('days', -30, max_viewership)
                GROUP BY 1
            ), three_months_ago as (
                SELECT
                    channel_title,
                    sum(views) as views
                FROM extended_base_cte
                WHERE start_timestamp >= date_add('days', -90, max_viewership) and start_timestamp < date_add('days', -60, max_viewership)
                GROUP BY 1
            ), four_months_ago as (
                SELECT
                    channel_title,
                    sum(views) as views
                FROM extended_base_cte
                WHERE start_timestamp >= date_add('days', -120, max_viewership) and start_timestamp < date_add('days', -90, max_viewership)
                GROUP BY 1
            )
            SELECT
                lm.channel_title as "Channel Title",
                lm.views as "Views - Last Month",
                round( ((lm.views * 1.0) / ((tma.views + thma.views + fma.views) / 3)) - 1, 2) as "90-day Trajectory"
            FROM last_month lm
            LEFT JOIN two_months_ago tma on lm.channel_title = tma.channel_title
            LEFT JOIN three_months_ago thma on lm.channel_title = thma.channel_title
            LEFT JOIN four_months_ago fma on lm.channel_title = fma.channel_title;
            """

        results = self.db_connector.read_redshift(query)
        records = []
        for result in results:
            records.append({
                "run_date": self.target_date,
                "channel_title": result[0],
                "views_last_month": result[1],
                "trajectory": result[2]
            })
        self.final_df = pd.DataFrame(records)


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing to Redshift...")
        self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.info(f"Running Youtube Trajectory Job for {self.target_date}")
        self.load_data()
        self.write_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
