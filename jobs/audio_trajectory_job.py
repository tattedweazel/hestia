import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime


class AudioTrajectoryJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
        super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'audio_trajectory')
        self.target_date = target_date
        self.target_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
        self.final_df = None


    def load_data(self):
        self.loggerv3.info("Running Query")
        query = f""" 
            WITH base_cte as (
                  SELECT
                      mp.clean_title as podcast_title,
                      CAST(mm.created_at AS date) as download_date,
                      count(*) as downloads
                  FROM warehouse.megaphone_metrics mm
                  INNER JOIN warehouse.dim_megaphone_podcast mp ON mm.podcast_id = mp.id
                  WHERE
                      CAST(mm.created_at AS date) >= date_add('days', -130, '{self.target_date}')
                      AND CAST(mm.created_at AS date) < date_add('days', 1, '{self.target_date}')
                      AND mp.clean_title NOT LIKE '%%(FIRST Member Early Access)%%'
                      AND mp.clean_title NOT LIKE '%%(P)'
                      AND mp.clean_title IN ('30 Morbid Minutes', 'ANMA', 'Always Open', 'DEATH BATTLE Cast', 'F**kface',
                                            'Face Jam', 'Funhaus Podcast', 'Hypothetical Nonsense', 'Must Be Dice',
                                            'Red Web', 'Rooster Teeth Podcast', 'So... Alright', 'Tales from the Stinky Dragon',
                                            'Trash for Trash')
                  GROUP BY 1, 2
              ), max_viewership_cte as (
                  SELECT
                      podcast_title,
                      date_add('days', -1, max(download_date)) as max_viewership
                  FROM base_cte
                  GROUP BY 1
              ), extended_base_cte as (
                  SELECT
                      b.podcast_title,
                      b.download_date,
                      b.downloads,
                      mv.max_viewership
                  FROM base_cte b
                  LEFT JOIN max_viewership_cte mv on mv.podcast_title = b.podcast_title
              ), last_month as (
                  SELECT
                    podcast_title,
                    sum(downloads) as downloads
                  FROM extended_base_cte
                  WHERE download_date >= date_add('days', -30, max_viewership) and download_date < max_viewership
                  GROUP BY 1
              ), two_months_ago as (
                  SELECT
                    podcast_title,
                    sum(downloads) as downloads
                  FROM extended_base_cte
                  WHERE download_date >= date_add('days', -60, max_viewership) and download_date < date_add('days', -30, max_viewership)
                  GROUP BY 1
              ), three_months_ago as (
                  SELECT
                    podcast_title,
                    sum(downloads) as downloads
                  FROM extended_base_cte
                  where download_date >= date_add('days', -90, max_viewership) and download_date < date_add('days', -60, max_viewership)
                  GROUP BY 1
              ), four_months_ago as (
                  SELECT
                    podcast_title,
                    sum(downloads) as downloads
                  FROM extended_base_cte
                  WHERE download_date >= date_add('days', -120, max_viewership) and download_date < date_add('days', -90, max_viewership)
                  GROUP BY 1
              )
              SELECT
                  lm.podcast_title as "Podcast Title",
                  lm.downloads as "Downloads - Last Month",
                  round( ((lm.downloads * 1.0) / ((tma.downloads + thma.downloads + fma.downloads) / 3)) - 1, 2) as "90-day Trajectory"
              FROM last_month lm
              LEFT JOIN two_months_ago tma on lm.podcast_title = tma.podcast_title
              LEFT JOIN three_months_ago thma on lm.podcast_title = thma.podcast_title
              LEFT JOIN four_months_ago fma on lm.podcast_title = fma.podcast_title;
            """

        results = self.db_connector.read_redshift(query)
        records = []
        for result in results:
            records.append({
                "run_date": self.target_date,
                "podcast_title": result[0],
                "downloads_last_month": result[1],
                "trajectory": result[2]
            })
        self.final_df = pd.DataFrame(records)


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing to Redshift...")
        self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append', chunksize=5000)
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.info(f"Running Audio Trajectory Job for {self.target_date}")
        self.load_data()
        self.write_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
