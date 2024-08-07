import pandas as pd
from datetime import datetime, timedelta
from utils.components.sql_helper import SqlHelper
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class EpisodeViewerCohorts:

    def __init__(self, episode_key):
        self.table_name = 'episode_viewer_cohorts'
        self.db_connector = DatabaseConnector('')
        self.loggerv3 = Loggerv3(name=__name__, file_location='', local_mode=1)
        self.episode_key = episode_key
        self.DAYS_DELTA = 15
        self.BATCH_LIMIT = 500
        self.sql_helper = SqlHelper()
        self.episode_info = None
        self.episode_viewers = None
        self.viewers_30day = None
        self.viewers_30_180day = None
        self.final_dataframe = None
        self.loggerv3.alert = False


    def get_episode_info(self):
        self.loggerv3.info('Getting episode info')
        results = self.db_connector.read_redshift(f"""
                                                    SELECT
                                                           dse.episode_key,
                                                           dse.episode_title,
                                                           dse.series_id,
                                                           dse.series_title,
                                                           dse.air_date
                                                    FROM warehouse.dim_segment_episode dse
                                                    WHERE 
                                                        dse.episode_key = '{self.episode_key}';
                                                """
                                                   )
        for result in results:
            self.episode_info = {
                'episode_key': result[0],
                'episode_title': result[1],
                'series_id': result[2],
                'series_title': result[3],
                'air_date': result[4]
            }


    def check_episode_guardrails(self):
        self.loggerv3.info('Checking episode air date')
        air_date = self.episode_info['air_date']
        today = datetime.today()
        day_diff = (today - air_date).days
        if day_diff < self.DAYS_DELTA:
            raise f'Episode air date must be at least {self.DAYS_DELTA} days old'


    def get_episode_viewers(self):
        """Get KNOWN viewers of that episode between [Air Date, Air Date+14]"""
        self.loggerv3.info('Getting episode viewers')
        self.episode_viewers = []
        air_date = self.episode_info['air_date'].strftime('%Y-%m-%d %H:%M:%S')
        final_viewing_date = (self.episode_info['air_date'] + timedelta(days=self.DAYS_DELTA)).strftime('%Y-%m-%d %H:%M:%S')
        results = self.db_connector.read_redshift(f"""
                                                    SELECT 
                                                        distinct vv.user_key
                                                    FROM warehouse.vod_viewership vv
                                                    WHERE
                                                        vv.user_key is not null AND 
                                                        vv.user_tier in ('trial', 'premium', 'free') AND
                                                        vv.episode_key = '{self.episode_key}' AND
                                                        vv.start_timestamp >= '{air_date}' AND 
                                                        vv.start_timestamp < '{final_viewing_date}' AND 
                                                        vv.active_seconds > 30;
                                                """
                                                   )

        for result in results:
            self.episode_viewers.append(result[0])


    def get_viewership_per_range(self, start_date, end_date):
        viewers = []
        results = self.db_connector.read_redshift(f"""
                                                    SELECT distinct user_key
                                                    FROM warehouse.vod_viewership
                                                    WHERE 
                                                          start_timestamp >= '{start_date}' AND
                                                          start_timestamp < '{end_date}' AND
                                                          user_key is not NULL AND
                                                          active_seconds > 30;
                                                    """
                                                   )
        for result in results:
            viewers.append(result[0])

        return viewers


    def get_30day_viewership(self):
        """Per batch, get non-anon viewers between [Air Date-30, Air Date)"""
        self.loggerv3.info('Getting viewership 30 days from air date')
        self.viewers_30day = []
        start_date = (self.episode_info['air_date'] + timedelta(days=-30)).strftime('%Y-%m-%d %H:%M:%S')
        end_date = self.episode_info['air_date'].strftime('%Y-%m-%d %H:%M:%S')
        viewers = self.get_viewership_per_range(start_date=start_date, end_date=end_date)

        for viewer in self.episode_viewers:
            if viewer in viewers:
                self.viewers_30day.append(viewer)


    def get_30day_180day_viewership(self):
        """Per batch, get non-anon viewers between [Air Date-180, Air Date-30)"""
        self.loggerv3.info('Getting viewership between 30 and 180 days from air date')
        self.viewers_30_180day = []
        start_date = (self.episode_info['air_date'] + timedelta(days=-180)).strftime('%Y-%m-%d %H:%M:%S')
        end_date = (self.episode_info['air_date'] + timedelta(days=-30)).strftime('%Y-%m-%d %H:%M:%S')
        viewers = self.get_viewership_per_range(start_date=start_date, end_date=end_date)

        for viewer in self.episode_viewers:
            if viewer in viewers:
                self.viewers_30_180day.append(viewer)


    def set_cohort_counts(self):
        self.loggerv3.info('Setting cohort counts')
        """"
        If a 30 day viewer is also in the 30-180 cohort, then they are in both (core). Otherwise, they are only in 30 day cohort (new)
        If the 30-180 day viewer is in the 30 day cohort, then they are in both (core). Otherwise, they are only in the 30-180 cohort (reactivated). 
        """
        viewers_30day_only = 0
        viewers_30_180day_only = 0
        viewers_both_cohorts = set()

        for viewer in self.viewers_30day:
            if viewer in self.viewers_30_180day:
                viewers_both_cohorts.add(viewer)
            else:
                viewers_30day_only += 1

        for viewer in self.viewers_30_180day:
            if viewer in self.viewers_30day:
                viewers_both_cohorts.add(viewer)
            else:
                viewers_30_180day_only += 1

        viewers_new = len(self.episode_viewers) - (len(viewers_both_cohorts) + viewers_30day_only + viewers_30_180day_only)

        self.episode_info['new_viewers'] = viewers_30day_only + viewers_new
        self.episode_info['reactivated_viewers'] = viewers_30_180day_only
        self.episode_info['core_viewers'] = len(viewers_both_cohorts)


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.episode_info['total_viewers'] = len(self.episode_viewers)
        self.final_dataframe = pd.DataFrame([self.episode_info])


    def write_to_redshift(self):
        self.loggerv3.info('Writing to redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')


    def execute(self):
        self.loggerv3.start(f"Running Episode Viewer Cohorts for {self.episode_key}")
        self.get_episode_info()
        self.check_episode_guardrails()
        self.get_episode_viewers()
        self.get_30day_viewership()
        self.get_30day_180day_viewership()
        self.set_cohort_counts()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.db_connector.update_redshift_table_permissions(self.table_name)
        self.loggerv3.success("All Processing Complete!")
