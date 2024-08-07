from tools.modules.episode_viewer_analysis.episode_viewer_cohorts import EpisodeViewerCohorts
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class EpisodeViewerCohortsBacklogWrapper:

    def __init__(self, start_date, end_date, date_type='viewership'):
        self.db_connector = DatabaseConnector('')
        self.loggerv3 = Loggerv3(name=__name__, file_location='', local_mode=1)
        self.start_date = start_date
        self.end_date = end_date
        self.date_type = date_type
        self.UNIQUE_VIEWER_THRESHOLD = 10
        self.episode_keys = []
        self.loggerv3.alert = False


    def get_episode_keys(self):
        self.loggerv3.info('Getting episode keys')
        if self.date_type == 'viewership':
            results = self.db_connector.read_redshift(f"""
                                                        SELECT
                                                            dse.episode_key,
                                                            count(distinct vv.user_key) as unique_viewers
                                                        FROM warehouse.vod_viewership vv
                                                        INNER JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
                                                        WHERE 
                                                            vv.start_timestamp >= '{self.start_date}' and 
                                                            vv.start_timestamp < '{self.end_date}' and
                                                            dse.air_date >= '2021-07-01' and
                                                            dse.channel_title in ('Rooster Teeth', 'Funhaus', 'Achievement Hunter', 'Squad Team Force', 'Death Battle', 'Red vs. Blue Universe', 'RWBY') and 
                                                            vv.active_seconds > 30
                                                        GROUP BY 1
                                                        HAVING unique_viewers >= {self.UNIQUE_VIEWER_THRESHOLD}
                                                        ORDER BY 2 DESC;
                                                        """
                                                       )
        elif self.date_type == 'air_date':
            results = self.db_connector.read_redshift(f"""
                                                            SELECT
                                                                dse.episode_key,
                                                                count(distinct vv.user_key) as unique_viewers
                                                            FROM warehouse.vod_viewership vv
                                                            INNER JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
                                                            WHERE dse.air_date >= '{self.start_date}' and dse.air_date < '{self.end_date}' and
                                                                  dse.channel_title in ('Rooster Teeth', 'Funhaus', 'Achievement Hunter', 'Squad Team Force', 'Death Battle', 'Red vs. Blue Universe', 'RWBY') and 
                                                                  vv.active_seconds > 30
                                                            GROUP BY 1
                                                            HAVING unique_viewers >= {self.UNIQUE_VIEWER_THRESHOLD}
                                                            ORDER BY 2 DESC;
                                                            """
                                                       )
        else:
            results = None

        for result in results:
            self.episode_keys.append(str(result[0]))


    def run_episode_viewer_cohorts(self):
        self.loggerv3.info('Running episode viewer cohorts jobs')
        total_episodes = len(self.episode_keys)
        for idx, episode_key in enumerate(self.episode_keys):
            self.loggerv3.info(f'Running episode {idx+1} of {total_episodes}')
            evc = EpisodeViewerCohorts(episode_key=episode_key)
            evc.execute()


    def execute(self):
        self.loggerv3.start('Starting Episode Viewer Cohorts Wrapper')
        self.get_episode_keys()
        self.run_episode_viewer_cohorts()
        self.loggerv3.success("All Processing Complete!")
