import config
from base.server_process_handler import ServerProcessHandler
from jobs.free_user_onboarding_episodes_job import FreeUserOnboardingEpisodesJob
from jobs.free_user_onboarding_series_job import FreeUserOnboardingSeriesJob
from jobs.log_file_handler_job import LogFileHandlerJob
from jobs.popular_job import PopularJob
from jobs.premium_user_onboarding_episodes_job import PremiumUserOnboardingEpisodesJob
from jobs.premium_user_onboarding_series_job import PremiumUserOnboardingSeriesJob
from jobs.search_job import SearchJob
from jobs.search_recs_job import SearchRecsJob
from jobs.trending_job import TrendingJob
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater
from utils.components.loggerv3 import Loggerv3


class DspPopularityProcessHandler(ServerProcessHandler):

    def __init__(self, local_mode):
        self.local_mode = local_mode
        if local_mode:
            self.file_location = ''
        else:
            self.file_location = '/home/ubuntu/processes/rt-data-hestia/'
        self.dater = Dater()
        config.file_location = self.file_location
        config.local_mode = self.local_mode
        config.process_handler = __name__
        self.connector = DatabaseConnector(config.file_location, config.dry_run)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location)
        self.loggerv3.disable_alerting()


    def run_jobs(self):
        self.loggerv3.start('Starting DSP Popularity Process Handler')

        yesterday = self.dater.format_date(self.dater.find_previous_day(self.dater.get_today()))

        pj = PopularJob(db_connector=self.connector, file_location=self.file_location)
        pj.execute()

        tj = TrendingJob(db_connector=self.connector, file_location=self.file_location)
        tj.execute()

        sj = SearchJob(db_connector=self.connector, file_location=self.file_location)
        sj.execute()

        fuoej = FreeUserOnboardingEpisodesJob(db_connector=self.connector, target_date=yesterday, file_location=self.file_location)
        fuoej.execute()

        puoej = PremiumUserOnboardingEpisodesJob(db_connector=self.connector, target_date=yesterday, file_location=self.file_location)
        puoej.execute()

        fuosj = FreeUserOnboardingSeriesJob(db_connector=self.connector, target_date=yesterday, file_location=self.file_location)
        fuosj.execute()

        puosj = PremiumUserOnboardingSeriesJob(db_connector=self.connector, target_date=yesterday, file_location=self.file_location)
        puosj.execute()

        srj = SearchRecsJob(db_connector=self.connector, target_date=yesterday, file_location=self.file_location)
        srj.execute()

        lfhj = LogFileHandlerJob()
        lfhj.execute()

        self.loggerv3.success("All Processing Complete!")
