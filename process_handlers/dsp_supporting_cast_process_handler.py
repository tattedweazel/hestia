import config
from base.server_process_handler import ServerProcessHandler
from jobs.log_file_handler_job import LogFileHandlerJob
from jobs.supporting_cast_episodes_job import SupportingCastEpisodesJob
from jobs.supporting_cast_members_job import SupportingCastMembersJob
from jobs.supporting_cast_plans import SupportingCastPlansJob
from jobs.supporting_cast_podcasts_job import SupportingCastPodcastsJob
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater
from utils.components.loggerv3 import Loggerv3


class DspSupportingCastProcessHandler(ServerProcessHandler):

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
        self.loggerv3.start('Starting DSP Supporting Cast Process Handler')

        yesterday = self.dater.format_date(self.dater.find_previous_day(self.dater.get_today()))

        # scmj = SupportingCastMembersJob(db_connector=self.connector, file_location=self.file_location)
        # scmj.execute()

        scej = SupportingCastEpisodesJob(target_date=yesterday, db_connector=self.connector, file_location=self.file_location)
        scej.execute()

        scpj = SupportingCastPlansJob(db_connector=self.connector, file_location=self.file_location)
        scpj.execute()

        scpcj = SupportingCastPodcastsJob(db_connector=self.connector, file_location=self.file_location)
        scpcj.execute()

        lfhj = LogFileHandlerJob()
        lfhj.execute()

        self.loggerv3.success("All Processing Complete!")
