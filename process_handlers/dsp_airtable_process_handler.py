import config
from base.server_process_handler import ServerProcessHandler
from jobs.airtable_funhaus_job import AirtableFunhausJob
from jobs.airtable_team_members_job import AirtableTeamMembersJob
from jobs.airtable_funhaus_cast_mapper_job import AirtableFunhausCastMapperJob
from jobs.funhaus_yt_cast_analysis_job import FunhausYTCastAnalysisJob
from jobs.funhaus_yt_editor_analysis_job import FunhausYTEditorAnalysisJob
from jobs.log_file_handler_job import LogFileHandlerJob
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater
from utils.components.loggerv3 import Loggerv3


class DspAirtableProcessHandler(ServerProcessHandler):

    def __init__(self, local_mode):
        if local_mode:
            self.file_location = ''
        else:
            self.file_location = '/home/ubuntu/processes/rt-data-hestia/'
        self.dater = Dater()
        config.file_location = self.file_location
        config.local_mode = local_mode
        config.process_handler = __name__
        self.connector = DatabaseConnector(config.file_location, config.dry_run)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location)
        self.loggerv3.disable_alerting()


    def run_jobs(self):
        self.loggerv3.start('Starting DSP Airtable Process Handler')

        afj = AirtableFunhausJob(db_connector=self.connector, file_location=self.file_location)
        afj.execute()

        atmj = AirtableTeamMembersJob(db_connector=self.connector, file_location=self.file_location)
        atmj.execute()

        afcmj = AirtableFunhausCastMapperJob(db_connector=self.connector)
        afcmj.execute()

        # fytcaj = FunhausYTCastAnalysisJob(db_connector=connector, series_show_flag='yt_show')
        # fytcaj.execute()
        #
        # fyteaj = FunhausYTEditorAnalysisJob(db_connector=connector, series_show_flag='yt_show')
        # fyteaj.execute()

        lfhj = LogFileHandlerJob()
        lfhj.execute()

        self.loggerv3.success("All Processing Complete!")
