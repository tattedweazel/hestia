import config
from base.server_process_handler import ServerProcessHandler
from jobs.shopify_dedupe_user_job import ShopifyDedupeUserJob
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater


class SandboxProcessHandler(ServerProcessHandler):

    def __init__(self, local_mode):
        if local_mode:
            self.file_location = ''
        else:
            raise Exception("Sandbox should only be run in local mode")
        self.dater = Dater()
        config.file_location = self.file_location
        config.local_mode = local_mode
        config.process_handler = __name__


    def run_jobs(self):
        connector = DatabaseConnector(self.file_location)
        yesterday = self.dater.format_date(self.dater.find_previous_day(self.dater.get_today()))

        sduj = ShopifyDedupeUserJob(db_connector=connector, file_location=self.file_location)
        sduj.execute()

        # scmj = SupportingCastMembersJob(db_connector=connector, file_location=self.file_location)
        # scmj.execute()

        # scej = SupportingCastEpisodesJob(target_date='2021-12-21', db_connector=connector)
        # scej.execute()

        # urc = UserRenewalCounter(db_connector=connector, start_month='2021-01-01', sub_type='New Paid')
        # urc.execute()

        # unrc = UserNonRenewalCounter(db_connector=connector, start_month='2021-11-01', sub_type='FTP Conversion')
        # unrc.execute()

        # sub_types = ['FTP Conversion', 'New Paid', 'Returning Paid']
        # backfill_months = backfill_by_month(latest_month='2021-11-01', earliest_month='2021-01-01', sort='asc')
        # for sub_type in sub_types:
        #     for month in backfill_months:
        #         unrc = UserNonRenewalCounter(db_connector=connector, start_month=month, sub_type=sub_type)
        #         unrc.execute()
