from api.base.helpers import params_to_json
from jobs.new_viewers_job import NewViewersJob
from utils.connectors.database_connector import DatabaseConnector


def run(*args):
    args_dict = params_to_json(args)
    connector = DatabaseConnector(args_dict['db_file_location'], dry_run=args_dict['dry_run'])
    nvj = NewViewersJob(target_date = args_dict['target_date'], db_connector = connector)
    nvj.execute()
