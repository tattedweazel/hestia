from api.base.helpers import params_to_json
from jobs.core_dag_data_check_job import CoreDagDataCheckJob
from utils.connectors.database_connector import DatabaseConnector


def run(*args):
    args_dict = params_to_json(args)
    connector = DatabaseConnector(args_dict['db_file_location'], dry_run=args_dict['dry_run'])
    cddcj = CoreDagDataCheckJob(db_connector=connector)
    cddcj.execute()
