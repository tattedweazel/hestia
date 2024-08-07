from api.base.helpers import params_to_json
from process_handlers.eds_monthly_process_handler import EdsMonthlyProcessHandler


def run(*args):
    args_dict = params_to_json(args)
    emph = EdsMonthlyProcessHandler(
        local_mode=args_dict['local_mode'],
        target_date=args_dict['target_date'],
        dry_run=args_dict['dry_run']
    )
    emph.run_jobs()
