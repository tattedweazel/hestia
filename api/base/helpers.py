import config


def params_to_json(args):
    local_mode = config.local_mode
    dry_run = config.dry_run
    target_date = None
    db_file_location = config.file_location
    if len(args) > 0:
        local_mode = args[0]['local_mode'] if 'local_mode' in args[0] else local_mode
        target_date = args[0]['target_date'] if 'target_date' in args[0] else target_date
        dry_run = bool(args[0]['dry_run']) if 'dry_run' in args[0] else dry_run
        db_file_location = args[0]['db_file_location'] if 'db_file_location' in args[0] else db_file_location

    return {
        "local_mode": local_mode,
        "target_date": target_date,
        "dry_run": dry_run,
        "db_file_location": db_file_location
    }
