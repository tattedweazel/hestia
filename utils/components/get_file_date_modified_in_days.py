import os
from datetime import datetime


def get_file_date_modified_in_days(file, comparison_date):
    modified_unix = os.path.getmtime(file)
    modified_date_str = datetime.utcfromtimestamp(modified_unix).strftime('%Y-%m-%dT%H:%M:%SZ')
    modified_datetime = datetime.strptime(modified_date_str, '%Y-%m-%dT%H:%M:%SZ')
    diff = (modified_datetime - comparison_date).days
    return diff
