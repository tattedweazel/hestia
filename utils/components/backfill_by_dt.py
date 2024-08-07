from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta



def backfill_by_date(latest_date='2021-11-21', earliest_date='2021-11-20', input_format='%Y-%m-%d', output_format='%Y-%m-%d', sort='desc'):

    latest = datetime.strptime(latest_date, input_format)
    earliest = datetime.strptime(earliest_date, input_format)
    date_arr = []
    date_delta = (latest - earliest).days
    for i in range(date_delta+1):
        new_date = latest - timedelta(days=i)
        new_date_str = new_date.strftime(output_format)
        date_arr.append(new_date_str)

    if sort == 'desc':
        date_arr.sort(reverse=True)
    else:
        date_arr.sort()

    return date_arr


def backfill_by_month(latest_month='2021-11-21', earliest_month='2021-11-20', input_format='%Y-%m-%d', output_format='%Y-%m-%d', sort='desc'):

    latest = datetime.strptime(datetime.strptime(latest_month, input_format).strftime('%Y-%m') + '-01', input_format)
    earliest = datetime.strptime(datetime.strptime(earliest_month, input_format).strftime('%Y-%m') + '-01', input_format)
    month_arr = []
    month_delta = (latest.year - earliest.year) * 12 + latest.month - earliest.month
    for i in range(month_delta+1):
        new_date = latest - relativedelta(months=i)
        new_date_str = new_date.strftime(output_format)
        month_arr.append(new_date_str)

    if sort == 'desc':
        month_arr.sort(reverse=True)
    else:
        month_arr.sort()

    return month_arr


def backfill_by_week(latest_date='2021-11-21', earliest_date='2021-11-20', input_format='%Y-%m-%d', output_format='%Y-%m-%d', sort='desc', start_dow=0):
    """start_dow is Sunday=0 - Saturday=6"""

    offset = -1
    latest = datetime.strptime(latest_date, input_format)
    earliest = datetime.strptime(earliest_date, input_format)
    date_arr = []
    date_delta = (latest - earliest).days
    for i in range(date_delta+1):
        new_date = latest - timedelta(days=i)
        new_start_week = new_date - timedelta(days=new_date.weekday() - (start_dow + offset))
        new_start_week_str = new_start_week.strftime(output_format)
        if new_start_week_str not in date_arr:
            date_arr.append(new_start_week_str)

    if sort == 'desc':
        date_arr.sort(reverse=True)
    else:
        date_arr.sort()

    return date_arr

