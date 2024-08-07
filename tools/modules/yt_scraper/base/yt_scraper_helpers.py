from time import sleep
import random


def sign_in_confirm_helper():
    while True:
        confirm = input("Did you sign in (y/n), 2-factor auth, and choose a channel (any channel)? ")
        if confirm != 'y':
            print("Need to enter a 'y' to confirm")
            continue
        else:
            break


def change_channel_helper(page, channel_positions, channel_title):
    while True:
        try:
            page.locator('#avatar-btn > yt-img-shadow').click()
            page.get_by_text('Switch Account').click()
            channel_position = channel_positions[channel_title]
            page.locator(f'#contents > ytd-account-item-renderer:nth-child({channel_position})').click()
        except:
            input('Re-authenticate')
            continue
        break


def retry_goto_helper(page, link, max_retries):
    retries = 1
    while retries <= max_retries:
        try:
            page.goto(link)
        except Exception as e:
            print(e)
            retries += 1
            sleep(6)
            continue
        break


def request_url_helper(page, min_sleep, max_sleep, max_retries, channel, video, url_sub_dir, locator, time_period, retries):
    response = page.goto(f'https://studio.youtube.com/video/{video}/analytics/{url_sub_dir}/{time_period}')
    if response.status == 200:
        sleep(random.randrange(min_sleep, max_sleep))
        if len(page.locator(locator).all_inner_texts()) > 0:
            return True
        else:
            if retries == max_retries:
                return False
            else:
                return request_url_helper(page, min_sleep, max_sleep, max_retries, channel, video, url_sub_dir, locator, time_period, retries + 1)

    else:
        if retries == max_retries:
            return False
        else:
            return request_url_helper(page, min_sleep, max_sleep, max_retries, channel, video, url_sub_dir, locator, time_period, retries + 1)


def convert_metrics_helper(metric):
    if '—' == metric or metric is None:
        return 0.0
    elif 'K' in metric:
        return float(metric.replace('K', '').replace(',', '')) * 1000
    elif 'M' in metric:
        return float(metric.replace('M', '').replace(',', '')) * 1000000
    else:
        return float(metric.replace(',', ''))


def convert_time_helper(metric):
    if metric == '—' or metric is None:
        return 0
    else:
        minutes_in_seconds = int(metric.split(':')[0]) * 60
        seconds = int(metric.split(':')[1])
        return minutes_in_seconds + seconds


def convert_subscribers_helper(metric):
    if '—' == metric or metric is None:
        return 0
    elif 'K' in metric:
        if '—' in metric:
            return -1 * int(float(metric.replace('—', '').replace('K', '').replace(',', '')) * 1000)
        else:
            return int(float(metric.replace('+', '').replace('K', '').replace(',', '')) * 1000)
    else:
        if '—' in metric:
            return -1 * int(metric.replace('—', '').replace(',', ''))
        else:
            return int(metric.replace('+', '').replace(',', ''))


def convert_percentages_helper(metric):
    if '—' == metric or metric is None:
        return 0.0
    else:
        return float(metric.replace('%', '')) / 100


def convert_int_metrics_helper(metric):
    if metric is None or metric == "0" or metric == '—':
        return None
    elif 'K' in metric:
        return int(float(metric.replace('K', '').replace(',', '').replace('$', '')) * 1000)
    elif 'M' in metric:
        return int(float(metric.replace('M', '').replace(',', '').replace('$', '')) * 1000000)
    else:
        return int(float(metric.replace('$', '').replace(',', '')))


def convert_float_metrics_helper(metric):
    if metric is None or metric == "0" or metric == '—':
        return None
    elif 'K' in metric:
        return float(metric.replace('K', '').replace(',', '').replace('$', '')) * 1000
    elif 'M' in metric:
        return float(metric.replace('M', '').replace(',', '').replace('$', '')) * 1000000
    else:
        return float(metric.replace('$', '').replace(',', ''))
