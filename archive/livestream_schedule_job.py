import boto3
import csv
import os
import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta
from pytz import timezone


class LivestreamScheduleJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='livestream_schedule')
        self.source_file = f"{target_date}.csv"
        self.rt_session = boto3.session.Session(profile_name='roosterteeth')
        self.rt_s3 = self.rt_session.client('s3')
        self.rt_bucket = "rt-livestream-schedule"
        self.downloads_directory = 'downloads/livestream'
        self.file_dir = "downloads/livestream/schedule.csv"
        self.events = []
        self.final_dataframe = None
        self.invalid_events = None
        self.brodcast_streams = {
            'RT Podcast Livestream': 'Rooster Teeth Podcast',
            'Off-topic-live': 'Off Topic'
        }
        if target_date < '2020-03-23':
            self.pre_streams = [
                'RT Podcast Livestream',
                'Off-topic-live'
            ]
        else:
            self.pre_streams = []


    def get_length(self, title, next_date, date_after_next):
        next_day = datetime.strftime(next_date, '%Y-%m-%d')
        day_after_next = datetime.strftime(date_after_next, '%Y-%m-%d')
        series_title = self.get_series_title_for_stream(title)
        results = self.db_connector.query_v2_db(f""" SELECT
                                                        se.length,
                                                        se.title,
                                                        se.uuid
                                                    FROM
                                                        show_episodes se
                                                    LEFT JOIN
                                                        show_seasons ss on se.season_id = ss.id
                                                    LEFT JOIN
                                                        shows s on ss.show_id = s.id
                                                    WHERE
                                                        se.sponsor_golive_at >= '{next_day}' AND
                                                        se.sponsor_golive_at < '{day_after_next}' AND
                                                        s.name = '{series_title}'
                                                    ORDER BY 1 asc
                                                    LIMIT 1;""")

        for result in results:
            return (result[0], result[1], result[2])


    def get_episode_id_by_slug(self, slug):
        results = self.db_connector.query_v2_db(f""" SELECT
                                                        se.uuid
                                                     FROM
                                                        show_episodes se
                                                     WHERE
                                                        se.slug = '{slug}'
                                                     LIMIT 1;""")
        for result in results:
            return result[0]
        return None


    def get_series_title_for_stream(self, title):
        return self.brodcast_streams[title]


    def load_csv(self):
        self.loggerv3.info('Loading CSV from S3')
        self.rt_s3.download_file(Bucket=self.rt_bucket, Key=self.source_file, Filename=self.file_dir)
        events = []
        with open(self.file_dir, 'r') as f:
            reader = csv.reader(f, delimiter=',')
            file_data = [row for row in reader][2:]  # skip the headers since there are 2 and it dorks things up
            HOUR = 0
            ASSET_ID = 4
            TYPE = 5
            TITLE = 7
            for event in file_data:
                if len(event) < 1:
                    continue
                record = {
                    "start_timestamp": event[HOUR],
                    "event_type": event[TYPE],
                    "event_key": event[ASSET_ID],
                    "event_title": event[TITLE]
                }
                events.append(record)
        self.events = events


    def drop_bad_records(self):
        self.loggerv3.info('Removing Bad Records')
        idx = 0
        for event in self.events:
            if event['start_timestamp'] == '':
                del self.events[idx]
            idx += 1


    def update_start_and_end_times(self):
        self.loggerv3.info('Updating start and end times')
        event_idx = 0
        for event in self.events:
            event_type = event['event_type']
            event_title = event['event_title']
            if event_type != 'Streams':  # Regular VOD linear content, use as listed
                if event_idx == len(self.events) - 1:  # last event of the day
                    event_date = datetime.strptime(event['start_timestamp'], '%Y-%m-%d %H:%M:%S:%f')
                    event['end_timestamp'] = f"{event_date.year}-{str(event_date.month).zfill(2)}-{str(event_date.day).zfill(2)} 05:59:59:99"
                else:
                    next_start_timestamp = datetime.strptime(self.events[event_idx + 1]['start_timestamp'], '%Y-%m-%d %H:%M:%S:%f')
                    this_end_timestamp = next_start_timestamp - timedelta(seconds=1)
                    event['end_timestamp'] = datetime.strftime(this_end_timestamp, '%Y-%m-%d %H:%M:%S:%f')
            else:  # Actually LIVE Livestreams
                if event_title in self.brodcast_streams:  # Scheduled Livestreams
                    if event_title in self.pre_streams:  # Broadcast Livestreams that have pre-streams
                        stream_date = datetime.strptime(event['start_timestamp'], '%Y-%m-%d %H:%M:%S:%f')
                        next_date = stream_date + timedelta(1)
                        date_after_next = next_date + timedelta(1)
                        stream_length, stream_title, stream_vod_id = self.get_length(event_title, next_date, date_after_next)
                        actual_stream_start = stream_date - timedelta(minutes=15)
                        event['start_timestamp'] = datetime.strftime(actual_stream_start, '%Y-%m-%d %H:%M:%S:%f')
                        event['event_title'] = f"LIVE: {self.brodcast_streams[event_title]} - {stream_title}"
                        event['episode_id'] = stream_vod_id
                        previous_event_end = actual_stream_start - timedelta(seconds=1)
                        stream_end = stream_date + timedelta(seconds=stream_length)
                        if self.events[event_idx - 1]['start_timestamp'] < event['start_timestamp']:
                            self.events[event_idx - 1]['end_timestamp'] = datetime.strftime(previous_event_end, '%Y-%m-%d %H:%M:%S:%f')
                        else:
                            self.events[event_idx - 2]['end_timestamp'] = datetime.strftime(previous_event_end, '%Y-%m-%d %H:%M:%S:%f')
                            self.events[event_idx - 1]['end_timestamp'] = datetime.strftime(previous_event_end, '%Y-%m-%d %H:%M:%S:%f')
                        event['end_timestamp'] = datetime.strftime(stream_end, '%Y-%m-%d %H:%M:%S:%f')
                    else:
                        stream_date = datetime.strptime(event['start_timestamp'], '%Y-%m-%d %H:%M:%S:%f')
                        next_date = stream_date + timedelta(1)
                        date_after_next = next_date + timedelta(1)
                        stream_length, stream_title, stream_vod_id = self.get_length(event_title, next_date, date_after_next)
                        stream_end = stream_date + timedelta(seconds=stream_length)
                        event['end_timestamp'] = datetime.strftime(stream_end, '%Y-%m-%d %H:%M:%S:%f')
                        event['event_title'] = f"LIVE: {self.brodcast_streams[event_title]} - {stream_title}"
                        event['episode_id'] = stream_vod_id
                else:  # Non-broadcast stream, use as listed
                    event['event_title'] = f"LIVE: {event['event_title']}"
                    if event_idx + 1 == len(self.events):
                        next_start_timestamp = datetime.strptime(self.events[0]['start_timestamp'], '%Y-%m-%d %H:%M:%S:%f') + timedelta(days=1)
                    else:
                        next_start_timestamp = datetime.strptime(self.events[event_idx + 1]['start_timestamp'], '%Y-%m-%d %H:%M:%S:%f')
                    this_end_timestamp = next_start_timestamp - timedelta(seconds=1)
                    event['end_timestamp'] = datetime.strftime(this_end_timestamp, '%Y-%m-%d %H:%M:%S:%f')
                    event['episode_id'] = None
            event_idx += 1


    def correct_timeline(self):
        self.loggerv3.info('Correcting timeline')
        approved_idxs = []
        skipped_idxs = []
        current_idx = 0
        events_copy = self.events.copy()
        for event in events_copy:
            if current_idx in skipped_idxs:
                current_idx += 1
                continue
            if current_idx == len(events_copy) - 1:  # last item in list
                approved_idxs.append(current_idx)
                break
            if event['end_timestamp'] > events_copy[current_idx + 1]['start_timestamp']:  # this event ends after the next starts
                approved_idxs.append(current_idx)
                if event['end_timestamp'] < events_copy[current_idx + 1]['end_timestamp']:  # this ends before the next does
                    this_end = datetime.strptime(event['end_timestamp'], '%Y-%m-%d %H:%M:%S:%f')
                    next_start = this_end + timedelta(seconds=1)
                    events_copy[current_idx + 1]['start_timestamp'] = datetime.strftime(next_start, '%Y-%m-%d %H:%M:%S:%f')
                else:  # this ends AFTER the next does
                    skipped_idxs.append(current_idx + 1)
                    searching = True
                    search_idx = current_idx + 2
                    while searching:  # keep going until you find an event that ends AFTER this one does
                        if event['end_timestamp'] > events_copy[search_idx]['end_timestamp']:
                            skipped_idxs.append(search_idx)
                            search_idx += 1
                        else:  # update this event with the proper start time and stop searching
                            this_end = datetime.strptime(event['end_timestamp'], '%Y-%m-%d %H:%M:%S:%f')
                            next_start = this_end + timedelta(seconds=1)
                            events_copy[search_idx]['start_timestamp'] = datetime.strftime(next_start, '%Y-%m-%d %H:%M:%S:%f')
                            searching = False
                current_idx += 1

            else:
                approved_idxs.append(current_idx)
                current_idx += 1
        corrected_events = []
        current_idx = 0
        for event in events_copy:
            if current_idx in approved_idxs:
                corrected_events.append(event)
                current_idx += 1
        self.events = corrected_events


    def drop_invalid_events(self):
        self.loggerv3.info('Dropping invalid events')
        valid_events = []
        invalid_events = []
        for event in self.events:
            if event['start_timestamp'] < event['end_timestamp']:
                valid_events.append(event)
            else:
                invalid_events.append(event)
        self.events = valid_events
        self.invalid_events = invalid_events


    def populate_event_meta(self):
        self.loggerv3.info('Populating event meta')
        for event in self.events:
            if event['event_type'].lower() not in ['streams', 'interstitial']:
                event['episode_id'] = self.get_episode_id_by_slug(event['event_key'])


    def update_timezone(self):
        self.loggerv3.info('Updating timezones')
        for event in self.events:
            start_date = datetime.strptime(event['start_timestamp'], '%Y-%m-%d %H:%M:%S:%f')
            start_date_central = timezone('US/Central').localize(start_date)
            start_date_utc = start_date_central.astimezone(timezone('UTC'))
            end_date = datetime.strptime(event['end_timestamp'], '%Y-%m-%d %H:%M:%S:%f')
            end_date_central = timezone('US/Central').localize(end_date)
            end_date_utc = end_date_central.astimezone(timezone('UTC'))
            event['start_timestamp'] = start_date_utc
            event['end_timestamp'] = end_date_utc


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.events)


    def write_all_results_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)



    def clean_up(self):
        if len(os.listdir(f'{self.downloads_directory}/')) > 1:
            os.system(f'rm {self.downloads_directory}/*.*')


    def execute(self):
        self.loggerv3.start(f"Running Livestream Schedule Job for {self.target_date}")
        self.load_csv()
        self.drop_bad_records()
        self.update_start_and_end_times()
        self.correct_timeline()
        self.drop_invalid_events()
        self.populate_event_meta()
        self.update_timezone()  # Because all db times should be utc
        self.build_final_dataframe()
        self.write_all_results_to_redshift()
        self.clean_up()
        self.loggerv3.success("All Processing Complete!")
