from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class RiskPredictionV2:

    def __init__(self, start_date, threshold, day_range, excluded_users):
        self.start_date = start_date
        self.threshold = threshold
        self.excluded_users = excluded_users
        self.day_range = day_range
        self.db_connector = DatabaseConnector('')
        self.loggerv3 = Loggerv3(name=__name__, file_location='', local_mode=1)
        self.results = []
        self.aggregate_results = None
        self.final_data_structure = []
        self.loggerv3.alert = False


    def get_results(self):
        results = self.db_connector.read_redshift(f"""
                                                        WITH renewals as (
                                                            SELECT 'Renewals' as type,
                                                                   user_key,
                                                                   signup_date,
                                                                   sub_type,
                                                                   sum(vod_time_watched_min) as vod_min
                                                            FROM warehouse.daily_user_renewal_cohorts
                                                            WHERE total_sub_days > 31
                                                              and days_into_sub <= {self.day_range}
                                                              and signup_date >= '{self.start_date}' and signup_date < dateadd('day', 1, '{self.start_date}')
                                                            GROUP BY 1, 2, 3, 4
                                                        ), non_renewals as (
                                                            SELECT 'Non Renewals' as type,
                                                                   user_key,
                                                                   signup_date,
                                                                   sub_type,
                                                                   sum(vod_time_watched_min) as vod_min
                                                            FROM warehouse.daily_user_non_renewal_cohorts
                                                            WHERE total_sub_days <= 31
                                                              and days_into_sub <= {self.day_range}
                                                              and signup_date >= '{self.start_date}' and signup_date < dateadd('day', 1, '{self.start_date}')
                                                            GROUP BY 1, 2, 3, 4
                                                        )
                                                        SELECT type, user_key, signup_date, sub_type, vod_min
                                                        FROM renewals
                                                        UNION ALL
                                                        SELECT type, user_key, signup_date, sub_type, vod_min
                                                        FROM non_renewals;
                                                    """
                                                )

        for result in results:
            user_key = str(result[1])
            if user_key not in self.excluded_users:
                self.results.append({
                    'type': result[0],
                    'user_key': user_key,
                    'signup_date': self.start_date,
                    'sub_type': result[3],
                    'range': range,
                    'threshold': self.threshold,
                    'vod_min': result[4]
                })


    def populate_aggregate_results(self):
        self.aggregate_results = {
            'FTP Conversion': {
                'top_left': 0,
                'top_right': 0,
                'bottom_left': 0,
                'bottom_right': 0,
                'hits': 0,
                'misses': 0
            },
            'Returning Paid': {
                'top_left': 0,
                'top_right': 0,
                'bottom_left': 0,
                'bottom_right': 0,
                'hits': 0,
                'misses': 0
            },
            'New Paid': {
                'top_left': 0,
                'top_right': 0,
                'bottom_left': 0,
                'bottom_right': 0,
                'hits': 0,
                'misses': 0
            }
        }
        for result in self.results:
            aggregate = self.aggregate_results[result['sub_type']]

            if result['type'] == 'Renewals' and result['vod_min'] >= self.threshold:
                # Top Left
                aggregate['top_left'] += 1
                aggregate['hits'] += 1
            elif result['type'] == 'Renewals' and result['vod_min'] < self.threshold:
                # Top Right
                aggregate['top_right'] += 1
                aggregate['misses'] += 1
            elif result['type'] == 'Non Renewals' and result['vod_min'] >= self.threshold:
                # Bottom Left
                aggregate['bottom_left'] += 1
                aggregate['misses'] += 1
            elif result['type'] == 'Non Renewals' and result['vod_min'] < self.threshold:
                # Bottom Right
                aggregate['bottom_right'] += 1
                aggregate['hits'] += 1


    def populate_final_data_structure(self):
        for sub_type, data in self.aggregate_results.items():
            self.final_data_structure.append({
                'sub_type': sub_type,
                'signup_date': self.start_date,
                'range': self.day_range,
                'threshold': self.threshold,
                'total': data['hits'] + data['misses'],
                'hits': data['hits'],
                'misses': data['misses'],
                'top_left': data['top_left'],
                'top_right': data['top_right'],
                'bottom_left': data['bottom_left'],
                'bottom_right': data['bottom_right'],

            })


    def execute(self):
        self.loggerv3.start(f'Running risk prediction on {self.start_date} for {self.day_range}-day range and {self.threshold} minute threshold')
        self.get_results()
        self.populate_aggregate_results()
        self.populate_final_data_structure()
        self.loggerv3.success("All Processing Complete!")

