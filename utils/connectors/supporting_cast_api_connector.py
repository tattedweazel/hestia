import json
import requests
from time import sleep
from base.exceptions import ResponseCodeException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils.connectors.api_connector import ApiConnector



class SupportingCastApiConnector(ApiConnector):

    def __init__(self, file_location, logger=None):
        super().__init__(file_location)
        self.endpoints = {
            "memberships": "v1/memberships",
            'episode_downloads': 'v1/downloads',
            "bulk": "v1/bulk",
            'feeds': 'v1/feeds',
            'plans': 'v1/plans'
        }
        self.records = None
        self.MAX_RECORDS_PER_PAGE = 500
        self.API_CALL_RATE_LIMIT = 6  # 100 GET requests every 10 seconds.
        self.TIMEOUT = 1000000000
        self.MAX_RETRIES = 15
        self.SLEEP = 30
        self.headers = self.get_request_headers()
        self.status_forcelist = [404, 500, 502, 503, 504]
        self.retries_total = 10
        self.backoff_factor = 0.1
        self.retries = Retry(total=self.retries_total,
                        backoff_factor=self.backoff_factor,
                        status_forcelist=self.status_forcelist
                        )
        self.loggerv3 = logger


    def get_request_headers(self):
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {self.creds["SUPPORTING_CAST_TOKEN"]}',
            'Content-Type': 'application/json'
        }
        return headers


    def get_base_url(self, endpoint):
        return f'https://{self.creds["SUPPORTING_CAST_URL"]}/{self.endpoints[endpoint]}'


    def get_members_url(self, endpoint, page, records_per_page, include_feeds):
        base_url = self.get_base_url(endpoint)
        if records_per_page > self.MAX_RECORDS_PER_PAGE:
            records_per_page = self.MAX_RECORDS_PER_PAGE
        return f'{base_url}?page={page}&max_page_size={records_per_page}&include_feeds={include_feeds}'


    def get_feeds_url(self, endpoint, page, records_per_page):
        base_url = self.get_base_url(endpoint)
        if records_per_page > self.MAX_RECORDS_PER_PAGE:
            records_per_page = self.MAX_RECORDS_PER_PAGE
        return f'{base_url}/?page={page}&max_page_size={records_per_page}'


    def make_single_page_request(self, endpoint, page, records_per_page, include_feeds='false'):
        full_url = self.get_members_url(endpoint, page, records_per_page, include_feeds)
        s = requests.Session()
        s.mount('https://', HTTPAdapter(max_retries=self.retries))
        response = s.get(url=full_url, headers=self.headers)

        if response.status_code > 202:
            raise ResponseCodeException(response)

        return {
            'code': response.status_code,
            'data': json.loads(response.text)['data'],
            'pages': json.loads(response.text)['last_page']
        }


    def get_single_day_downloads_url(self, endpoint, target_date):
        base_url = self.get_base_url(endpoint)
        return f'{base_url}/csv?start_date={target_date}'


    def get_multi_day_downloads_url(self, endpoint, earliest_date, latest_date):
        pass


    def make_single_day_request(self, endpoint, target_date):
        full_url = self.get_single_day_downloads_url(endpoint, target_date)
        response = requests.get(url=full_url, headers=self.headers)

        if response.status_code > 202:
            raise ResponseCodeException(response)

        return {
            'code': response.status_code,
            'data': response.text
        }


    def make_feeds_request(self, endpoint, page, records_per_page):
        full_url = self.get_feeds_url(endpoint, page, records_per_page)
        response = requests.get(url=full_url, headers=self.headers)

        if response.status_code > 202:
            raise ResponseCodeException(response)

        return {
            'code': response.status_code,
            'data': json.loads(response.text)['data'],
            'pages': json.loads(response.text)['last_page']
        }

    def make_bulk_request(self, endpoint, records_per_page, total_pages):
        results = []
        if records_per_page > self.MAX_RECORDS_PER_PAGE:
            records_per_page = self.MAX_RECORDS_PER_PAGE

        full_url = self.get_base_url('bulk')
        body = {}
        for i in range(1, total_pages+1):
            req_key = f"req-{i}"
            data = {
                "path": self.endpoints[endpoint],
                "method": "get",
                "data": {
                    "page": i,
                    "max_page_size": records_per_page
                }
            }
            body[req_key] = data
            if i % self.API_CALL_RATE_LIMIT == 0 or i == total_pages:
                self.loggerv3.inline_info(f'page {i}')
                response = requests.post(url=full_url, data=json.dumps(body), headers=self.headers, timeout=self.TIMEOUT)
                retry = 0
                while response.status_code in (503, 504):
                    self.loggerv3.inline_info(f'Retrying page {i}')
                    sleep(self.SLEEP)
                    retry += 1
                    response = requests.post(url=full_url, data=json.dumps(body), headers=self.headers, timeout=self.TIMEOUT)
                    if retry >= self.MAX_RETRIES:
                        self.loggerv3.exception(f'Max retries exceeded. {response}', terminate=True)

                if response.status_code > 202 and response.status_code != 504:
                    self.loggerv3.exception(f'{response}', terminate=True)

                response_dict = json.loads(response.text)
                for key, result in response_dict.items():
                    results.extend(result['data']['data'])
                sleep(self.SLEEP)
                body = {}

        return results
