from datetime import datetime
import json
import os
import unittest
from base.exceptions import MissingBrazeRecordsException
from jobs.shopify_dedupe_user_job import ShopifyDedupeUserJob
from random import randint
from utils.components.extract_from_zip import ExtractFromZip
from zipfile import ZipFile


class TestExtractFromZip(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': '123@mail.com', 'apps': [{'name': 'Rooster Teeth iOS'}], 'total_revenue': 0.0}
        self.user2 = {'external_id': '456-B', 'email': '123@mail.com', 'total_revenue': 0.0, 'custom_attributes': [{'member_tier': 'free_member'}]}
        self.user3 = {'external_id': '789-C', 'email': '789@mail.com', 'total_revenue': 0.0, 'custom_events': [{'name':'Episode Watched','first':'2020-06-26T21:48:58.581Z','last':'2021-01-25T07:24:35.558Z','count':14}], 'purchases': [{'name': '672','first': '2021-10-21T10:49:10.000Z', 'last': '2021-10-21T10:49:10.000Z', 'count':1}]}
        self.users = [self.user1, self.user2, self.user3]
        self.directory = 'tests'
        self.txt_file = 'test_users.txt'
        self.zip_file = 'test_users.zip'
        self.txt_file_path = os.path.join(self.directory, self.txt_file)
        self.zip_file_path = os.path.join(self.directory, self.zip_file)
        with open(self.txt_file_path, 'w') as f:
            for user in self.users:
                f.write(f'{json.dumps(user)}\n')
        with ZipFile(self.zip_file_path, 'w') as z:
            z.write(self.txt_file_path)


    def test_true_extract_files_from_download(self):
        extractor = ExtractFromZip(data_file_name=self.zip_file_path, directory='')
        extractor.extract_files_from_download()
        self.assertTrue(os.path.isfile(self.txt_file_path))


    def test_true_populate_records_from_extracted_files(self):
        extractor = ExtractFromZip(data_file_name=self.zip_file_path, directory=self.directory)
        extractor.populate_records_from_extracted_files()
        records = extractor.records
        True_results = []
        for user in self.users:
            True_results.append(user in records)
        self.assertTrue(False not in True_results)

    def test_false_populate_records_from_extracted_files(self):
        extractor = ExtractFromZip(data_file_name=self.zip_file_path, directory=self.directory)
        extractor.populate_records_from_extracted_files()
        records = extractor.records
        false_results = []
        additional_user = {'external_id': '999-A', 'email': '999@mail.com', 'apps': [{'name': 'Rooster Teeth iOS'}], 'total_revenue': 0.0}
        self.users.append(additional_user)
        for user in self.users:
            false_results.append(user in records)
        self.assertFalse(False not in false_results)


    def tearDown(self):
        os.system(f'rm {self.txt_file_path}')
        os.system(f'rm {self.zip_file_path}')


class TestFilterRecordsToDuplicateUsers(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': '123@mail.com', 'apps': [{'name': 'Rooster Teeth iOS'}],'total_revenue': 0.0}
        self.user2 = {'external_id': '456-B', 'email': '123@mail.com', 'total_revenue': 0.0, 'custom_attributes': [{'member_tier': 'free_member'}]}
        self.user3 = {'external_id': '789-C', 'email': '789@mail.com', 'total_revenue': 0.0, 'custom_events': [{'name': 'Episode Watched', 'first': '2020-06-26T21:48:58.581Z', 'last': '2021-01-25T07:24:35.558Z', 'count': 14}], 'purchases': [{'name': '672', 'first': '2021-10-21T10:49:10.000Z', 'last': '2021-10-21T10:49:10.000Z', 'count': 1}]}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False
        self.sduj.records = [self.user1, self.user2, self.user3]
        self.sduj.filter_records_to_duplicate_users()
        self.dupe_external_ids = self.sduj.duplicate_records_df['external_id'].tolist()


    def test_in_filter_records_to_duplicate_users(self):
        self.assertIn('123-A', self.dupe_external_ids)
        self.assertIn('456-B', self.dupe_external_ids)


    def test_not_in_filter_records_to_duplicate_users(self):
        self.assertNotIn('789-C', self.dupe_external_ids)


class TestLastUsedAttributeExtraction(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': '123@mail.com', 'apps': [{'name': 'Rooster Teeth iOS', 'platform': 'iOS', 'version': '3.6.0', 'sessions': 13, 'first_used': '2020-04-21T07:12:44.413Z', 'last_used': '2020-04-28T07:01:01.516Z'},
                                                                           {'name': 'RoosterTeeth', 'platform': 'Web', 'version': None, 'sessions': 9, 'first_used': '2020-04-25T00:23:16.824Z', 'last_used': '2020-05-24T15:28:33.157Z'}]}
        self.user2 = {'external_id': '789-C', 'email': '789@mail.com', 'apps': None}
        self.user3 = {'external_id': '845-A', 'email': '123@mail.com', 'apps': [{'name': 'Rooster Teeth iOS', 'platform': 'iOS', 'version': '3.6.0', 'sessions': 13, 'first_used': '2020-04-21T07:12:44.413Z', 'last_used': '2021-09-28T07:01:01.516Z'}]}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False


    def test_equal_extract_last_used_attribute(self):
        self.sduj.dupe_records = [self.user1, self.user2]
        self.sduj.extract_last_used_attribute()
        for record in self.sduj.dupe_records:
            if record['external_id'] == '123-A':
                self.assertEqual(record['last_used'], datetime.strptime('2020-05-24T15:28:33.157Z', '%Y-%m-%dT%H:%M:%S.%fZ'))
            elif record['external_id'] == '789-C':
                self.assertEqual(record['last_used'], None)


    def test_equal_determine_max_last_used(self):
        self.sduj.dupe_records = [self.user1, self.user2, self.user3]
        self.sduj.extract_last_used_attribute()
        self.sduj.determine_max_last_used()
        for record in self.sduj.dupe_records:
            if record['external_id'] in ['123-A', '789-C']:
                self.assertEqual(record['max_last_used'], None)
            elif record['external_id'] == '845-C':
                self.assertEqual(record['max_last_used'], True)


class TestCustomAttributeCountExtraction(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': '123@mail.com', 'custom_attributes': {'username': 'gg_11525', 'member_tier': 'free_member',
                              'stuff_i_like.enable_email': True, 'stuff_i_like.enable_push': True, 'stuff_i_like.enable_sms': False, 'stuff_i_like.enable_web': True,
                              'content.enable_email': True, 'content.enable_push': True, 'content.enable_sms': False, 'content.enable_web': True,
                              'community.enable_email': True, 'community.enable_push': True}}
        self.user2 = {'external_id': '845-A', 'email': '123@mail.com', 'custom_attributes': {'username': 'gg_11525', 'member_tier': 'free_member',
                              'stuff_i_like.enable_email': True, 'stuff_i_like.enable_push': True, 'stuff_i_like.enable_sms': False, 'stuff_i_like.enable_web': True,
                              'content.enable_email': True, 'content.enable_push': True, 'content.enable_sms': False, 'content.enable_web': True}}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False
        self.sduj.dupe_records = [self.user1, self.user2]


    def test_equal_extract_custom_attribute_count(self):
        self.sduj.extract_custom_attribute_count()
        for record in self.sduj.dupe_records:
            if record['external_id'] == '123-A':
                self.assertEqual(record['custom_attribute_count'], 12)


    def test_equal_determine_max_custom_attributes(self):
        self.sduj.extract_custom_attribute_count()
        self.sduj.determine_max_custom_attributes()
        for record in self.sduj.dupe_records:
            if record['external_id'] == '123-A':
                self.assertEqual(record['max_custom_attributes'], True)
            elif record['external_id'] == '845-A':
                self.assertEqual(record['max_custom_attributes'], None)


class TestCustomEventExtraction(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A',
                      'email': '123@mail.com',
                      'custom_events': [{'name': 'Episode Watched', 'first': '2021-05-25T01:39:44.133Z', 'last': '2021-05-25T02:45:46.985Z', 'count': 2},
                                        {'name': 'RT Episode Watched', 'first': '2020-07-25T01:39:44.113Z', 'last': '2020-07-25T02:31:45.182Z', 'count': 7}]}
        self.user2 = {'external_id': '845-A', 'email': '123@mail.com', 'custom_events': None}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False


    def test_equal_extract_custom_events_into_dict(self):
        self.sduj.dupe_records = [self.user1, self.user2]
        self.sduj.extract_custom_events_into_dict()
        expected_output = {'Episode Watched': {'name': 'Episode Watched', 'first': '2021-05-25T01:39:44.133Z', 'last': '2021-05-25T02:45:46.985Z', 'count': 2},
                           'RT Episode Watched': {'name': 'RT Episode Watched', 'first': '2020-07-25T01:39:44.113Z', 'last': '2020-07-25T02:31:45.182Z', 'count': 7}}
        for record in self.sduj.dupe_records:
            if record['external_id'] == '123-A':
                self.assertEqual(record['custom_events_dict'], expected_output)
            elif record['external_id'] == '845-A':
                self.assertEqual(record['custom_events_dict'], {})


class TestSelectingRecords(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': '123@mail.com', 'rooster_teeth_id': '123-A', 'max_last_used': True}
        self.user2 = {'external_id': '456-A', 'email': '123@mail.com', 'rooster_teeth_id': '456-A', 'max_last_used': None}
        self.user3 = {'external_id': '123-A', 'email': '123@mail.com', 'rooster_teeth_id': '123-A', 'max_last_used': None, 'max_custom_attributes': None}
        self.user4 = {'external_id': '456-A', 'email': '123@mail.com', 'rooster_teeth_id': '456-A', 'max_last_used': None, 'max_custom_attributes': True}
        self.user5 = {'external_id': '123-A', 'email': '123@mail.com', 'rooster_teeth_id': '123-A', 'max_last_used': None, 'max_custom_attributes': None}
        self.user6 = {'external_id': '456-A', 'email': '123@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': None}
        self.user7 = {'external_id': '123-A', 'email': '123@mail.com', 'rooster_teeth_id': None, 'max_last_used': True}
        self.user8 = {'external_id': '456-A', 'email': '123@mail.com', 'rooster_teeth_id': None, 'max_last_used': None}
        self.user9 = {'external_id': '123-A', 'email': '123@mail.com', 'rooster_teeth_id': None, 'max_last_used': True, 'max_custom_attributes': None}
        self.user10 = {'external_id': '456-A', 'email': '123@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': True}
        self.user11 = {'external_id': '123-A', 'email': '123@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': None}
        self.user12 = {'external_id': '456-A', 'email': '123@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': None}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False


    def test_equal_rt_id_and_last_used(self):
        self.sduj.dupe_records = [self.user1, self.user2]
        self.sduj.select_records_to_update()
        for email, record in self.sduj.records_to_update.items():
            if email == '123@gmail.com':
                self.assertEqual(record['external_id'], '123-A')


    def test_equal_rt_id_and_custom_attribute_count(self):
        self.sduj.dupe_records = [self.user3, self.user4]
        self.sduj.select_records_to_update()
        for email, record in self.sduj.records_to_update.items():
            if email == '123@gmail.com':
                self.assertEqual(record['external_id'], '456-A')


    def test_equal_just_rt_id(self):
        self.sduj.dupe_records = [self.user5, self.user6]
        self.sduj.select_records_to_update()
        for email, record in self.sduj.records_to_update.items():
            if email == '123@gmail.com':
                self.assertEqual(record['external_id'], '123-A')


    def test_equal_just_last_used(self):
        self.sduj.dupe_records = [self.user7, self.user8]
        self.sduj.select_records_to_update()
        for email, record in self.sduj.records_to_update.items():
            if email == '123@gmail.com':
                self.assertEqual(record['external_id'], '123-A')


    def test_equal_just_custom_attribute_count(self):
        self.sduj.dupe_records = [self.user9, self.user10]
        self.sduj.select_records_to_update()
        for email, record in self.sduj.records_to_update.items():
            if email == '123@gmail.com':
                self.assertEqual(record['external_id'], '456-A')


    def test_equal_no_identifiable_atributes(self):
        self.sduj.dupe_records = [self.user11, self.user12]
        self.sduj.select_records_to_update()
        for email, record in self.sduj.records_to_update.items():
            if email == '123@gmail.com':
                self.assertEqual(record['external_id'], '123-A')


class TestMissingRecordException(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': 'A@mail.com', 'rooster_teeth_id': '123-A', 'max_last_used': True}
        self.user2 = {'external_id': '456-A', 'email': 'A@mail.com', 'rooster_teeth_id': '456-A', 'max_last_used': None}
        self.user3 = {'external_id': '123-B', 'email': 'B@mail.com', 'rooster_teeth_id': '123-B', 'max_last_used': None, 'max_custom_attributes': None}
        self.user4 = {'external_id': '456-B', 'email': 'B@mail.com', 'rooster_teeth_id': '456-B', 'max_last_used': None, 'max_custom_attributes': True}
        self.user5 = {'external_id': '123-C', 'email': 'C@mail.com', 'rooster_teeth_id': '123-C', 'max_last_used': None, 'max_custom_attributes': None}
        self.user6 = {'external_id': '456-C', 'email': 'C@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': None}
        self.user7 = {'external_id': '123-D', 'email': 'D@mail.com', 'rooster_teeth_id': None, 'max_last_used': True}
        self.user8 = {'external_id': '456-D', 'email': 'D@mail.com', 'rooster_teeth_id': None, 'max_last_used': None}
        self.user9 = {'external_id': '123-E', 'email': 'E@mail.com', 'rooster_teeth_id': None, 'max_last_used': True, 'max_custom_attributes': None}
        self.user10 = {'external_id': '456-E', 'email': 'E@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': True}
        self.user11 = {'external_id': '123-F', 'email': 'F@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': None}
        self.user12 = {'external_id': '456-F', 'email': 'F@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': None}
        self.user13 = {'external_id': '456-G', 'email': 'G@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': None}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False
        self.sduj.dupe_records = [self.user1, self.user2, self.user3, self.user4, self.user5, self.user6, self.user7, self.user8, self.user9, self.user10, self.user11, self.user12]
        self.sduj.select_records_to_update()


    def test_raises_check_for_missing_records_to_update(self):
        self.sduj.dupe_records.append(self.user13)
        self.assertRaises(MissingBrazeRecordsException, self.sduj.check_for_missing_records_to_update)


class TestRecordsToRemove(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': 'A@mail.com', 'rooster_teeth_id': '123-A', 'max_last_used': True}
        self.user2 = {'external_id': '456-A', 'email': 'A@mail.com', 'rooster_teeth_id': '456-A', 'max_last_used': None}
        self.user3 = {'external_id': '123-B', 'email': 'B@mail.com', 'rooster_teeth_id': '123-B', 'max_last_used': None, 'max_custom_attributes': None}
        self.user4 = {'external_id': '456-B', 'email': 'B@mail.com', 'rooster_teeth_id': '456-B', 'max_last_used': None, 'max_custom_attributes': True}
        self.user13 = {'external_id': '789-B', 'email': 'B@mail.com', 'rooster_teeth_id': None, 'max_last_used': True, 'max_custom_attributes': True}
        self.user5 = {'external_id': '123-C', 'email': 'C@mail.com', 'rooster_teeth_id': '123-C', 'max_last_used': None, 'max_custom_attributes': None}
        self.user6 = {'external_id': '456-C', 'email': 'C@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': None}
        self.user7 = {'external_id': '123-D', 'email': 'D@mail.com', 'rooster_teeth_id': None, 'max_last_used': True}
        self.user8 = {'external_id': '456-D', 'email': 'D@mail.com', 'rooster_teeth_id': None, 'max_last_used': None}
        self.user9 = {'external_id': '123-E', 'email': 'E@mail.com', 'rooster_teeth_id': None, 'max_last_used': True, 'max_custom_attributes': None}
        self.user10 = {'external_id': '456-E', 'email': 'E@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': True}
        self.user14 = {'external_id': '789-E', 'email': 'E@mail.com', 'rooster_teeth_id': None, 'max_last_used': True, 'max_custom_attributes': True}
        self.user11 = {'external_id': '123-F', 'email': 'F@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': None}
        self.user12 = {'external_id': '456-F', 'email': 'F@mail.com', 'rooster_teeth_id': None, 'max_last_used': None, 'max_custom_attributes': None}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False
        self.sduj.dupe_records = [self.user1, self.user2, self.user3, self.user4, self.user5, self.user6, self.user7, self.user8, self.user9, self.user10, self.user11, self.user12, self.user13]
        self.sduj.select_records_to_update()
        self.sduj.select_records_to_remove()


    def test_in_select_records_to_remove(self):
        for record in self.sduj.records_to_remove:
            self.assertIn(record['external_id'], ['456-A', '123-B', '789-B', '456-C', '456-D', '123-E', '456-E', '456-F'])


    def test_not_in_select_records_to_remove(self):
        for record in self.sduj.records_to_remove:
            self.assertNotIn(record['external_id'], ['123-A', '456-B', '123-C', '123-D', '789-E', '123-E', '123-F'])


class TestNewCustomAttributes(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': 'A@mail.com', 'custom_attributes': {'member_tier': 'first_member', 'stuff_i_like.enable_email': True, 'stuff_i_like.enable_push': True,
                                                                                           'stuff_i_like.enable_sms': False, 'stuff_i_like.enable_web': True, 'content.enable_email': True}}
        self.user2 = {'external_id': '456-A', 'email': 'A@mail.com', 'custom_attributes': {'content.enable_push': True, 'content.enable_sms': False, 'content.enable_web': True}}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False
        self.sduj.records_to_update = {}
        self.sduj.records_to_update[self.user1['email']] = self.user1
        self.sduj.records_to_remove = [self.user2]
        self.sduj.append_new_custom_attributes()


    def test_in_append_new_custom_attributes(self):
        record_to_update = self.sduj.records_to_update[self.user1['email']]
        self.assertIn('content.enable_push', record_to_update['new_custom_attributes'].keys())
        self.assertIn('content.enable_sms', record_to_update['new_custom_attributes'].keys())
        self.assertIn('content.enable_web', record_to_update['new_custom_attributes'].keys())
        self.assertNotIn('member_tier', record_to_update['new_custom_attributes'].keys())


class TestAppendUpdateNewCustomEvents(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': '123@mail.com', 'custom_events_dict': {'Episode Watched': {'name': 'Episode Watched', 'first': '2021-07-21T01:39:44.133Z', 'last': '2021-07-22T02:45:46.985Z', 'count': 2},
                                                                                              'RT Episode Watched': {'name': 'RT Episode Watched', 'first': '2020-07-25T01:39:44.113Z', 'last': '2020-07-25T02:31:45.182Z', 'count': 7}}}
        self.user2 = {'external_id': '845-A', 'email': '123@mail.com', 'custom_events_dict': {'Episode Watched': {'name': 'Episode Watched', 'first': '2021-08-21T01:39:44.133Z', 'last': '2021-09-22T02:45:46.985Z', 'count': 5},
                                                                                         'New Account Created': {'name': 'New Account Created','first': '2020-04-17T04:50:34.565Z','last': '2020-04-17T04:50:34.565Z','count': 1}}}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False
        self.sduj.records_to_update = {}
        self.sduj.records_to_update[self.user1['email']] = self.user1
        self.sduj.records_to_remove = [self.user2]
        self.sduj.append_update_new_custom_event()


    def test_equal_append_update_new_custom_event(self):
        record_to_update = self.sduj.records_to_update[self.user1['email']]
        self.assertEqual(record_to_update['new_custom_events_dict']['Episode Watched']['time'], datetime.strptime('2021-07-21T01:39:44.133Z', '%Y-%m-%dT%H:%M:%S.%fZ').isoformat())
        self.assertEqual(record_to_update['new_custom_events_dict']['Episode Watched']['properties']['last'], datetime.strptime('2021-09-22T02:45:46.985Z', '%Y-%m-%dT%H:%M:%S.%fZ').isoformat())
        self.assertEqual(record_to_update['new_custom_events_dict']['Episode Watched']['properties']['count'], 5)
        self.assertNotIn('RT Episode Watched', record_to_update['new_custom_events_dict'].keys())


class TestAppendNewPurchases(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': 'A@mail.com', 'purchases': None}
        self.user2 = {'external_id': '456-A', 'email': 'A@mail.com', 'purchases': [{'name': '4352','first': '2021-10-03T08:23:34.000Z','last': '2021-10-03T08:23:34.000Z','count': 1}]}
        self.user3 = {'external_id': '123-B', 'email': 'B@mail.com', 'purchases': [{'name': '712688','first': '2021-10-03T08:23:34.000Z','last': '2021-10-03T08:23:34.000Z','count': 1}]}
        self.user4 = {'external_id': '456-B', 'email': 'B@mail.com', 'purchases': None}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False
        self.sduj.records_to_update = {}
        self.sduj.records_to_update[self.user1['email']] = self.user1
        self.sduj.records_to_update[self.user3['email']] = self.user3
        self.sduj.records_to_remove = [self.user2, self.user4]
        self.sduj.append_new_purchases()


    def test_equal_append_new_purchases(self):
        record_to_update = self.sduj.records_to_update[self.user1['email']]
        self.assertEqual(record_to_update['new_purchases'][0]['product_id'], '4352')
        self.assertEqual(record_to_update['new_purchases'][0]['time'],  datetime.strptime('2021-10-03T08:23:34.000Z', '%Y-%m-%dT%H:%M:%S.%fZ').isoformat())


    def test_in_append_new_purchases(self):
        record_to_update = self.sduj.records_to_update[self.user1['email']]
        self.assertIn('app_id', record_to_update['new_purchases'][0].keys())
        self.assertIn('price', record_to_update['new_purchases'][0].keys())
        self.assertIn('currency', record_to_update['new_purchases'][0].keys())


    def test_equal_null_new_purchases(self):
        record_to_update = self.sduj.records_to_update[self.user3['email']]
        self.assertEqual(record_to_update['new_purchases'], [])


class TestUpdateRevenue(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': 'A@mail.com', 'total_revenue': 24.7}
        self.user2 = {'external_id': '456-A', 'email': 'A@mail.com', 'total_revenue': 8.0}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False
        self.sduj.records_to_update = {}
        self.sduj.records_to_update[self.user1['email']] = self.user1
        self.sduj.records_to_remove = [self.user2]
        self.sduj.update_total_revenue()


    def test_equal_update_total_revenue(self):
        record_to_update = self.sduj.records_to_update[self.user1['email']]
        self.assertEqual(record_to_update['new_total_revenue'], 32.7)


class TestBuildAttributeOutputDataStructures(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': 'A@mail.com', 'new_total_revenue': 24.7, 'new_custom_attributes': {'content.enable_push': True, 'content.enable_sms': False, 'content.enable_web': True}}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False
        self.sduj.records_to_update = {}
        self.sduj.records_to_update[self.user1['email']] = self.user1
        self.sduj.build_attributes_output_data_structure()


    def test_equal_build_attributes_output_data_structure(self):
        record_to_update = self.sduj.attribute_updates[0]
        self.assertEqual(record_to_update, {'external_id': '123-A', 'content.enable_push': True, 'content.enable_sms': False, 'content.enable_web': True, 'total_revenue': 24.7})


class TestBuildEventOutputDataStructures(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-B', 'email': 'B@mail.com', 'new_custom_events_dict': {'Episode Watched': {'name': 'Episode Watched', 'time': datetime.strptime('2021-07-21T01:39:44.133Z', '%Y-%m-%dT%H:%M:%S.%fZ').isoformat(), 'properties': {'last': datetime.strptime('2021-07-22T02:45:46.985Z', '%Y-%m-%dT%H:%M:%S.%fZ').isoformat(), 'count': 2}},
                                                                                              'RT Episode Watched': {'name': 'RT Episode Watched', 'time': datetime.strptime('2020-07-25T01:39:44.113Z', '%Y-%m-%dT%H:%M:%S.%fZ').isoformat(), 'properties': {'last': datetime.strptime('2020-07-25T02:31:45.182Z', '%Y-%m-%dT%H:%M:%S.%fZ').isoformat(), 'count': 7}}}}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False
        self.sduj.records_to_update = {}
        self.sduj.records_to_update[self.user1['email']] = self.user1
        self.sduj.build_events_output_data_structure()


    def test_in_build_events_output_data_structure(self):
        self.assertIn({'external_id': '123-B', 'name': 'Episode Watched', 'time': '2021-07-21T01:39:44.133000', 'properties': {'last': '2021-07-22T02:45:46.985000', 'count': 2}}, self.sduj.event_updates)


    def test_equal_build_events_output_data_structure(self):
        self.assertEqual(len(self.sduj.event_updates), 9)
        rt_count = 0
        for event in self.sduj.event_updates:
            if event['name'] == 'RT Episode Watched':
                rt_count += 1
        self.assertEqual(rt_count, 7)
        id_count = 0
        for event in self.sduj.event_updates:
            if 'external_id' in event:
                id_count += 1
        self.assertEqual(id_count, 9)


class TestBuildPurchaseOutputDataStructure(unittest.TestCase):


    def setUp(self):
        self.user1 = {'external_id': '123-A', 'email': 'A@mail.com', 'new_total_revenue': 24.7, 'new_purchases': [{'product_id': '712688', 'app_id': 123, 'price': 0.0, 'currency': 'USD', 'time': '2021-10-03T08:23:34.000Z', 'properties': {'last': '2021-10-03T08:23:34.000Z', 'count': 1}}]}
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False
        self.sduj.records_to_update = {}
        self.sduj.records_to_update[self.user1['email']] = self.user1
        self.sduj.build_purchases_output_data_structure()


    def test_in_build_purchases_output_data_structure(self):
        record_to_update = self.sduj.purchase_updates[0]
        self.assertEqual(record_to_update, {'external_id': '123-A', 'product_id': '712688', 'app_id': 123, 'price': 0.0, 'currency': 'USD', 'time': '2021-10-03T08:23:34.000Z', 'properties': {'last': '2021-10-03T08:23:34.000Z', 'count': 1}})


class TestUpdatedAttributeBatching(unittest.TestCase):

    def setUp(self):
        self.sduj = ShopifyDedupeUserJob()
        self.sduj.logger.enabled = False
        self.number_of_attributes_per_update = randint(2, 5)
        self.number_of_updates = randint(self.sduj.UPDATE_USER_BATCH_SIZE, self.sduj.UPDATE_USER_BATCH_SIZE*10)
        self.total_number_of_updates = self.number_of_attributes_per_update * self.number_of_updates
        self.sduj.attribute_updates = []
        for i in range(self.number_of_updates):
            data_dict = {}
            for j in range(1, self.number_of_attributes_per_update+1):
                data_dict[f'attribute{j}'] = j
            self.sduj.attribute_updates.append(data_dict)
        self.sduj.create_attribute_batches()


    def test_less_equal_create_attribute_batches(self):
        for batch in self.sduj.attribute_batches:
            self.assertLessEqual(len(batch), self.sduj.UPDATE_USER_BATCH_SIZE)


    def test_equal_create_attribute_batches(self):
        count = 0
        for batch in self.sduj.attribute_batches:
            for updates in batch:
                count += len(updates)
        self.assertEqual(count, self.total_number_of_updates)



if __name__ == '__main__':
    unittest.main()



