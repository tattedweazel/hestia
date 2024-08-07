import gzip
import json
import os
import shutil
from zipfile import ZipFile, is_zipfile


class ExtractFromZip:

    def __init__(self, full_file_path, directory, extracted_file_name=None, clean_up=True):
        self.clean_up = clean_up
        self.full_file_path = full_file_path
        self.directory = directory
        self.extracted_file_name = extracted_file_name
        self.files = []
        self.records = []


    def extract_files_from_download(self):
        if is_zipfile(self.full_file_path):
            with ZipFile(self.full_file_path, 'r') as f:
                f.extractall(self.directory)
        else:
            with gzip.open(self.full_file_path, 'rb') as f_in:
                output_file_path = '/'.join([self.directory, self.extracted_file_name])
                with open(output_file_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)


    def populate_records_from_extracted_files(self):
        if self.extracted_file_name is None:
            for filename in os.listdir(self.directory):
                fileloc = os.path.join(self.directory, filename)
                self.files.append(fileloc)
        else:
            self.files = [os.path.join(self.directory, self.extracted_file_name)]

        for filename in self.files:
            if os.path.splitext(filename)[1] in ['.txt', '.json']:
                with open(filename, 'rb') as f:
                    lines = f.readlines()
                    for line in lines:
                        self.records.append(json.loads(line))


    def clean_up_directory(self):
        if self.clean_up:
            if len(os.listdir(f'{self.directory}/')) > 1:
                os.system(f'rm {self.directory}/*.*')
            else:
                pass


    def execute(self):
        if self.directory:
            self.clean_up_directory()
            self.extract_files_from_download()
            self.populate_records_from_extracted_files()
            self.clean_up_directory()
        return self.records
