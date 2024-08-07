from datetime import datetime, timedelta
import time


class Dater():

	def get_today(self):
		return datetime.now().strftime("%Y%m%d")
	

	def format_date(self, date):
		return f"{date[:4]}-{date[4:6]}-{date[-2:]}"


	def find_previous_day(self, start_date):
		start_year = int(start_date[:4])
		start_month = int(start_date[4:6])
		start_day = int(start_date[-2:])
		days_in_month = self.days_in_month(start_month, start_year)

		if start_day == 1:
			if start_month == 1:
				return f"{start_year - 1}1231"
			return f"{start_year}{str(start_month - 1).zfill(2)}{self.days_in_month(start_month - 1, start_year)}"

		return f"{start_year}{str(start_month).zfill(2)}{str(start_day - 1).zfill(2)}"


	def find_next_day(self, start_date):
		start_year = int(start_date[:4])
		start_month = int(start_date[4:6])
		start_day = int(start_date[-2:])
		days_in_month = self.days_in_month(start_month, start_year)
		if start_day == days_in_month:
			if start_month == 12:
				return f"{start_year + 1}0101"
			return f"{start_year}{str(start_month + 1).zfill(2)}01"
		return f"{start_year}{str(start_month).zfill(2)}{str(start_day + 1).zfill(2)}"


	def find_x_days_ago(self, start_date, days_back):
		idx = 0
		pointer_date = start_date
		while idx < days_back:
			pointer_date = self.find_previous_day(pointer_date)
			idx += 1
		return pointer_date


	def find_x_days_ahead(self, start_date, days_ahead):
		idx = 0
		pointer_date = start_date
		while idx < days_ahead:
			pointer_date = self.find_next_day(pointer_date)
			idx += 1
		return pointer_date


	def get_end_of_month(self, date):
		year = date[:4]
		month = date[4:6]
		day = str(self.days_in_month( int(date[4:6]), int(date[:4]) )).zfill(2)

		return f"{year}{month}{day}"


	def get_start_of_month(self, date):
		year = date[:4]
		month = date[4:6]
		day = '01'

		return f"{year}{month}{day}"


	def get_previous_month(self, date):
		year = date[:4]
		if int(date[4:6]) > 1:
			month_int = int(date[4:6]) - 1
		else:
			month_int = 12
			year = str(int(year) - 1)
		month = str(month_int).zfill(2)
		day = date[-2:]

		return f"{year}{month}{day}"


	def days_in_month(self, month, year):
		if month == 2:
			if year in [2012, 2016, 2020, 2024, 2028]:
				return 29
			return 28
		if month in [1,3,5,7,8,10,12]:
			return 31
		return 30


	def convert_to_unix_timestamp(self, date, date_format='%Y-%m-%d'):
		dt = datetime.strptime(date, date_format)
		dt = dt + timedelta(hours=7)
		return int(time.mktime(dt.timetuple()) * 1000)
