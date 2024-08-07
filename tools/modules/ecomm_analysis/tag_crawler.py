from utils.connectors.database_connector import DatabaseConnector


class TagCrawler:

	def __init__(self):
		self.connector = DatabaseConnector('')
		self.records = []
		self.parsed = {}


	def get_tags(self):
		query = "SELECT product_type, tags FROM warehouse.shopify_products WHERE status = 'active';"
		results = self.connector.read_redshift(query)
		for result in results:
			self.records.append({
				"type": result[0],
				"tags": result[1]
				})


	def parse_tags(self):
		for record in self.records:
			if record['tags']:
				if record['type'] not in self.parsed:
					tag_split = record['tags'].split(',')
					self.parsed[record['type']] = set()
					for tag in tag_split:
						self.parsed[record['type']].add(tag.strip())
				else:
					tag_split = record['tags'].split(',')
					for tag in tag_split:
						self.parsed[record['type']].add(tag.strip())


	def write_to_file(self):
		with open('tools/output/tag_map.txt', 'w') as f:
			for category in self.parsed:
				f.write(f"{category}")
				for tag in self.parsed[category]:
					f.write(f"\n\t-{tag}")
				f.write("\n\n")



	def execute(self):
		self.get_tags()
		self.parse_tags()
		self.write_to_file()