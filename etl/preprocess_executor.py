from pyspark.sql.functions import *


class PreProcessExecutor:
	__init__(self, args):
		self._args = args

	def generate_pivot_file_user_x_category_page(spark_session):
		file = self._args[2]
		output_path = self._args[3]

		df = spark_session.read.load(file)

		df = df \
			.select("studentId_clientType", "Page_Category") \
			.groupBy("studentId_clientType") \
			.pivot("Page_Category") \
			.count()

		df \
			.write.
			.parquet(output_path)