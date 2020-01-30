from utils.datalake_utils import get_path_by_day


class PreProcessExecutor:
    def __init__(self, args, input_data_dir):
        self._args = args
        self._input_data_dir = input_data_dir

    def generate_pivot_file_user_x_category_page(self, spark_session, output_base_dir):
        file = self._input_data_dir + "/" + self._args[2]
        output_path = get_path_by_day(output_base_dir, self._args[3])

        df = spark_session.read.load(file)

        df = df \
            .select("studentId_clientType", "Page_Category") \
            .groupBy("studentId_clientType") \
            .pivot("Page_Category") \
            .count() \
            .na \
            .fill(0)

        df \
            .coalesce(4) \
            .write \
            .mode("overwrite") \
            .parquet(output_path)
