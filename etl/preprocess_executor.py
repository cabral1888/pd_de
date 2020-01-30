from utils.datalake_utils import get_path_by_day


class PreProcessExecutor:
    """
    This class is responsable to run any kind of pre process
    to make data ready to be used by models
    """
    def __init__(self, args, input_data_dir):
        """

        Parameters
        ----------
        args: list
            Program arguments to be used in the application
        input_data_dir
            Path where resides the input data
        """
        self._args = args
        self._input_data_dir = input_data_dir

    def generate_pivot_file_user_x_category_page(self, spark_session, output_base_dir):
        """
        Generaing a pivot table of user x category_page

        Parameters
        ----------
        spark_session: SparkSession
            Spark session object
        output_base_dir: str
            The directory where data will be outputted
        Returns
        -------

        """
        file = self._input_data_dir + "/" + self._args[2]
        output_path = get_path_by_day(output_base_dir, self._args[3])

        df = spark_session.read.load(file)

        df_a = df \
            .select("studentId_clientType", "Page_Category") \
            .groupBy("studentId_clientType") \
            .pivot("Page_Category") \
            .count() \
            .na \
            .fill(0)

        df_b = df \
            .groupBy("studentId_clientType") \
            .count() \
            .withColumnRenamed("studentId_clientType", "studentId_clientType_b") \
            .withColumnRenamed("count", "count_all_access")

        df = df_a.join(df_b, df_a.studentId_clientType == df_b.studentId_clientType_b) \
            .drop("studentId_clientType_b")

        df \
            .coalesce(4) \
            .write \
            .mode("overwrite") \
            .parquet(output_path)
