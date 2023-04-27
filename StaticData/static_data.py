import os
from pyspark.sql import DataFrame as pys_dataframe
import utils

path = os.getcwd()
parent_directory = os.path.dirname(path)
current_directory = path

spark = utils.establish_spark_session()


class StaticData:
    def __init__(self):
        self._source_dataset = spark.read.csv(
            f"{current_directory}/StaticData/sales_data.tsv",
            sep=r"\t",
            header=True,
            inferSchema=True,
        )

        self.computed_labels = {
            "units_sum_by_product_w_store": "units_sum_by_product_w_store",
            "units_sum_by_store": "units_sum_by_store",
            "sales_profile": "sales_profile",
            "product_name": "_product_name",
        }

        self.join_type = "inner"
        self.output_file_name = "sales_profile_by_store"

    def get_source_dataset(self) -> pys_dataframe:
        return self._source_dataset

    def get_source_dataset_labels(self) -> dict:
        if self._source_dataset is not None:
            if len(self._source_dataset.columns) > 0:
                temp_dict = {}

                for label in self._source_dataset.columns:
                    temp_dict[label] = label

                return temp_dict

    def get_user_message_format(self):
        return "\n*****************************************************\n"
