import os, json
import findspark
import pandas as pd

import main, utils
from transformation import Transformation
from StaticData import static_data


findspark.init()
path = os.getcwd()
parent_directory = os.path.dirname(path)
current_directory = path


class TestDataProcessingPipeline:
    def test_main(self):
        file_exists = os.system("python main.py")

        assert file_exists == 0
        assert main.process(["1", "2"], True) is None
        assert main.process([]) is None
        assert main.process(["a", "b", "c"]) is None

    def test_utils(self):
        spark = utils.establish_spark_session()
        assert spark is not None

        with open(
            f"{current_directory}/OutputFiles/sales_profile_by_store.json", "r"
        ) as file:
            data_file = json.load(file)

        status = utils.save_data(
            data_file,
            pd.DataFrame(
                pd.read_csv(
                    f"{current_directory}/OutputFiles/PowerBI/sales_profile_by_store.csv"
                )
            ),
            "sales_profile_by_store",
            True,
        )
        assert status == True

        status = utils.save_data()
        assert status == False

    def test_static_data(self):
        static_data_obj = static_data.StaticData()
        static_data_file = static_data_obj.get_source_dataset()

        assert static_data_file.count() > 0
        assert static_data_obj.get_user_message_format() is not None
        assert static_data_obj.get_source_dataset_labels() is not None

    def test_transformation(self):
        static_data_obj = static_data.StaticData()
        static_data_file = static_data_obj.get_source_dataset()

        transformation_obj = Transformation(static_data_file, [1], True)
        assert transformation_obj.validate_store_id() == True
        assert transformation_obj.process_data_pipeline() == True

        transformation_obj = Transformation(static_data_file, [100], True)
        assert transformation_obj.validate_store_id() == False
        assert transformation_obj.process_data_pipeline() == False

        transformation_obj = Transformation(None, [100], True)
        assert transformation_obj.process_data_pipeline() == False

        transformation_obj = Transformation(None, [], True)
        assert transformation_obj.source_data_lookup() is None
        assert transformation_obj.compute_sales_profile_per_store() is None
