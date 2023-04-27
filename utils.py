import os, json, csv
from typing import Dict, List
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as pys_dataframe
from pandas import DataFrame as df

path = os.getcwd()
parent_directory = os.path.dirname(path)
current_directory = path


def establish_spark_session():
    print(
        f"\n***************************\nCreating spark session...\n***************************\n"
    )
    return SparkSession.builder.appName("Sales Profiles").getOrCreate()


def validate_input(sys_argv: List = []) -> bool:
    for input_parameter in sys_argv:
        if not str(input_parameter).isdigit():
            return False

    return True


def get_distinct_store_ids(sales_data_pys_df: pys_dataframe = None):
    if sales_data_pys_df is not None:
        return sales_data_pys_df.select("store_id").distinct().collect()


def save_data(
    resultset: Dict = None,
    resultset_df: df = None,
    filename: str = "",
    is_test_enabled=False,
) -> bool:
    if is_test_enabled:
        filename = "test_" + filename

    print(filename)

    if resultset is not None and resultset_df is not None and filename != "":
        with open(
            f"{current_directory}\OutputFiles\{filename}.json", "w"
        ) as json_out_file:
            json.dump(resultset, json_out_file, sort_keys=True, indent=4)

        resultset_df.to_csv(
            f"{current_directory}\OutputFiles\PowerBI\{filename}.csv",
            sep=",",
            index=False,
        )

        return True

    else:
        return False
