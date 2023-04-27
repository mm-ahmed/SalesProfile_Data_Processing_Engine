import sys
from typing import List
from transformation import Transformation
from StaticData import static_data
import utils

static_data_obj = static_data.StaticData()


def process(sys_argv: List = [], is_test_enabled=False):
    if len(sys_argv) > 1:
        del sys_argv[0]
        if utils.validate_input(sys_argv):
            input_store_id_list: List = list(map(int, sys_argv))

            static_data_object = static_data.StaticData()
            sales_data_pys_df = static_data_object.get_source_dataset()

            transformation_object = Transformation(
                sales_data_pys_df, input_store_id_list, is_test_enabled
            )
            if transformation_object.validate_store_id():
                if transformation_object.process_data_pipeline():
                    print(
                        f"{static_data_obj.get_user_message_format()}The output resultset is successfully saved in 'sales_profile_by_store.json'..\nNow exiting...{static_data_obj.get_user_message_format()}"
                    )
        else:
            print(
                f"{static_data_obj.get_user_message_format()}All the 'STORE ID' values must be 'integer'.\nNow exiting...{static_data_obj.get_user_message_format()}"
            )

    else:
        print(
            f"{static_data_obj.get_user_message_format()}Atleast one 'STORE ID' is required. For assistance, please follow the instructions in the 'README.txt' file.\nNow exiting...{static_data_obj.get_user_message_format()}"
        )


if __name__ == "__main__":
    process(sys.argv)
