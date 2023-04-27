from typing import List
import pandas as pd
from pyspark.sql.functions import col, regexp_replace, when
from pyspark.sql import DataFrame as pys_dataframe
from pyspark.sql.types import *

import utils
from StaticData import static_data

static_data_obj = static_data.StaticData()


class Transformation:
    def __init__(
        self,
        sales_data_pys_df: pys_dataframe = None,
        input_store_id_list: List = None,
        is_test_enabled=False,
    ):
        self._sales_data_pys_df = sales_data_pys_df
        self._input_store_id_list = input_store_id_list
        self._source_dataset_lookup = pd.DataFrame()
        self._compute_sales_profile_df = pd.DataFrame()
        self._join_type = static_data_obj.join_type
        self.output_file_name = static_data_obj.output_file_name
        self._is_test_enabled = is_test_enabled

        self._store_id_label = str(
            static_data_obj.get_source_dataset_labels().get("store_id", "")
        )
        self._product_id_label = str(
            static_data_obj.get_source_dataset_labels().get("product_id", "")
        )
        self._product_name_label = str(
            static_data_obj.get_source_dataset_labels().get("product_name", "")
        )
        self._units_label = str(
            static_data_obj.get_source_dataset_labels().get("units", "")
        )
        self._price_label = str(
            static_data_obj.get_source_dataset_labels().get("price", "")
        )
        self._units_sum_by_product_w_store = str(
            static_data_obj.computed_labels.get("units_sum_by_product_w_store", "")
        )
        self._units_sum_by_store = str(
            static_data_obj.computed_labels.get("units_sum_by_store", "")
        )
        self._sales_profile = str(
            static_data_obj.computed_labels.get("sales_profile", "")
        )
        self._product_name = str(
            static_data_obj.computed_labels.get("product_name", "")
        )

    def validate_store_id(self):
        if self._sales_data_pys_df is not None:
            if self._store_id_label in self._sales_data_pys_df.columns:
                temp_list = []

                for row in utils.get_distinct_store_ids(self._sales_data_pys_df):
                    temp_list.append(row[self._store_id_label])

                if set(self._input_store_id_list) & set(temp_list):
                    return True
                else:
                    print(
                        f"{static_data_obj.get_user_message_format()}The provided 'STORE ID' value(s) doesn't exist in the source dataset. Provide a valid 'STORE ID' to proceed.{static_data_obj.get_user_message_format()}"
                    )
                    return False

    def source_data_lookup(self, temp_processing_df: pd.DataFrame = None):
        if temp_processing_df is not None:
            return (
                (
                    temp_processing_df.select(
                        self._store_id_label, self._product_id_label, self._product_name
                    )
                    .distinct()
                    .orderBy(self._product_id_label)
                    .toPandas()
                )
                if len(self._source_dataset_lookup.columns) == 0
                else self._source_dataset_lookup._append(
                    temp_processing_df.select(
                        self._store_id_label, self._product_id_label, self._product_name
                    )
                    .distinct()
                    .orderBy(self._product_id_label)
                    .toPandas()
                )
            )

    def compute_sales_profile_per_store(
        self,
        temp_processing_df: pys_dataframe = None,
        source_dataset_lookup: pd.DataFrame = None,
    ):
        if temp_processing_df is not None and source_dataset_lookup is not None:
            return (
                source_dataset_lookup.merge(
                    temp_processing_df.groupBy(
                        self._product_id_label, self._store_id_label
                    )
                    .sum(self._units_label)
                    .toPandas(),
                    how=self._join_type,
                    on=[self._product_id_label, self._store_id_label],
                ).merge(
                    temp_processing_df.groupBy(self._store_id_label)
                    .sum(self._units_label)
                    .toPandas(),
                    how=self._join_type,
                    on=self._store_id_label,
                )
                if len(self._compute_sales_profile_df.columns) == 0
                else self._compute_sales_profile_df._append(
                    source_dataset_lookup.merge(
                        temp_processing_df.groupBy(
                            self._product_id_label, self._store_id_label
                        )
                        .sum(self._units_label)
                        .toPandas(),
                        how=self._join_type,
                        on=[self._product_id_label, self._store_id_label],
                    ).merge(
                        temp_processing_df.groupBy(self._store_id_label)
                        .sum(self._units_label)
                        .toPandas(),
                        how=self._join_type,
                        on=self._store_id_label,
                    )
                )
            )

    def start_sales_profile_compute_engine(self):
        if self._compute_sales_profile_df is not None:
            return (
                self._compute_sales_profile_df.assign(
                    sales_profile=round(
                        self._compute_sales_profile_df[
                            self._units_sum_by_product_w_store
                        ]
                        / self._compute_sales_profile_df[self._units_sum_by_store],
                        2,
                    )
                )
                .sort_values(by=[self._store_id_label, self._product_name_label])
                .drop(
                    [
                        self._product_id_label,
                        self._units_sum_by_product_w_store,
                        self._units_sum_by_store,
                    ],
                    axis=1,
                )
            )

    def produce_transformed_resultset(self) -> bool:
        resultset_dict = {}
        if self._compute_sales_profile_df is not None:
            for store_id in set(self._compute_sales_profile_df[self._store_id_label]):
                temp_dict = {}
                for index, row in self._compute_sales_profile_df.iterrows():
                    if row[self._store_id_label] == store_id:
                        temp_dict.update(
                            {
                                f"{row[self._product_name_label]}": row[
                                    self._sales_profile
                                ]
                            }
                        )

                resultset_dict[store_id] = temp_dict

            return (
                utils.save_data(
                    resultset_dict,
                    self._compute_sales_profile_df,
                    self.output_file_name,
                    self._is_test_enabled,
                )
                if len(resultset_dict) > 0
                else False
            )

    def process_data_pipeline(self) -> bool:
        if self._sales_data_pys_df is not None:
            if self._store_id_label in self._sales_data_pys_df.columns:
                for item in self._input_store_id_list:
                    temp_processing_df = None
                    temp_processing_df = self._sales_data_pys_df.filter(
                        col(self._store_id_label) == int(item)
                    ).filter(col(self._units_label) >= 0)

                    temp_processing_df = temp_processing_df.withColumn(
                        self._product_name,
                        when(
                            ~temp_processing_df[self._product_name_label].rlike(
                                "^[a-zA-Z0-9 ]+$"
                            ),
                            regexp_replace(
                                temp_processing_df[self._product_name_label],
                                "[^a-zA-Z0-9 ]+",
                                " ",
                            ),
                        ).otherwise(temp_processing_df[self._product_name_label]),
                    )

                    self._source_dataset_lookup = self.source_data_lookup(
                        temp_processing_df
                    )

                    self._compute_sales_profile_df = (
                        self.compute_sales_profile_per_store(
                            temp_processing_df, self._source_dataset_lookup
                        )
                    )

                self._compute_sales_profile_df.rename(
                    columns={
                        "sum(units)_x": self._units_sum_by_product_w_store,
                        "sum(units)_y": self._units_sum_by_store,
                        "_product_name": self._product_name_label,
                    },
                    inplace=True,
                )

                self._compute_sales_profile_df = (
                    self.start_sales_profile_compute_engine()
                )

                status = self.produce_transformed_resultset()

                return status

        else:
            return False
