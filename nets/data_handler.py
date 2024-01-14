import datetime as dt
import json
from collections import Counter

import pandas as pd
# import numpy as np


class FeaturesBlueprint:

    def __init__(self,  feature_funcs: list[callable]):
        self.__feature_funcs: list[tuple[callable, tuple, int]] = feature_funcs

    def __call__(self, dataframe: pd.DataFrame):
        for func, arguments, required_periods in self.__feature_funcs:
            func(dataframe[-required_periods:], *arguments)


# pandas DataFrame wrapper class
class DataHandler:

    def __init__(
        self, features_blueprint: FeaturesBlueprint,
        raw_data_fields: dict[str, type], dataframe: pd.DataFrame
    ):
        self.__features_blueprint: FeaturesBlueprint = features_blueprint
        self.__raw_data_fields: dict[str, type] = raw_data_fields
        self.__feature_counter: Counter = Counter(raw_data_fields.keys())
        self.__dataframe: pd.DataFrame = dataframe

    def add_json_data(self, json_data: json):
        loaded_json_data: dict = json.loads(json_data)
        if self.__feature_counter != Counter(loaded_json_data.keys()):
            raise ValueError('Unexpected format of input data.')

        for field, data_type in self.__raw_data_fields.items():
            if type(loaded_json_data.get(field)) != data_type:
                raise TypeError(
                    f'"{field}" is not of the expected type "{data_type}".'
                )

        new_data: pd.DataFrame = pd.read_json(
            loaded_json_data, orient='records'
        )
        self.__dataframe = pd.concat(
            [self.__dataframe, new_data], ignore_index=True
        )

        # return self.__dataframe # fluent style or not?

    def apply_features(self):
        self.__features_blueprint(self.__dataframe)


class TimeSeriesDataHandler(DataHandler):

    def __init__(
        self, features_blueprint: FeaturesBlueprint,
        raw_data_fields: dict[str, type], dataframe: pd.DataFrame,
        start_dt: dt.datetime, end_dt: dt.datetime
    ):
        super().__init__(features_blueprint, raw_data_fields, dataframe)
        self.__start_dt: dt.datetime = start_dt
        self.__end_dt: dt.datetime = end_dt
