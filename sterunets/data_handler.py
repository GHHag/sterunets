import datetime as dt
import json
from collections import Counter

import pandas as pd
# import numpy as np


class FeatureBlueprint:

    def __init__(
        self, name: str, feature_func: callable, 
        arguments: tuple, required_periods: int
    ):
        self.__name: str = name
        self.__feature_func: callable = feature_func
        self.__arguments: tuple = arguments
        self.__required_periods: int = required_periods

    @property
    def name(self) -> str:
        return self.__name

    @property
    def feature_func(self) -> callable:
        return self.__feature_func

    @property
    def arguments(self) -> tuple:
        return self.__arguments

    @property
    def required_periods(self) -> int:
        return self.__required_periods

    def __call__(self, dataframe: pd.DataFrame):
        return self.__feature_func(
            dataframe.iloc[:, -self.__required_periods:], 
            *self.__arguments
        )


class DataHandler:

    __feature_blueprints: list[FeatureBlueprint] = []

    def __init__(
        self, raw_data_fields: dict[str, type], dataframe: pd.DataFrame
    ):
        self.__raw_data_fields: dict[str, type] = raw_data_fields
        self.__feature_counter: Counter = Counter(raw_data_fields.keys())
        self.__dataframe: pd.DataFrame = dataframe

    @property
    def dataframe(self):
        return self.__dataframe if self.__dataframe is not None else None

    def add_feature_blueprint(self, feature_blueprint: FeatureBlueprint):
        self.__feature_blueprints.append(feature_blueprint)

    def add_json_data(self, json_data: json):
        loaded_json_data: dict = json.loads(json_data)
        if self.__feature_counter != Counter(loaded_json_data.keys()):
            raise ValueError('Unexpected format of input data.')

        for field, data_type in self.__raw_data_fields.items():
            if type(loaded_json_data.get(field)) != data_type:
                raise TypeError(
                    f'"{field}" is not of the expected type "{data_type}".'
                )

        columns = self.__dataframe.columns
        # TODO: Evaluate if possible to use from_dict method instead to
        # create the new dataframe here.
        new_data = pd.DataFrame([loaded_json_data], columns=columns)
        self.__dataframe = pd.concat(
            [self.__dataframe, new_data], ignore_index=True
        )

    def apply_features(self):
        for feature_blueprint in self.__feature_blueprints:
            # self.__dataframe = feature_blueprint(self.__dataframe)
            feature_blueprint(self.__dataframe)
        print(self.__dataframe)


class TimeSeriesDataHandler(DataHandler):

    def __init__(
        self, raw_data_fields: dict[str, type], dataframe: pd.DataFrame,
        start_dt: dt.datetime, end_dt: dt.datetime
    ):
        super().__init__(raw_data_fields, dataframe)
        self.__start_dt: dt.datetime = start_dt
        self.__end_dt: dt.datetime = end_dt


if __name__ == '__main__':
    pass
