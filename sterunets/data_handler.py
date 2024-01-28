import datetime as dt
import json
from collections import Counter

import pandas as pd


class FeatureBlueprint:

    def __init__(
        self, name: str, feature_func: callable, arguments: tuple,
    ):
        self.__name: str = name
        # TODO: Implement protocol for feature_func required behaviour
        self.__feature_func: callable = feature_func
        self.__arguments: tuple = arguments

    @property
    def name(self) -> str:
        return self.__name

    @property
    def feature_func(self) -> callable:
        return self.__feature_func

    @property
    def arguments(self) -> tuple:
        return self.__arguments

    def __call__(self, dataframe: pd.DataFrame, **kwargs):
        self.__feature_func(dataframe, *self.__arguments, **kwargs)


class DataHandler:

    __feature_blueprints: list[FeatureBlueprint] = []

    def __init__(
        self, raw_data_fields: dict[str, type], dataframe: pd.DataFrame
    ):
        self.__raw_data_fields: dict[str, type] = raw_data_fields
        self.__feature_counter: Counter = Counter(raw_data_fields.keys())
        self.__dataframe: pd.DataFrame = dataframe

    @property
    def dataframe(self) -> pd.DataFrame:
        return self.__dataframe if self.__dataframe is not None else None

    def get_dataframe(self) -> pd.DataFrame:
        return self.__dataframe if self.__dataframe is not None else None

    def add_feature_blueprint(self, feature_blueprint: FeatureBlueprint):
        self.__feature_blueprints.append(feature_blueprint)

    def add_dict_data(self, dict_data: dict):
        if self.__feature_counter != Counter(dict_data.keys()):
            raise ValueError('Unexpected format of input data.')

        for field, data_type in self.__raw_data_fields.items():
            if type(dict_data.get(field)) != data_type:
                raise TypeError(
                    f'"{field}" is not of the expected type "{data_type}".'
                )

        new_dataframe = pd.DataFrame.from_records([dict_data])
        self.__dataframe = pd.concat(
            [self.__dataframe, new_dataframe], ignore_index=True
        )

    def add_json_data(self, json_data: json):
        loaded_json_data: dict = json.loads(json_data)
        self.add_dict_data(loaded_json_data)

    def apply_features(self, **kwargs):
        for feature_blueprint in self.__feature_blueprints:
            feature_blueprint(self.__dataframe, **kwargs)


class TimeSeriesDataHandler(DataHandler):

    def __init__(
        self, raw_data_fields: dict[str, type], dataframe: pd.DataFrame,
        datetime_column: str, datetime_format='%Y-%m-%d %H:%M:%S'
    ):
        super().__init__(raw_data_fields, dataframe)
        self.__datetime_column = datetime_column
        self.__datetime_format = datetime_format
        self.get_dataframe()[self.__datetime_column] = pd.to_datetime(
            self.get_dataframe()[self.__datetime_column]
        )
        self.__latest_datetime = (
            self.get_dataframe().iloc[-1][self.__datetime_column]
        )

    def add_time_series_data(self, json_data: json):
        loaded_json_data: dict = json.loads(json_data)
        try:
            loaded_json_data[self.__datetime_column] = dt.datetime.strptime(
                loaded_json_data.get(self.__datetime_column),
                self.__datetime_format
            )
        except Exception as e:
            raise e

        self.add_dict_data(loaded_json_data)

    # make sure data can't be added multiple times for the same datetime
    def _datetime_check(self):
        pass

    # make sure the data is following a consistent frequency
    def _frequency_check(self):
        pass


if __name__ == '__main__':
    pass
