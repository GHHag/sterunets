import datetime as dt
import json
from collections import Counter

import pandas as pd
import numpy as np


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
    __number_of_periods: int = 0
    __expected_row_drops: int = 0

    def __init__(
        self, raw_data_fields: dict[str, type], dataframe: pd.DataFrame
    ):
        self.__raw_data_fields: dict[str, type] = raw_data_fields
        self.__feature_counter: Counter = Counter(raw_data_fields.keys())
        self.__dataframe: pd.DataFrame = dataframe
        self.__number_of_periods: int = len(self.__dataframe)

    @property
    def raw_data_fields(self) -> dict[str, type]:
        return self.__raw_data_fields

    @property
    def feature_counter(self) -> Counter:
        return self.__feature_counter

    @property
    def dataframe(self) -> pd.DataFrame:
        return self.__dataframe if self.__dataframe is not None else None

    def add_feature_blueprint(self, feature_blueprint: FeatureBlueprint):
        self.__feature_blueprints.append(feature_blueprint)

    def concat_dataframes(self, new_dataframe: pd.DataFrame):
        self.__dataframe = pd.concat([self.__dataframe, new_dataframe])
        self.__number_of_periods += len(new_dataframe)

        if not self._intactness_check():
            raise ValueError(
                f'Inconsistent number of rows in the DataFrame "__dataframe". '
                'Expected '
                f'{self.__number_of_periods - self.__expected_row_drops} '
                f'rows, but found {len(self.__dataframe)} rows.'
            )

    # define similar function for lists of data?
    # how to type hint if 'data' can be either json or dict?
    def add_data(self, data, data_format='json'):
        if data_format == 'json':
            data: dict = json.loads(data)

        if self.__feature_counter != Counter(data.keys()):
            raise ValueError('Unexpected format of input data.')

        for field, data_type in self.__raw_data_fields.items():
            if type(data.get(field)) != data_type:
                raise TypeError(
                    f'"{field}" is not of the expected type "{data_type}".'
                )

        new_dataframe = pd.DataFrame.from_records([data])
        self.concat_dataframes(new_dataframe)

    def apply_features(self, **kwargs):
        for feature_blueprint in self.__feature_blueprints:
            feature_blueprint(self.__dataframe, **kwargs)

    def _intactness_check(self) -> bool:
        return (
            self.__number_of_periods - self.__expected_row_drops
            == len(self.dataframe)
        )

    def some_func_handling_drop_nan(self):
        # fill na?
        # drop na
        # add to self.__expected_row_drops
        # call _intactness_check, raise error if returning False
        pass


class TimeSeriesDataHandler(DataHandler):

    def __init__(
        self, raw_data_fields: dict[str, type], dataframe: pd.DataFrame,
        datetime_column: str, datetime_format='%Y-%m-%d %H:%M:%S'
    ):
        super().__init__(raw_data_fields, dataframe)
        self.__datetime_column: str = datetime_column
        self.__datetime_format: str = datetime_format
        self.dataframe[self.__datetime_column] = pd.to_datetime(
            self.dataframe[self.__datetime_column]
        )
        if not pd.api.types.is_datetime64_any_dtype(
            self.dataframe[self.__datetime_column]
        ):
            self.dataframe.set_index(self.__datetime_column, inplace=True)
        self.dataframe.set_index(self.__datetime_column, inplace=True)
        self.__latest_datetime: pd.Timestamp = (
            self.dataframe.last_valid_index()
        )

    @property
    def latest_datetime(self) -> pd.Timestamp:
        return self.__latest_datetime

    # define a similar method that works on list of multiple json data points
    # or add that functionality to this method
    def add_data(self, data, data_format='json'):
        if data_format == 'json':
            data: dict = json.loads(data)
            try:
                data[self.__datetime_column] = dt.datetime.strptime(
                    data.get(self.__datetime_column),
                    self.__datetime_format
                )
            except ValueError as e:
                raise e

        if self.feature_counter != Counter(data.keys()):
            raise ValueError('Unexpected format of input data.')

        for field, data_type in self.raw_data_fields.items():
            if type(data.get(field)) != data_type:
                raise TypeError(
                    f'"{field}" is not of the expected type "{data_type}".'
                )

        new_dataframe = pd.DataFrame.from_records([data])
        new_dataframe[self.__datetime_column] = pd.to_datetime(
            new_dataframe[self.__datetime_column]
        )
        new_dataframe.set_index(self.__datetime_column, inplace=True)
        self.concat_dataframes(new_dataframe)

    def add_time_series_data_list(self, json_data: json):
        pass

    # make sure data can't be added multiple times for the same datetime
    def _datetime_check(self):
        pass

    # make sure the data is following a consistent frequency
    def _frequency_check(self):
        pass


if __name__ == '__main__':
    pass


# df.shape - tuple with data shape

# how to check if columns are removed from dropna() or such? and 
# how to take action?

# maybe you just want to store the n latest data points of a dataframe
# to stop it from growing unnecessarily large
