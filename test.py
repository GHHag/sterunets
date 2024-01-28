import datetime as dt
import json

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

from sterunets.model_handler import ModelHandler
from sterunets.data_handler import DataHandler, TimeSeriesDataHandler, FeatureBlueprint
from sterunets.grpc_service import *
from sterunets.grpc_service import run_grpc_server

# 1.
# Import data + create dataframe
# Create/calculate feature
# Create model

# 2.
# Statefully store model + dataframe
# Add new data
# Calculate features
# Make predictions


def persist_model(model):
    pass


# def calculate_mean(df: pd.DataFrame, required_periods: int, **_):
def calculate_mean(df: pd.DataFrame, **_):
    df['mean_value'] = df['value'].mean()


def calculate_rolling_mean(
    df: pd.DataFrame, periods: int, 
    full_df_apply: bool=False, **_
):
    if full_df_apply:
        df['rolling_mean_value'] = df['value'].rolling(periods).mean()
    else:
        df.at[len(df)-1, 'rolling_mean_value'] = df['value'].iloc[-periods:].mean()


if __name__ == '__main__':
    raw_data_fields = {'dummy_column': str, 'value': float}
    initial_data = {
        'dummy_column': ['a', 'b', 'c', 'd'],
        'value': [10, 15, 15, 20]
    }
    initial_df = pd.DataFrame(initial_data)
    data_handler = DataHandler(raw_data_fields, initial_df)

    feature_blueprint = FeatureBlueprint('mean_value', calculate_mean, ())
    feature_blueprint1 = FeatureBlueprint(
        'rolling_mean_value', calculate_rolling_mean, (2,),
    )
    data_handler.add_feature_blueprint(feature_blueprint)
    data_handler.add_feature_blueprint(feature_blueprint1)

    data_handler.apply_features(full_df_apply=True)

    new_data = {'dummy_column': 'e', 'value': 22.5}
    json_data = json.dumps(new_data)
    data_handler.add_json_data(json_data)
    data_handler.apply_features()
    # print(data_handler.dataframe)

    new_data = {'dummy_column': 'f', 'value': 25.0}
    json_data = json.dumps(new_data)
    data_handler.add_json_data(json_data)
    data_handler.apply_features()
    # print(data_handler.dataframe)

    new_data = {'dummy_column': 'g', 'value': 100.0}
    json_data = json.dumps(new_data)
    data_handler.add_json_data(json_data)
    data_handler.apply_features()
    # print(data_handler.dataframe)

    ts_raw_data_fields = {
        'date': dt.datetime,
        'dummy_column': str,
        'value': float
    }
    ts_initial_data = {
        'date': [
            str(dt.datetime(2024, 1, 21)),
            str(dt.datetime(2024, 1, 22)),
            str(dt.datetime(2024, 1, 23)),
            str(dt.datetime(2024, 1, 24))
        ],
        'dummy_column': ['a', 'b', 'c', 'd'],
        'value': [10, 15, 15, 20]
    }
    ts_initial_df = pd.DataFrame(ts_initial_data)
    ts_data_handler = TimeSeriesDataHandler(ts_raw_data_fields, ts_initial_df, 'date')

    ts_data_handler.add_feature_blueprint(feature_blueprint)
    ts_data_handler.add_feature_blueprint(feature_blueprint1)

    ts_data_handler.apply_features(full_df_apply=True)

    new_data = {
        'date': str(dt.datetime(2024, 1, 25)),
        'dummy_column': 'e',
        'value': 22.5
    }
    json_data = json.dumps(new_data)
    ts_data_handler.add_time_series_data(json_data)
    ts_data_handler.apply_features()
    print(ts_data_handler.dataframe)

    new_data = {
        'date': str(dt.datetime(2024, 1, 26)),
        'dummy_column': 'f',
        'value': 25.0
    }
    json_data = json.dumps(new_data)
    ts_data_handler.add_time_series_data(json_data)
    ts_data_handler.apply_features()
    print(ts_data_handler.dataframe)

    new_data = {
        'date': str(dt.datetime(2024, 1, 27)),
        'dummy_column': 'g', 
        'value': 100.0
    }
    json_data = json.dumps(new_data)
    ts_data_handler.add_time_series_data(json_data)
    ts_data_handler.apply_features()
    print(ts_data_handler.dataframe)

    new_data = {
        'date': str(dt.datetime(2024, 1, 27)),
        'dummy_column': 'g', 
        'value': 200.0
    }
    json_data = json.dumps(new_data)
    ts_data_handler.add_time_series_data(json_data)
    ts_data_handler.apply_features()
    print(ts_data_handler.dataframe)

    print(ts_data_handler.dataframe.dtypes)