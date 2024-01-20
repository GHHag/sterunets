import datetime as dt
import json

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

from sterunets.model_handler import ModelHandler
from sterunets.data_handler import DataHandler, FeatureBlueprint
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


def calculate_mean(df):
    df['mean_value'] = df['value'].mean()
    # return df


def calculate_rolling_mean(df, periods):
    df['rolling_mean_value'] = df['value'].rolling(periods).mean()
    # return df


if __name__ == '__main__':
    raw_data_fields = {'dummy_column': str, 'value': float}
    initial_data = {
        'dummy_column': ['a', 'b', 'c', 'd'],
        'value': [10, 15, 15, 20]
    }
    initial_df = pd.DataFrame(initial_data)
    data_handler = DataHandler(raw_data_fields, initial_df)

    feature_blueprint = FeatureBlueprint('mean_value', calculate_mean, (), 2)
    feature_blueprint1 = FeatureBlueprint(
        'rolling_mean_value', calculate_rolling_mean, (2,), 2
    )
    data_handler.add_feature_blueprint(feature_blueprint)
    # data_handler.add_feature_blueprint(feature_blueprint1)

    data_handler.apply_features()

    new_data = {'dummy_column': 'e', 'value': 22.5}
    json_data = json.dumps(new_data)
    data_handler.add_json_data(json_data)

    data_handler.apply_features()

    print(data_handler.dataframe)
