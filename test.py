import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import train_test_split

from nets.model_handler import ModelHandler
from nets.data_handler import DataHandler
from nets.grpc_service import *
from nets.grpc_service import run_grpc_server

# 1.
# Import data + create dataframe
# Create/calculate feature
# Create model

# 2.
# Statefully store model + dataframe
# Add new data
# Calculate features
# Make predictions


def feature_x(df):
    pass


def feature_y(df):
    pass


def apply_features(df):
    pass


def persist_model(model):
    pass


def main():
    df = pd.read_csv('some_csv_file.csv')

    apply_features(df)

    # Pass df to train_test_split or convert to numpy data structure first?
    X_train, y_train, X_test, y_test = train_test_split(df)

    model = LinearRegression()


if __name__ == '__main__':
    main()
