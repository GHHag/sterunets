import pandas as pd
import numpy as np


class DataHandler:

    # feature_blueprint as a class implementing __call__?
    def __init__(self, feature_blueprint: callable, dataframe: pd.DataFrame):
        self.feature_blueprint = feature_blueprint
        self.dataframe = dataframe

    def calculate_features(self):
        self.feature_blueprint(self.dataframe)
