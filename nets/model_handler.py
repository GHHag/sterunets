import numpy as np
import pandas as pd


class ModelHandler:

    def __init__(self, data_blueprint, model):
        self.data_blueprint = data_blueprint
        self.model = model
        self.models = {}

    def _validate_input(self):
        pass

    def make_prediction(self, input_data):
        if self._validate_input(input_data):
            return self.model.predict(input_data)
        else:
            return False


# Should be a function in the nets package.
# 'data' is a generic data structure containing the data in a format suitable
# to the model in question.
def create_dataframe(data):
    pass


# input = data in the format the model in question requires
# output = prediction made by the model
def make_prediction(data):
    pass


# Should the model be stored in memory or be persisted
# (using for example pickle)?
def create_model(input_params):
    pass


if __name__ == '__main__':
    pass
