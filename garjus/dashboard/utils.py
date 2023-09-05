import pandas as pd
from dash import callback_context

def make_options(values):
    return [{'label': x, 'value': x} for x in values]


def make_columns(values):
    return [{'name': x, 'id': x} for x in values]


def read_data(filename):
    df = pd.read_pickle(filename)
    return df


def save_data(df, filename):
    df.to_pickle(filename)


def was_triggered(button_id):
    result = (
        callback_context.triggered and
        callback_context.triggered[0]['prop_id'].split('.')[0] == button_id)

    return result
