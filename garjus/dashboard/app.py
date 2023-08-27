import dash
import dash_bootstrap_components as dbc

dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY, dbc_css])

server = app.server

app.config.suppress_callback_exceptions = True

# more here:
# https://hellodash.pythonanywhere.com
# https://codepen.io/chriddyp/pen/bWLwgP.css
# dbc.themes.DARKLY flatdark
# dbc.themes.FLATLY flat
# dbc.themes.LUMEN bright
# dbc.themes.SLATE dark
# dbc.themes.SPACELAB buttony
# dbc.themes.YETI nice
# "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css",
