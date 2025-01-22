import os
import logging

import dash
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template

from . import content

dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"

logger = logging.getLogger('garjus.dashboard.demo')

# TODO: fix this to find path more dynamically
assets_path = os.path.expanduser('~/git/garjus/garjus/dashboard/assets')

stylesheets = [dbc.themes.DARKLY, dbc_css]

load_figure_template("darkly")

# Build the dash app with the configs
app = dash.Dash(
    __name__,
    external_stylesheets=stylesheets,
    assets_folder=assets_path,
    suppress_callback_exceptions=True,
)

# Set the title to appear on web pages
app.title = 'dashboard'

#server = app.server
#server.config.update(SECRET_KEY=os.urandom(24))

# Set the main content
app.layout = content.get_content(demo=True)

logger.debug(f'{app.layout}')


#app.config.suppress_callback_exceptions = True


if __name__ == "__main__":
    app.run_server(debug=True)
