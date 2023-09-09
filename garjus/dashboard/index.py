"""dash index page."""
import logging

from dash import dcc, html
import dash_bootstrap_components as dbc

from .app import app
from . import utils
from . import qa
from . import activity
from . import issues
from . import queue
from . import stats
from . import analyses
from . import processors


logger = logging.getLogger('garjus.dashboard')


footer_content = [
    html.Hr(),
    html.Div(
        [
            html.A(
                "garjus",
                href='https://github.com/ccmvumc/garjus',
                target="_blank",
            )
        ],
        style={'textAlign': 'center'},
    ),
]

tabs = dbc.Tabs([
    dbc.Tab(
        label='QA',
        tab_id='tab-qa',
        children=qa.get_content(),
    ),
    dbc.Tab(
        label='Issues',
        tab_id='tab-issues',
        children=issues.get_content(),
    ),
     dbc.Tab(
        label='Queue',
        tab_id='tab-queue',
        children=queue.get_content(),
    ),
    dbc.Tab(
        label='Activity',
        tab_id='tab-activity',
        children=activity.get_content(),
    ),
    dbc.Tab(
        label='Stats',
        tab_id='tab-stats',
        children=stats.get_content(),
    ),
    dbc.Tab(
        label='Analyses',
        tab_id='tab-analyses',
        children=analyses.get_content(),
    ),
    dbc.Tab(
        label='Processors',
        tab_id='tab-processors',
        children=processors.get_content(),
    )
])

app.layout = html.Div(
    className='dbc',
    style={'marginLeft': '20px', 'marginRight': '20px'},
    children=[
        html.Div(id='report-content', children=[tabs]),
        html.Div(id='footer-content', children=footer_content)
    ])

# For gunicorn to work correctly
server = app.server

#app.css.config.serve_locally = False

# Set the title to appear on web pages
app.title = 'dashboard'

if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
