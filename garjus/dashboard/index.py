from dash import dcc, html

from .app import app
from . import qa
from . import activity
from . import issues
from . import queue
from . import stats
from . import analyses

from ..garjus import Garjus


def get_layout():
    tabs = []
    g = Garjus()

    tabs.append(dcc.Tab(label='QA', value='qa', children=qa.get_content()))

    if g.redcap_enabled():
        tabs.append(dcc.Tab(
            label='Activity',
            value='activity',
            children=activity.get_content()))

        tabs.append(dcc.Tab(
            label='Issues',
            value='issues',
            children=issues.get_content()))

        tabs.append(dcc.Tab(
            label='Queue',
            value='queue',
            children=queue.get_content()))

        tabs.append(dcc.Tab(
            label='Stats',
            value='stats',
            children= stats.get_content()))

        tabs.append(dcc.Tab(
            label='Analyses',
            value='analyses',
            children=analyses.get_content()))

    report_content = [
        html.Div(dcc.Tabs(
            id='tabs', value='qa', vertical=False, children=tabs))]

    footer_content = [
        html.Hr(),
        html.Div(dcc.Link(
                [html.P('garjus')],
                href='https://github.com/ccmvumc/garjus'),
            style={'textAlign': 'center'}),
    ]

    # Make the main app layout
    main_content = html.Div([
        html.Div(children=report_content, id='report-content'),
        html.Div(children=footer_content, id='footer-content')],
    )

    return main_content


# For gunicorn to work correctly
server = app.server

app.css.config.serve_locally = False

# Set the title to appear on web pages
app.title = 'DAX Dashboard'

app.layout = get_layout()

if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
