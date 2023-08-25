from dash import dcc, html
import dash_bootstrap_components as dbc

from .app import app
from . import qa
from . import activity
from . import issues
from . import queue
from . import stats
from . import analyses


def get_layout():
    qa_content = qa.get_content()
    activity_content = activity.get_content()
    stats_content = stats.get_content()
    issues_content = issues.get_content()
    queue_content = queue.get_content()
    analyses_content = analyses.get_content()

    tabs = []

    tabs.append(dcc.Tab(label='QA', value='qa', children=qa_content))

    if activity_content:
        tabs.append(dcc.Tab(
            label='Activity', value='activity', children=activity_content))

    if issues_content:
        tabs.append(dcc.Tab(
            label='Issues', value='issues', children=issues_content))

    if queue_content:
        tabs.append(dcc.Tab(
            label='Queue', value='queue', children=queue_content))

    if stats_content:
        tabs.append(dcc.Tab(
            label='Stats', value='stats', children=stats_content))

    if analyses_content:
        tabs.append(dcc.Tab(
            label='Analyses', value='analyses', children=analyses_content))

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
        html.Div(children=footer_content, id='footer-content')])

    return main_content


# For gunicorn to work correctly
server = app.server

app.css.config.serve_locally = False

# Set the title to appear on web pages
app.title = 'DAX Dashboard'

# Set the content and templates
# more here:
# https://hellodash.pythonanywhere.com
app.css.append_css({
    #'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css',
    #'external_url': dbc.themes.BOOTSTRAP,
    #'external_url': dbc.themes.LUMEN,
    #'external_url': dbc.themes.YETI,
    'external_url': dbc.themes.FLATLY,
})

app.layout = get_layout()

if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
