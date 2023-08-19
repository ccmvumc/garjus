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

    report_content = [
        html.Div(
            dcc.Tabs(id='tabs', value='1', vertical=False, children=[
                dcc.Tab(
                    label='QA', value='1', children=qa_content),
                dcc.Tab(
                    label='Activity', value='2', children=activity_content),
                dcc.Tab(
                    label='Issues', value='3', children=issues_content),
                dcc.Tab(
                    label='Queue', value='4', children=queue_content),
                dcc.Tab(
                    label='Stats', value='5', children=stats_content),
                 dcc.Tab(
                    label='Analyses', value='6', children=analyses_content),
            ]),
            style={
                #'paddingLeft': '40px',
                #'align-items': 'left',
                #'justify-content': 'center',
            },
        )
    ]

    footer_content = [
        html.Hr(),
        html.Div(
            html.P('https://github.com/ccmvumc/garjus'),
            style={'textAlign': 'center'}),
    ]

    # Make the main app layout
    main_content = html.Div([
        #html.Div([html.H1('DAX Dashboard')]),
        html.Div(children=report_content, id='report-content'),
        html.Div(children=footer_content, id='footer-content')])

    return main_content


# For gunicorn to work correctly
server = app.server

app.css.config.serve_locally = False

# Set the title to appear on web pages
app.title = 'DAX Dashboard'

# Set the content and templates
app.css.append_css({
    'external_url': dbc.themes.BOOTSTRAP,
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css',
})

app.layout = get_layout()

if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
