from dash import dcc, html
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template

from .app import app
from . import qa
from . import activity
from . import issues
from . import queue
from . import stats
from . import analyses



def get_layout_darkmode():
    qa_content = qa.get_content(darkmode=True)
    #activity_content = activity.get_content()
    #stats_content = stats.get_content()
    #issues_content = issues.get_content()
    #queue_content = queue.get_content()
    #analyses_content = analyses.get_content()

    report_content = [
        html.Div(
            dbc.Tabs(id='tabs', children=[
                dbc.Tab(
                    label='QA', tab_id='1', children=qa_content),
                #dbc.Tab(
                #    label='Activity', tab_id='2', children=activity_content),
                #dbc.Tab(
                #    label='Issues', tab_id='3', children=issues_content),
                #dbc.Tab(
                #    label='Queue', tab_id='4', children=queue_content),
                #dbc.Tab(
                #    label='Stats', tab_id='5', children=stats_content),
                #dbc.Tab(
                #    label='Analyses', tab_id='6', children=analyses_content),
            ]),
            style={
                'paddingLeft': '70px',
                'align-items': 'left',
                'justify-content': 'left'},
        )
    ]

    footer_content = [
        html.Hr(),
        html.H5('F: Failed'),
        html.H5('P: Passed QA'),
        html.H5('Q: To be determined')]

    # Make the main app layout
    main_content = html.Div([
        #html.Div([html.H1('DAX Dashboard')]),
        html.Div(children=report_content, id='report-content'),
        html.Div(children=footer_content, id='footer-content')])

    main_content = dbc.Container([main_content], fluid=True, className="dbc")

    return main_content


def get_layout(darkmode=False):
    if darkmode:
        return get_layout_darkmode()

    qa_content = qa.get_content(darkmode)
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
darkmode = False  
if darkmode:
    # not working well yet 8/12/2023 bdb

    # Could this to make a switcher, also could use theme color css
    # https://github.com/AnnMarieW/dash-bootstrap-templates

    load_figure_template('DARKLY')
    dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates@V1.0.2/dbc.css"
    app.css.append_css({
        #'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css',
        'external_url': dbc_css,
        'external_url': dbc.themes.DARKLY,
    })
else:
    app.css.append_css({
        'external_url': dbc.themes.BOOTSTRAP,
        'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css',
    })

app.layout = get_layout(darkmode=darkmode)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
