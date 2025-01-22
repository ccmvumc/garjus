"""dash index page."""
import logging

from dash import html
import dash_bootstrap_components as dbc

from .app import app
from .pages import hub
from .pages import qa
from .pages import activity
from .pages import issues
from .pages import queue
from .pages import stats
from .pages import analyses
from .pages import processors
from .pages import reports
from ..garjus import Garjus


logger = logging.getLogger('garjus.dashboard.content')


def _redcap_found():
    return Garjus.redcap_found()


def _xnat_found():
    return Garjus.xnat_found()


def _footer_content(include_logout=False):
    content = []

    content.append(html.Hr())

    if include_logout:
        content.append(
            html.Div([
                dbc.Row([
                    dbc.Col(
                        html.A(
                            "garjus",
                            href='https://github.com/ccmvumc/garjus',
                            target="_blank",
                        ),
                    ),
                    dbc.Col(
                        html.A('xnat', href='https://xnat.vanderbilt.edu/xnat'),
                    ),
                    dbc.Col(
                        html.A('logout', href='../logout'),
                    ),
                ]),
                ],
                style={'textAlign': 'center'},
            )
        )
    else:
        content.append(
            html.Div([
                html.A(
                    "garjus",
                    href='https://github.com/ccmvumc/garjus',
                    target="_blank",
                )
                ],
                style={'textAlign': 'center'}
            )
        )

    return content


def get_content(include_logout=False, demo=True):

    if demo:
        return demo_content()

    #has_xnat = _xnat_found()
    has_xnat = True
    has_redcap = _redcap_found()
    tabs = ''
    content = ''

    logger.debug(f'{has_xnat=}, {has_redcap=}')

    if has_xnat and has_redcap:
        # include all tabs
        tabs = dbc.Tabs([
            #dbc.Tab(
            #    label='Home',
            #    tab_id='tab-home',
            #    children=hub.get_content(),
            #),
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
                label='Processors',
                tab_id='tab-processors',
                children=processors.get_content(),
            ),
            dbc.Tab(
                label='Reports',
                tab_id='tab-reports',
                children=reports.get_content(),
            ),
            dbc.Tab(
                label='Analyses',
                tab_id='tab-analyses',
               children=analyses.get_content(),
            )
            ],
            active_tab="tab-qa",
        )
    elif has_xnat and not has_redcap:
        tabs = html.Div(qa.get_content())
    elif has_redcap and not has_xnat:
        tabs = dbc.Tabs([
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
                label='Processors',
                tab_id='tab-processors',
                children=processors.get_content(),
            ),
            dbc.Tab(
                label='Reports',
                tab_id='tab-reports',
                children=reports.get_content(),
            ),
            dbc.Tab(
                label='Analyses',
                tab_id='tab-analyses',
                children=analyses.get_content(),
            ),
        ])

    footer_content = _footer_content(include_logout)

    content = html.Div(
        className='dbc',
        style={'marginLeft': '20px', 'marginRight': '20px'},
        children=[
            html.Div(id='report-content', children=[tabs]),
            html.Div(id='footer-content', children=footer_content)
    ])

    return content


def demo_content():
    tabs = dbc.Tabs([
        dbc.Tab(
            label='QA',
            tab_id='tab-qa',
            children=qa.get_content(),
        ),
        #dbc.Tab(
        #    label='Issues',
        #    tab_id='tab-issues',
        #    children=issues.get_content(),
        # ),
        #dbc.Tab(
        #    label='Queue',
        #    tab_id='tab-queue',
        #    children=queue.get_content(),
        # ),
        #dbc.Tab(
        #    label='Activity',
        #    tab_id='tab-activity',
        #    children=activity.get_content(),
        #),
        #dbc.Tab(
        #    label='Stats',
        #    tab_id='tab-stats',
        #    children=stats.get_content(),
        #),
        #dbc.Tab(
        #    label='Processors',
        #    tab_id='tab-processors',
        #    children=processors.get_content(),
        #),
        #dbc.Tab(
        #    label='Reports',
        #    tab_id='tab-reports',
        #    children=reports.get_content(),
        #),
        #dbc.Tab(
        #    label='Analyses',
        #    tab_id='tab-analyses',
        #   children=analyses.get_content(),
        #)
    ], active_tab="tab-qa")

    footer_content = _footer_content()

    content = html.Div(
        className='dbc',
        style={'marginLeft': '20px', 'marginRight': '20px'},
        children=[
            html.Div(id='report-content', children=[tabs]),
            html.Div(id='footer-content', children=footer_content)
    ])

    return content
