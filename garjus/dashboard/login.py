import os
import datetime
import logging

from flask import Flask, request, redirect, session, jsonify, url_for, render_template
from flask_login import login_user, LoginManager, UserMixin, logout_user, current_user
import dash
from dash import html
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template

from .pages import qa
from .pages import activity
from .pages import issues
from .pages import queue
from .pages import stats
from .pages import analyses
from .pages import processors
from .pages import reports


# This file serves the same purpose as index.py but wrapped in a flask app
# with user/password authentication. garjus will return this app when
# login option is requested.

logger = logging.getLogger('garjus.dashboard.login')

# Connect to an underlying flask server so we can configure it for auth
templates = os.path.expanduser('~/git/garjus/garjus/dashboard/templates')
server = Flask(__name__, template_folder=templates)


@server.before_request
def check_login():
    # TODO: use dash pages module to return pages based on user access level
    if request.method == 'GET':
        if request.path in ['/login', '/logout']:
            return
        if current_user:
            if current_user.is_authenticated:
                logger.debug(f'user is authenticated:{current_user.id}')
                return
        return redirect(url_for('login'))
    else:
        if current_user:
            if request.path == '/login' or current_user.is_authenticated:
                return
        return jsonify({'status': '401', 'statusText': 'unauthorized access'})


@server.route('/login', methods=['POST', 'GET'])
def login(message=""):
    if request.method == 'POST':
        if request.form:
            hostname = 'https://xnat.vanderbilt.edu/xnat'
            username = request.form['username']
            password = request.form['password']

            # Get the xnat alias token
            from ..garjus import Garjus
            Garjus.login(hostname, username, password)

            # TODO: check that valid token was acquired, otherwise cannot login
            is_valid = True

            if is_valid:
                login_user(User(username, hostname))

                # What page do we send?
                if session.get('url', False):
                    # redirect to original target
                    url = session['url']
                    logger.debug(f'redirecting to target url:{url}')
                    session['url'] = None
                    return redirect(url)
                else:
                    # redirect to home
                    return redirect('/')
            else:
                # Invalid so set the return message to display
                message = 'invalid username and/or password'
    else:
        if current_user:
            if current_user.is_authenticated:
                return redirect('/')

    return render_template('login.html', message=message)


@server.route('/logout', methods=['GET'])
def logout():
    if current_user:
        if current_user.is_authenticated:
            logout_user()
    return render_template('login.html', message="you have been logged out")

# Prep the configs for the app
dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
assets_path = os.path.expanduser('~/git/garjus/garjus/dashboard/assets')
darkmode = True

#hour = datetime.datetime.now().hour
#if hour < 9 or hour > 17:
if darkmode:
    stylesheets = [dbc.themes.DARKLY, dbc_css]
    load_figure_template("darkly")
else:
    stylesheets = [dbc.themes.FLATLY, dbc_css]
    load_figure_template("flatly")

# Build the dash app with the configs
app = dash.Dash(
    __name__,
    server=server,
    external_stylesheets=stylesheets,
    assets_folder=assets_path,
    suppress_callback_exceptions=True,
)

# Set the title to appear on web pages
app.title = 'dashboard'

server.config.update(SECRET_KEY=os.urandom(24))

# Login manager object will be used to login / logout users
login_manager = LoginManager()
login_manager.init_app(server)
login_manager.login_view = "/login"


class User(UserMixin):
    # User data model. It has to have at least self.id as a minimum
    def __init__(self, username, hostname=None):
        self.id = username
        self.hostname = hostname


@login_manager.user_loader
def load_user(username):
    """This function loads the user by user id."""
    return User(username)


def redcap_found():
    from ..garjus import Garjus
    return Garjus.redcap_found


footer_content = [
    html.Hr(),
    html.Div(
        [
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
    ),
]

if redcap_found():
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
else:
    tabs = html.Div(qa.get_content())

app.layout = html.Div(
    className='dbc',
    style={'marginLeft': '20px', 'marginRight': '20px'},
    children=[
        html.Div(id='report-content', children=[tabs]),
        html.Div(id='footer-content', children=footer_content)
    ])

if __name__ == "__main__":
    app.run_server(debug=True)
