import os
import logging

from flask import Flask, request, redirect, session, jsonify, url_for, render_template
from flask_login import login_user, LoginManager, UserMixin, logout_user, current_user
import dash
from dash import html
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template

from ..garjus import Garjus
from .pages import qa
from .pages import activity
from .pages import issues
from .pages import queue
from .pages import stats
from .pages import analyses
from .pages import processors
from .pages import reports
from . import content

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
    logger.debug('checking login')
    if request.method == 'GET':
        if request.path in ['/login', '/logout']:
            # nothing to check here
            return
        if is_authenticated():
            logger.debug(f'user is authenticated:{current_user.id}')
            return

        # nothing to check so user must log in
        return redirect(url_for('login'))
    else:
        if current_user:
            if request.path == '/login' or is_authenticated():
                return

        logout_user()
        return


def is_authenticated():
    return current_user and current_user.is_authenticated and Garjus.is_authenticated()


@server.route('/login', methods=['POST', 'GET'])
def login(message=""):
    try:
        if request.method == 'POST':
            if request.form:
                hostname = 'https://xnat.vanderbilt.edu/xnat'
                username = request.form['username']
                password = request.form['password']

                try:
                    # Get the xnat alias token
                    from ..garjus import Garjus
                    logger.debug('Garjus login')
                    Garjus.login(hostname, username, password)

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
                        logger.debug('redirecting to home')
                        return redirect('/')
                except Exception as err:
                    logger.debug(f'login failed:{err}')
                    message = 'login failed, try again'
        else:
            if current_user:
                if current_user.is_authenticated:
                    try:
                        logger.debug('redirecting to /')
                        return redirect('/')
                    except Exception as err:
                        logger.debug(f'cannot log in, try again:{err}')
                        message = 'login failed, try again'
    except Exception as err:
        logger.error(f'login error, route to logout:{err}')
        return logout()

    logger.debug('rendering login.html')
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
    logger.debug(f'loading user:{username}')
    return User(username)

# Set the main content
app.layout = content.get_content(include_logout=True)

logger.debug(f'{app.layout}')

if __name__ == "__main__":
    app.run_server(debug=True)
