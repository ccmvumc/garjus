"""dash index page."""
import logging

from .app import app
from . import content


logger = logging.getLogger('garjus.dashboard')


app.layout = content.get_content()

# For gunicorn to work correctly
server = app.server

# Allow external css
app.css.config.serve_locally = False

# Set the title to appear on web pages
app.title = 'dashboard'

if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
