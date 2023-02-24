import click
import pprint
import logging

from .garjus import Garjus

logging.basicConfig(
    format='%(asctime)s - %(levelname)s:%(name)s:%(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


@click.group()
@click.option('--debug/--no-debug', default=False)
def cli(debug):
    if debug:
        click.echo('garjus! debug')
        logging.getLogger().setLevel(logging.DEBUG)


@cli.command('copysess')
def copy_session():
    click.echo('garjus! copy session')
    pprint.pprint('TBD')


@cli.command('issues')
@click.pass_context
def issues(ctx):
    click.echo('garjus! issues')
    g = Garjus()
    pprint.pprint(g.issues())


@cli.command('update')
@click.option('--project', '-p', 'project')
def update(project):
    click.echo('garjus! update')
    if project:
        project = project.split(',')

    g = Garjus()
    result = g.update(projects=project)
    click.echo(result)


@cli.command('progress')
@click.option('--project', '-p', 'project')
def progress(project):
    click.echo('garjus! progress')
    if project:
        project = project.split(',')

    g = Garjus()
    result = g.progress(projects=project)
    click.echo(result)


@cli.command('dashboard')
def dashboard():
    import sys
    import os
    import webbrowser
    url = 'http://localhost:8050'

    # TODO: check that we have credentials for redcap and xnat

    # start up a dashboard app
    try:
        # TODO: install dashboard as a package and load instead of this hack
        # to find it or or or
        # move the dashboard into garjus as a subdir and then import from
        # there and start rewriting it to use main garjus to get data
        sys.path.append(os.path.expanduser(
            '~/git/dax-dashboard/dashboard'))
        from index import app
    except ModuleNotFoundError as err:
        print(f'error loading function:{err}')
        return

    # Open URL in a new tab, if a browser window is already open.
    webbrowser.open_new_tab(url)

    app.run_server(host='0.0.0.0')

    print('app returned')


def quick_test():
    click.echo('garjus!')
    g = Garjus()
    scans = g.scans(projects=['CHAMP'])
    print(scans)

# subcommmands:
# build (jobs)
# update (double entry, issues, automations, stats, progress reports, image03, etc)
# info ()
# download (images, stats)
# upload (images)
# issues
# activity
# stats


# garjus update CHAMP

# Garjus is the interface to everything that's stored in XNAT/REDCap and it uses
# REDCap to store it's own settings and tracking data. Anytime we want to
# access these systems in python or CLI, try to use Garjus in between, creating 
# a Garjus instance means just setting up the interfaces with minimal interaction 
# with XNAT/REDCap, i.e. cheap. 
