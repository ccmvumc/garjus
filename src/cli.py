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
@click.argument('src', required=True)
@click.argument('dst', required=True)
def copy_session(src, dst):
    click.echo('garjus! copy session')
    Garjus().copy_sess(src, dst)


@cli.command('issues')
@click.option('--project', '-p', 'project')
@click.pass_context
def issues(ctx, project):
    click.echo('garjus! issues')
    g = Garjus()
    pprint.pprint(g.issues(project))


@cli.command('build')
@click.option('--project', '-p', 'project')
def build(project):
    click.echo('garjus! build')
    Garjus().build(project)


@cli.command('subjects')
@click.option('--project', '-p', 'project')
@click.pass_context
def subjects(ctx, project):
    click.echo('garjus! subjects')
    g = Garjus()
    pprint.pprint(g.subjects(project))


@cli.command('activity')
@click.option('--project', '-p', 'project')
def activity(project):
    click.echo('garjus! activity')
    g = Garjus()
    pprint.pprint(g.activity(project))


@cli.command('tasks')
def tasks():
    click.echo('garjus! tasks')
    g = Garjus()
    pprint.pprint(g.tasks())


@cli.command('q2d')
def q2d():
    click.echo('garjus! q2d')
    Garjus().queue2dax()


@cli.command('update')
@click.argument(
    'choice', 
    type=click.Choice(['stats' ,'issues', 'progress', 'automations', 'compare', 'tasks']),
    required=False,
    nargs=-1)
@click.option('--project', '-p', 'project', multiple=True)
def update(choice, project):
    click.echo('garjus! update')
    g = Garjus()
    g.update(projects=project, choices=choice)
    click.echo('ALL DONE!')


@cli.command('progress')
@click.option('--project', '-p', 'project')
def progress(project):
    click.echo('garjus! progress')
    if project:
        project = project.split(',')

    g = Garjus()
    print(g.progress(projects=project))


@cli.command('processing')
@click.option('--project', '-p', 'project', required=True)
def processing(project):
    click.echo('garjus! processing')

    g = Garjus()
    pprint.pprint(g.processing_protocols(project))


@cli.command('report')
@click.option('--project', '-p', 'project', required=True)
def report(project):
    click.echo('garjus! report')
    Garjus().report(project)


@cli.command('compare')
@click.option('--project', '-p', 'project', required=True)
def compare(project):
    click.echo('garjus! compare')
    Garjus().compare(project)


@cli.command('importdicom')
@click.argument('src', required=True)
@click.argument('dst', required=True)
def import_dicom(src, dst):
    click.echo('garjus! import')
    g = Garjus()
    g.import_dicom(src, dst)


@cli.command('dashboard')
def dashboard():
    import sys
    import os
    import webbrowser
    url = 'http://localhost:8050'

    # TODO: check that we have credentials for redcap and/or xnat

    # start up a dashboard app
    from .dashboard import app

    # Open URL in a new tab, if a browser window is already open.
    webbrowser.open_new_tab(url)

    app.run_server(host='0.0.0.0')

    print('dashboard app closed!')


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
# activity
# stats

# Garjus is the interface to everything that's stored in XNAT/REDCap and it uses
# REDCap to store it's own settings and tracking data. Anytime we want to
# access these systems in python or CLI, try to use Garjus in between, creating 
# a Garjus instance means just setting up the interfaces with minimal interaction 
# with XNAT/REDCap, i.e. cheap. 
