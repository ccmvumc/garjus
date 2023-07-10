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
@click.option('--quiet/--no-quiet', default=False)
def cli(debug, quiet):
    if debug:
        click.echo('garjus! debug')
        logging.getLogger().setLevel(logging.DEBUG)

    if quiet:
        logging.getLogger().setLevel(logging.ERROR)


@cli.command('copysess')
@click.argument('src', required=True)
@click.argument('dst', required=True)
def copy_session(src, dst):
    click.echo('garjus! copy session')
    Garjus().copy_sess(src, dst)


@cli.command('setsesstype')
@click.argument('src', required=True)
@click.argument('sesstype', required=True)
def set_sesstype(src, sesstype):
    click.echo('garjus! set session type')
    Garjus().set_session_type(src, sesstype)


@cli.command('setsite')
@click.argument('src', required=True)
@click.argument('site', required=True)
def set_site(src, site):
    click.echo('garjus! set session site')
    Garjus().set_session_site(src, site)


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


@cli.command('analyses')
@click.option('--project', '-p', 'project')
def analyses(project):
    click.echo('garjus! analyses')
    g = Garjus()
    pprint.pprint(g.analyses(project))


@cli.command('tasks')
def tasks():
    click.echo('garjus! tasks')
    g = Garjus()
    pprint.pprint(g.tasks())


@cli.command('q2d')
def q2d():
    click.echo('garjus! q2d')
    Garjus().queue2dax()


@cli.command('d2q')
def d2q():
    click.echo('garjus! d2q')
    Garjus().dax2queue()


@cli.command('update')
@click.argument(
    'choice',
    type=click.Choice([
        'stats',
        'issues',
        'progress',
        'automations',
        'compare',
        'tasks',
        'analyses'
    ]),
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
@click.option('--monthly/--no-monthly', default=True, required=False)
def report(project, monthly):
    click.echo('garjus! report')
    Garjus().report(project, monthly=monthly)


@cli.command('stats')
@click.option('--projects', '-p', 'projects', required=True)
@click.option('--types', '-t', 'proctypes', required=False)
@click.option('--sesstypes', '-s', 'sesstypes', required=False)
@click.option('--persubject', is_flag=True)
@click.argument('csv', required=True)
def stats(projects, proctypes, sesstypes, csv, persubject):
    click.echo('garjus! stats')
    Garjus().export_stats(projects, proctypes, sesstypes, csv, persubject)


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


@cli.command('pdf')
@click.argument('src', required=True)
@click.option('--project', '-p', 'project', required=True)
def export_pdf(src, project):
    click.echo('garjus! pdf')
    g = Garjus()
    g.pdf(src, project)


@cli.command('image03')
@click.option('--project', '-p', 'project', required=True)
@click.option(
    '--start', '-s', 'startdate', type=click.DateTime(formats=['%Y-%m-%d']))
@click.option(
    '--end', '-e', 'enddate', type=click.DateTime(formats=['%Y-%m-%d']))
def image03(project, startdate, enddate):
    click.echo('garjus! image03')
    g = Garjus()
    g.image03(project, startdate, enddate)


@cli.command('retry')
@click.option('--project', '-p', 'project', required=True)
def retry(project):
    click.echo('garjus! retry')
    g = Garjus()
    g.retry(project)


@cli.command('image03download')
@click.argument('image03_csv', required=True)
@click.argument('download_dir', required=True)
@click.option('--project', '-p', 'project', required=True)
def image03download(project, image03_csv, download_dir):
    click.echo('garjus! image03download')
    g = Garjus()
    g.image03download(project, image03_csv, download_dir)


@cli.command('delete')
@click.option('--project', '-p', 'project', required=True)
@click.option('--type', '-t', 'proctype', required=True)
def delete(project, proctype):
    click.echo('garjus! delete')
    g = Garjus()
    g.delete_proctype(project, proctype)


@cli.command('dashboard')
def dashboard():
    import webbrowser
    url = 'http://localhost:8050'

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
