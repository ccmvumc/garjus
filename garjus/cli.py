import click
import pprint
import logging
import os

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
        logging.getLogger('werkzeug').setLevel(logging.DEBUG)
        logging.getLogger('dash').setLevel(logging.DEBUG)

    if quiet:
        logging.getLogger().setLevel(logging.ERROR)
        logging.getLogger('werkzeug').setLevel(logging.ERROR)
        logging.getLogger('dash').setLevel(logging.ERROR)


@cli.command('copysess')
@click.argument('src', required=True)
@click.argument('dst', required=True)
def copy_session(src, dst):
    click.echo('garjus! copy session')
    Garjus().copy_sess(src, dst)


@cli.command('copyscan')
@click.argument('src', required=True)
@click.argument('dst', required=True)
def copy_scan(src, dst):
    click.echo('garjus! copy scan')
    Garjus().copy_scan(src, dst)


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


@cli.command('subjects')
@click.option('--project', '-p', 'project')
@click.option('--csv', '-c', 'csv', required=False)
@click.pass_context
def subjects(ctx, project, csv):
    click.echo('garjus! subjects')
    g = Garjus()
    import pandas as pd
    pd.set_option('display.max_rows', None)
    subjects = g.subjects(project)
    if csv:
        subjects = subjects.set_index('ID')
        columns = subjects.columns
        columns = [x for x in columns if x not in ['identifier_id', 'GUID', 'DOB']]
        subjects[columns].to_csv(csv)
    else:
        pprint.pprint(subjects)


@cli.command('orphans')
@click.option('--project', '-p', 'project')
@click.option('--delete/--no-delete', default=False)
def orphans(project, delete):
    click.echo('garjus! orphans')
    g = Garjus()

    orphans = g.orphans(project)

    print(*orphans, sep='\n')

    if delete:
        print('deleting')
        for cur in orphans:
            print('delete', cur)
            g.delete_assessor(project, cur)
    else:
        print('not deleting')


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


@cli.command('getinputs')
@click.argument('analysis_id', required=True)
@click.argument('download_dir', required=True)
@click.option('--project', '-p', 'project', required=True)
@click.option('--processor', '-y', 'processor', required=False)
def getinputs(project, analysis_id, download_dir, processor):
    click.echo('garjus! getinputs')
    g = Garjus()
    g.get_analysis_inputs(project, analysis_id, download_dir, processor)


@cli.command('getoutputs')
@click.argument('analysis_id', required=True)
@click.argument('download_dir', required=True)
@click.option('--project', '-p', 'project', required=True)
def getoutputs(project, analysis_id, download_dir):
    click.echo('garjus! getoutputs')
    g = Garjus()
    g.get_analysis_outputs(project, analysis_id, download_dir)


@cli.command('download')
@click.argument('download_dir', required=True)
@click.option('--type', '-t', 'proctype', multiple=False, required=True)
@click.option('--resource', '-r', 'resource', multiple=True, required=True)
@click.option('--file', '-f', 'file', multiple=True, required=False)
@click.option('--project', '-p', 'project', required=True)
@click.option('--sesstype', '-s', 'sesstype', multiple=True, required=False)
@click.option('--analysis', '-a', 'analysis', required=False)
@click.option('--session', '-e', 'session', multiple=True, required=False)
@click.option(
    "--scan", is_flag=True, default=False, help="scans instead of assessors.")
def download(
    project,
    proctype,
    download_dir,
    resource,
    file,
    scan,
    sesstype,
    analysis,
    session
):
    click.echo('garjus! download')
    g = Garjus()
    if scan:
        g.download_scantype(
            project,
            download_dir,
            proctype,
            resource,
            file,
            sesstype,
            session
        )
    else:
        g.download_proctype(
            project,
            download_dir,
            proctype,
            resource,
            file,
            sesstype,
            analysis,
            session
        )


@cli.command('switch')
@click.option('--type', '-t', 'proctype', multiple=False, required=True)
@click.option('--old', '-o', 'oldstatus', multiple=False, required=True)
@click.option('--new', '-n', 'newstatus', multiple=False, required=True)
@click.option('--project', '-p', 'project', required=True)
@click.option('--sesstype', '-s', 'sesstype', multiple=True, required=False)
@click.option('--session', '-e', 'session', multiple=True, required=False)
def switch_status(
    project,
    proctype,
    oldstatus,
    newstatus,
    sesstype,
    session
):
    click.echo('garjus! switch status')
    Garjus().switch_status(
        project,
        proctype,
        oldstatus,
        newstatus,
        sesstype,
        session
    )


@cli.command('run')
@click.option('--project', '-p', 'project', required=True)
@click.option('--subjects', '-s', 'subjects', required=False)
@click.option('--exclude', '-x', 'exclude', required=False)
@click.option('--repo', '-r', 'repo', required=True)
@click.option('--dir', '-d', 'jobdir', type=click.Path(), required=True)
@click.option('--imagedir', '-i', 'imagedir', type=click.Path(), required=False)
@click.option('--csv', '-c', 'csv', required=False)
@click.option('--yaml', '-y', 'yamlfile', type=click.Path(), required=False)
def run(project, subjects, repo, jobdir, csv, yamlfile, imagedir, exclude):
    click.echo('garjus! run')

    g = Garjus()
    g.run_analysis(project, subjects, repo, jobdir, csv, yamlfile, imagedir, exclude)


@cli.command('finish')
@click.argument('analysis_id', required=True)
@click.argument('analysis_dir', required=True)
@click.option('--project', '-p', 'project', required=True)
@click.option('--processor', '-y', 'processor', required=False)
def finish(project, analysis_id, analysis_dir, processor):
    click.echo('garjus! finish')
    g = Garjus()
    g.finish_analysis(project, analysis_id, analysis_dir, processor)


@cli.command('tasks')
def tasks():
    click.echo('garjus! tasks')
    g = Garjus()
    pprint.pprint(g.tasks())


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
        'analyses',
        'scans'
    ]),
    required=False,
    nargs=-1)
@click.option('--project', '-p', 'project', multiple=True)
@click.option('--types', '-t', 'types', multiple=True, required=False)
def update(choice, project, types):
    click.echo('garjus! update')
    g = Garjus()
    g.update(projects=project, choices=choice, types=types)
    click.echo('ALL DONE!')


@cli.command('progress')
@click.option('--project', '-p', 'project')
def progress(project):
    click.echo('garjus! progress')
    if project:
        project = project.split(',')

    g = Garjus()
    pprint.pprint(g.progress(projects=project))


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
@click.option('--analysis', '-a', 'analysis', required=False)
@click.option('--persubject', is_flag=True)
@click.option('--sessions', '-e', 'sessions', required=False)
@click.argument('csv', required=True)
def stats(projects, proctypes, sesstypes, csv, persubject, analysis, sessions):
    click.echo('garjus! stats')
    Garjus().export_stats(
        projects,
        proctypes,
        sesstypes,
        csv,
        persubject,
        analysis,
        sessions)


@cli.command('export')
@click.option('--projects', '-p', 'projects', required=True)
@click.option('--types', '-t', 'proctypes', required=False)
@click.option('--sesstypes', '-s', 'sesstypes', required=False)
@click.option('--analysis', '-a', 'analysis', required=False)
@click.option('--sessions', '-e', 'sessions', required=False)
@click.argument('filename', required=True)
def export(filename, projects, proctypes, sesstypes, analysis, sessions):
    click.echo('garjus! export')
    Garjus().export_zip(
        filename,
        projects,
        proctypes,
        sesstypes,
        analysis,
        sessions)


@cli.command('statshot')
@click.option('--projects', '-p', 'projects', required=True, multiple=True)
@click.option('--proctypes', '-t', 'proctypes', required=False)
@click.option('--sesstypes', '-s', 'sesstypes', required=False)
@click.option('--exclude', '-x', 'exclude', required=False)
@click.option('--guid/--no-guid', default=False)
@click.option('--ident/--no-ident', default=False)
def statshot(projects, proctypes, sesstypes, exclude, guid, ident):
    click.echo('garjus! statshot')

    # Split projects into lists    
    projects = [x.split(',') for x in projects]

    # Flatten to single list
    projects = sum(projects, [])

    Garjus().statshot(
        projects,
        proctypes,
        sesstypes,
        exclude,
        guid=guid,
        ident=ident)

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


@cli.command('importnifti')
@click.argument('src', required=True)
@click.argument('dst', required=True)
@click.option('--modality', '-m', 'modality', required=False)
def import_nifti(src, dst, modality):
    click.echo('garjus! importnifti')
    g = Garjus()
    g.import_nifti(src, dst, modality=modality)


@cli.command('importedat')
@click.argument('src', required=True)
@click.argument('dst', required=True)
def import_edat(src, dst):
    click.echo('garjus! importedat')
    g = Garjus()
    g.import_edat(src, dst)


@cli.command('pdf')
@click.argument('src', required=True)
@click.option('--project', '-p', 'project', required=True)
def export_pdf(src, project):
    click.echo('garjus! pdf')
    g = Garjus()
    g.pdf(src, project)


@cli.command('image03csv')
@click.option('--project', '-p', 'project', required=True)
@click.option(
    '--start', '-s', 'startdate', type=click.DateTime(formats=['%Y-%m-%d']))
@click.option(
    '--end', '-e', 'enddate', type=click.DateTime(formats=['%Y-%m-%d']))
@click.option('--site', multiple=True)
def image03csv(project, startdate, enddate, site):
    click.echo('garjus! image03csv')
    g = Garjus()
    g.image03csv(project, startdate, enddate, site)


@cli.command('retry')
@click.option('--project', '-p', 'project', required=True)
@click.option('--type', '-t', 'proctype', required=False)
def retry(project, proctype=None):
    click.echo('garjus! retry')
    g = Garjus()
    g.retry(project, proctype)


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
@click.option('--procstatus', '-s', 'procstatus', required=False)
@click.option('--qcstatus', '-q', 'qcstatus', required=False)
def delete(project, proctype, procstatus=None, qcstatus=None):
    click.echo('garjus! delete')
    g = Garjus()
    g.delete_proctype(
        project,
        proctype,
        procstatus=procstatus,
        qcstatus=qcstatus)


@cli.command('cleanup')
def cleanup():
    click.echo('garjus! cleanup')
    g = Garjus()
    g.delete_bad_tasks()


@cli.command('dashboard')
@click.option('--auth', 'auth_file', required=False)
@click.option('--login', required=False, is_flag=True)
@click.option('--demo', required=False, is_flag=True)
def dashboard(auth_file=None, login=False, demo=False):
    import webbrowser
    url = 'http://localhost:8050'

    if demo:
        from .dashboard.demo import app
    elif login:
        from .dashboard.login import app
    else:
        from .dashboard.index import app

        if auth_file:
            import yaml
            import dash_auth

            # Load user passwords to use dash's basic authentication
            with open(auth_file, 'rt') as file:
                _data = yaml.load(file, yaml.SafeLoader)
                dash_auth.BasicAuth(
                    app,
                    _data['VALID_USERNAME_PASSWORD_PAIRS']
                )

    # Open URL in a new tab, if a browser window is already open.
    webbrowser.open_new_tab(url)

    # start up a dashboard app
    app.run_server(host='0.0.0.0')


@cli.command('quicktest')
def quicktest():
    click.echo('garjus!')

    try:
        redcap_project = Garjus._default_redcap()
    except Exception:
        click.echo('could not connect to REDCap')
        return

    try:
        xnat_interface = Garjus._default_xnat()
    except Exception:
        click.echo('could not connect to XNAT')
        return

    try:
        g = Garjus(redcap_project, xnat_interface)
    except Exception:
        click.echo('something went wrong')
        return

    g.scans()
    click.echo('all good!')
