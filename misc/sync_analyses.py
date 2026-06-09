from garjus import Garjus
from garjus.utils_redcap import upload_file
import tempfile

# must be run on system with access to both XNAT and REDCap via garjus
# copy analyses PDF/LOG/PBS from XNAT analyses to REDCAp records
# Get analyses records from REDCap
# Iterate each, checking against XNAT
# Copy files from XNAT to REDCap as needed


def _check_analysis(rcq, xnat, a):
    project = a[rcq.def_field]
    output = a['analysis_output']
    analysis_id = a['redcap_repeat_instance']
    report_field = 'analysis_reportfile'
    log_field = 'analysis_logfile'
    batch_field = 'analysis_batchfile'
    stats_field = 'analysis_statsfile'    
    report_file = 'report.pdf'

    if not output:
        return

    log_file = f'{output}.txt'
    batch_file = f'{output}.slurm'
    stats_file = 'stats.csv'

    if a[log_field] and a[batch_field]:
        print(f'log/batch files already on REDCap:{project}:{analysis_id}:{output}')
        return

    # Get list of files on xnat for analysis output
    res = xnat.select_project(project).resource(output)
    files = list(res.files().get())
    print(f'{output}:xnat files=', files)

    if log_file not in files and batch_file not in files:
        print(f'log/batch files not on XNAT:{project}:{analysis_id}:{output}')
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        print(f'copying:{project}:{analysis_id}:{output}')

        if not a[report_field] and report_file in files:
            # download file from xnat
            src = report_file
            dst = f'{tmpdir}/{src}'
            print('DOWNLOAD from xnat', dst)
            res.file(src).get(dst)

            # upload file to redcap
            print('UPLOAD to redcap', dst)
            upload_file(rcq, project, report_field, dst, repeat_id=analysis_id)

        if not a[log_field] and log_file in files:
            # download file from xnat
            src = log_file
            dst = f'{tmpdir}/{src}'
            print(f'DOWNLOAD from xnat:{dst}')
            res.file(src).get(dst)

            # upload file to redcap
            print('UPLOAD to redcap', dst)
            upload_file(rcq, project, log_field, dst, repeat_id=analysis_id)

        if not a[batch_field] and batch_file in files:
            # download file from xnat
            src = batch_file
            dst = f'{tmpdir}/{src}'
            print(f'DOWNLOAD from xnat:{dst}')
            res.file(src).get(dst)

            # upload file to redcap
            print('UPLOAD to redcap', dst)
            upload_file(rcq, project, batch_field, dst, repeat_id=analysis_id)

        if not a[stats_field] and 'stats.csv' in files:
            # download file from xnat
            src = stats_file
            dst = f'{tmpdir}/{src}'
            print(f'DOWNLOAD from xnat:{dst}')
            res.file(src).get(dst)

            # upload file to redcap
            print('UPLOAD to redcap', dst)
            upload_file(rcq, project, stats_field, dst, repeat_id=analysis_id)


def _check(rcq, xnat, projects):
    # Get analyses records from redcap
    rec = rcq.export_records(
        records=projects,
        forms=['analyses']
    )

    # Filter out extra records
    rec = [x for x in rec if x['redcap_repeat_instrument'] == 'analyses']

    # Only complete
    rec = [x for x in rec if x['analysis_status'] == 'READY']

    for r in rec:
        _check_analysis(rcq, xnat, r)

def main(g, projects):
    print('loading analyses')

    rcq = g._rcq
    xnat = g.xnat()
    _check(rcq, xnat, projects)


if __name__ == '__main__':
    g = Garjus()
    projects = ['CHAMP', 'D3']
    main(g, projects)
