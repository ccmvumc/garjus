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

    if not output:
        return

    res = xnat.select_project(project).resource(output)

    for f in res.files():
        print(f)

    # Get report on XNAT
    xnat_report = res.file('report.pdf')
    if not xnat_report.exists():
        print('No report', project, analysis_id, output)
        return

    # Get report on REDCap
    cur_report = a[report_field]
    if cur_report:
        print(f'report found:{cur_report}', project, analysis_id, output)
        return

    print('no report, copying:', project, analysis_id, output)

    with tempfile.TemporaryDirectory() as tmpdir:
        # download file from xnat
        dst = f'{tmpdir}/report.pdf'
        print('DOWNLOAD from xnat', dst)
        xnat_report.get(dst)

        # upload file to redcap
        print('UPLOAD to redcap', dst)
        upload_file(rcq, project, report_field, dst, repeat_id=analysis_id)

    # TODO: log, batch, stats


def _check(rcq, xnat, projects):
    def_field = rcq.def_field

    rec = rcq.export_records(
        records=projects,
        forms=['analyses'],
        fields=[def_field])

    print(rec)

    rec = [x for x in rec if x['redcap_repeat_instrument'] == 'analyses']

    print(f'filter projects:{projects}')

    rec = [x for x in rec if x[def_field] in projects]

    for r in rec:
        print(r)
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
