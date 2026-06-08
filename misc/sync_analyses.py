from garjus import Garjus
from garjus.utils_redcap import upload_file
import tempfile

# must be run on system with access to both XNAT and REDCap via garjus
# copy analyses PDF/LOG/PBS from XNAT analyses to REDCAp records
# Get analyses records from REDCap
# Iterate each, checking against XNAT
# Copy files from XNAT to REDCap as needed


print('loading analyses')
g = Garjus()

print(g.projects())

df = g.analyses(projects=['CHAMP', 'D3'], download=False)

print(df.columns)


def _check_report(g, a):
    if not a['OUTPUT']:
        return

    res = g.xnat().select_project(a['PROJECT']).resource(a['OUTPUT'])
    xnat_res_file = res.file('report.pdf')

    if not xnat_res_file.exists():
        print('No PDF', a['PROJECT'], a['ID'], a['OUTPUT'])
        return

    rcq = g._rcq
    rec = rcq.export_records(records=[a['PROJECT']], forms=['analyses'], fields=['analysis_reportfile'])   
    rec = [x for x in rec if x['redcap_repeat_instance'] == a['ID']]

    if not rec:
        print('records not found:', a['PROJECT'], a['ID'], a['OUTPUT'])
        return

    cur_report = rec[0]['analysis_reportfile']
    if cur_report:
        print(f'report found:{cur_report}', a['PROJECT'], a['ID'], a['OUTPUT'])
        return

    print('no report, copying:', a['PROJECT'], a['ID'], a['OUTPUT'])
    with tempfile.TemporaryDirectory() as tmpdir:
        # download file from xnat
        dst = f'{tmpdir}/report.pdf'
        print('DOWNLOAD from xnat', dst)
        xnat_res_file.get(dst)

        # upload file to redcap
        print('UPLOAD to redcap', dst)
        upload_file(rcq, a['PROJECT'], 'analysis_reportfile', dst, repeat_id=[a['ID']])


def _check_log(g, a):
    file_field = 'analysis_logfile'

    if not a['OUTPUT']:
        return

    filename = f'{a["OUTPUT"]}.txt'

    res = g.xnat().select_project(a['PROJECT']).resource(a['OUTPUT'])

    xnat_res_file = res.file(filename)

    if not xnat_res_file.exists():
        print(f'{filename}:file not on XNAT', a['PROJECT'], a['ID'], a['OUTPUT'])
        return

    rcq = g._rcq
    rec = rcq.export_records(records=[a['PROJECT']], forms=['analyses'], fields=[file_field])   
    rec = [x for x in rec if x['redcap_repeat_instance'] == a['ID']]

    if not rec:
        print('records not found:', a['PROJECT'], a['ID'], a['OUTPUT'])
        return

    cur_file = rec[0][file_field]
    if cur_file:
        print(f'{file_field}:found:{cur_file}', a['PROJECT'], a['ID'], a['OUTPUT'])
        return

    print(f'no {file_field}, copying:', a['PROJECT'], a['ID'], a['OUTPUT'])
    with tempfile.TemporaryDirectory() as tmpdir:
        # download file from xnat
        dst = f'{tmpdir}/log.txt'
        print('DOWNLOAD from xnat', dst)
        xnat_res_file.get(dst)

        # upload file to redcap
        print('UPLOAD to redcap', dst)
        upload_file(rcq, a['PROJECT'], file_field, dst, repeat_id=[a['ID']])




for i, a in df.iterrows():
    #_check_report(g, a)
    _check_log(g, a)
    #_check_batch(g, a)
    #_check_stats(g, a)
