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


def _check(g, a):
    if not a['OUTPUT']:
        return

    res = g.xnat().select_project(a['PROJECT']).resource(a['OUTPUT'])
    xnat_res_file = res.file('report.pdf')

    if not xnat_res_file.exists():
        print('No PDF', a['PROJECT'], a['ID'], a['OUTPUT'])
        return

    print('report.pdf', a['PROJECT'], a['ID'], a['OUTPUT'])

    rcq = g._rcq
    rec = rcq.export_records(records=[a['PROJECT']], forms=['analyses'], fields=['analysis_reportfile'])   
    rec = [x for x in rec if x['redcap_repeat_instance'] == a['ID']]

    if not rec:
        print('records not found')
        return

    cur_report = rec[0]['analysis_reportfile']

    if cur_report:
        print(f'report found:{cur_report}')
        return

    print('no report, copy')
    with tempfile.TemporaryDirectory() as tmpdir:
        # download file from xnati
        dst = f'{tmpdir}/report.pdf'
        print('download from xnat', dst)
        xnat_res_file.get(dst)

        # upload file to redcap
        print('upload to redcap', dst)
        upload_file(rcq, a['PROJECT'], 'analysis_reportfile', dst, repeat_id=[a['ID']])


for i, a in df.iterrows():
    _check(g, a)

