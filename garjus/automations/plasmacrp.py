import pandas as pd
from garjus import Garjus


def _load(project, data):
    # Load the data back to redcap
    try:
        _response = project.import_records(data)
        assert 'count' in _response
        return True
    except (AssertionError, Exception) as err:
        logger.error(err)
        return False



subj2hscrp = {}
subj2order = {}
id2subj = {}

# Get the redcap via garjus
g = Garjus()
rc = g.primary('D3')

# Load file data
df = pd.read_excel('/Users/boydb1/VUMC Plasma hsCRP Nov 2024.xlsx')
for i, row in df.iterrows():
    order = row['Assay order ']
    subject = row['Subject Number']
    hscrp = row['hsCRP (mg/L)']
    subj2hscrp[subject] =  str(hscrp)
    subj2order[subject] = str(order)

# Get the field names for main id and secondary id
def_field = rc.def_field
sec_field = rc.export_project_info()['secondary_unique_field']

# Load the subject numbers
if sec_field:
    rec = rc.export_records(fields=[def_field, sec_field])
    id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}
else:
    rec = rc.export_records(fields=[def_field])
    id2subj = {x[def_field]: x[def_field] for x in rec if x[def_field]}

# Get the plasma records
records = rc.export_records(fields=['plasma_hscrp', 'plasma_file', 'plasma_assayorder'])
records = [x for x in records if x['redcap_event_name'] in ['baseline_arm_1', 'baseline_arm_2']]
records = [x for x in records if not x['redcap_repeat_instrument']]

# Iterate looking for blanks
for r in records:
    record_id = r['record_id']

    if r.get('plasma_hscrp', False):
        print('existing', record_id)
        continue
 
    # Get the subject data from file data
    subj = id2subj[record_id]
    hscrp = subj2hscrp.get(subj, None)
    order = subj2order.get(subj, None)
    if not hscrp or not order:
        print('nope', subj)
        continue

    # Upload new data
    data = {
        rc.def_field: record_id,
        'redcap_event_name': r['redcap_event_name'],
        'plasma_hscrp': hscrp,
        'plasma_assayorder': order,
        'plasma_file': 'VUMC Plasma hsCRP Nov 2024.xlsx',
        'plasma_complete': '2',
    }
    _load(rc, [data])

print('done!')
