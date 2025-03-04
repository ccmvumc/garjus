import pandas as pd
from garjus import Garjus
from garjus.utils_redcap import field2events


def _load(project, data):
    # Load the data back to redcap
    try:
        _response = project.import_records(data)
        assert 'count' in _response
        return True
    except (AssertionError, Exception) as err:
        print(err)
        return False


def _extract(filename):
    # Load file data
    df = pd.read_csv(filename)
    return df


def _transform(rc, df):
    subj2id = {}
    data = []
    def_field = rc.def_field
    sec_field = None

    # Load the subject numbers
    sec_field = rc.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = rc.export_records(fields=[def_field, sec_field])
        subj2id = {x[sec_field]: x[def_field] for x in rec if x[sec_field]}
    else:
        rec = rc.export_records(fields=[def_field])
        subj2id = {x[def_field]: x[def_field] for x in rec if x[def_field]}

    for i, row in df.iterrows():
        record_id = subj2id[row['subject']]
        data.append(
            {
                rc.def_field: record_id,
                'fallypride_accumbens': str(row['accumbens']),
                'fallypride_amygdala': str(row['amygdala']),
                'fallypride_caudate': str(row['caudate']),
                'fallypride_pallidum': str(row['pallidum']),
                'fallypride_putamen': str(row['putamen']),
                'fallypride_thalamus': str(row['thalamus']),
                'fallypride_complete': '2',
            }
        )

    # Get existing records
    _fields = ['fallypride_amygdala']
    _events = field2events(rc, 'fallypride_amygdala')
    records = rc.export_records(fields=_fields)
    records = [x for x in records if x['redcap_event_name'] in _events]

    existing = [x[def_field] for x in records if x.get('fallypride_amygdala', False)]

    data = [x for x in data if x[def_field] not in existing]

    # Copy event name from existing records
    for d in data:
        for r in records:
            if d[def_field] == r[def_field]:
                d['redcap_event_name'] = r['redcap_event_name']

    return data


if __name__ == "__main__":
    import sys

    filename = sys.argv[1]

    print(filename)

    # Get the redcap via garjus
    g = Garjus()
    rc = g.primary('D3')

    # Extract from file as pandas dataframe
    df = _extract(filename)

    # Prep for redcap by converting to records with field names that match redcap
    data = _transform(rc, df)

    # Upload to redcap
    print(f'loading:n={len(data)} records')
    _load(rc, data)
    print('done!')
