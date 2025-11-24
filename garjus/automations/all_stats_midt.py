from garjus import Garjus


def get_stats(xnat, fullpath, subject):
    filename = xnat.select(
        fullpath).resource('PREPROC').file(f'{subject}/FMRI/Baseline/behavior.txt').get()
    data = {}
    rows = []

    try:
        with open(filename) as f:
            rows = f.readlines()

        for r in rows:
            (k, v) = r.strip().replace('=', ',').split(',')
            data[k] = v
    except ValueError:
        print(f'cannot load stats file:{filename}')
        return {}

    return data


g = Garjus()

df = g.assessors(projects=['D3'])

df = df[df.PROCTYPE == 'fmri_midt_D3_v1']

df = df.sort_values('ASSR').reset_index()

df = df[df['PROCSTATUS'] == 'COMPLETE']
df = df[df['QCSTATUS'] != 'Failed']

for i, row in df.iterrows():
    subject = row['SUBJECT']

    try:
        print(i, row['full_path'], subject, 'get stats')
        stats = get_stats(g.xnat(), row['full_path'], subject)
    except Exception as err:
        print(err)
        continue

    print('set stats', stats)
    g.set_stats(
        row['PROJECT'],
        subject,
        row['SESSION'],
        row['ASSR'],
        stats)

print('DONE!')
