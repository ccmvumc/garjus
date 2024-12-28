from garjus import Garjus


def get_stats(xnat, fullpath):
    filename = xnat.select(
        fullpath).resource('1stLEVEL').file('behavior.txt').get()
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

df = g.assessors(projects=['REMBRANDT'])

df = df[df.PROCTYPE == 'fmri_msit_v3']

df = df.sort_values('ASSR').reset_index()

df = df[df['PROCSTATUS'] == 'COMPLETE']
df = df[df['QCSTATUS'] != 'Failed']

for i, row in df.iterrows():
    if i != 341:
        continue

    print(i, row['full_path'])
    try:
        stats = get_stats(g.xnat(), row['full_path'])
    except Exception as err:
        print(err)
        continue

    print('set stats', stats)
    g.set_stats(
        row['PROJECT'],
        row['SUBJECT'],
        row['SESSION'],
        row['ASSR'],
        stats)

print('DONE!')
