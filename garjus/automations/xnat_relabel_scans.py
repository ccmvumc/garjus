import json


SCAN_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
ID,\
label,\
project,\
subject_label,\
xnat:imagesessiondata/acquisition_site,\
xnat:imagescandata/id,\
xnat:imagescandata/type,\
xnat:imagescandata/quality,\
xnat:imagesessiondata/date'


def process_project(xnat, project, relabels):
    """Apply relabels to project scans"""
    results = relabel_scans(xnat, project, relabels)
    return results


def relabel_scans(xnat, project, relabels):
    results = []

    # get a list of scans from the project
    scan_uri = '{}&project={}'.format(SCAN_URI, project)
    json_data = json.loads(xnat._exec(scan_uri, 'GET'),  strict=False)
    scan_list = json_data['ResultSet']['Result']

    # iterate scan and relabel if needed
    for cur_scan in scan_list:
        scan_type = cur_scan['xnat:imagescandata/type']
        if scan_type in relabels:
            proj = cur_scan['project']
            subj = cur_scan['subject_label']
            sess = cur_scan['label']
            scan = cur_scan['xnat:imagescandata/id']

            scan_obj = xnat.select_scan(proj, subj, sess, scan)
            if not scan_obj.exists():
                print('nope')
                continue

            # Set the new type
            new_type = relabels[scan_type]
            scan_obj.attrs.set('xnat:imagescandata/type', new_type)
            results.append({
                    'description': f'xnat_relabel_scans:{new_type}',
                    'result': 'COMPLETE',
                    'category': 'xnat_relabel_scans',
                    'subject': subj,
                    'session': sess,
                    'scan': scan})

    return results
