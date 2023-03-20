import fnmatch
import re
import json
import logging


SESS_URI = '/REST/experiments?xsiType=xnat:imagesessiondata&columns=\
ID,\
label,\
project,\
xsiType,\
subject_label,\
session_label,\
session_type,\
xnat:imagesessiondata/acquisition_site,\
xnat:imagesessiondata/date,\
modality'

# RELABELS
# If the key in [0] matches the value in [1],
# then change the value for key in [2] to the value in [3]
# if[0]==[1]: [2] = [3]


def process_project(xnat, project, relabels, replace):
    """Apply relabels to project sessions."""
    results = relabel_sessions(xnat, project, relabels)
    return results


def relabel(xnat, session, relabels, overwrite=False, replace=[]):
    cur_sess = session['label']
    cur_proj = session['project']
    cur_subj = session['subject_label']
    cur_site = session['xnat:imagesessiondata/acquisition_site']
    allowed_k1 = ['session_type', 'session_label']
    allowed_k2 = ['session_type', 'site']
    mset = {}

    # test each relabel
    for k1, v1, k2, v2 in relabels:
        if k1 not in allowed_k1:
            print('not allowed', k1)
            continue

        if k2 not in allowed_k2:
            print('not allowed', k2)
            continue

        # Try to match
        if not re.match(fnmatch.translate(v1), session[k1]):
            # no match
            continue

        if (overwrite is False) and (session[k2] and (session[k2] not in replace)):
            # There's already a value there
            #print('already set:{}:{}'.format(k2, session[k2]))
            continue
        elif session[k2] == v2:
            # have we already set this to the same thing
            #print('already set:{}:{}'.format(k2, session[k2]))
            continue

        # Add the relabel to mset
        if k2 == 'site':
            # We have to use the full path for site, no shortcut
            mset['xnat:imagesessiondata/acquisition_site'] = v2
        else:
            mset[k2] =  v2

    # Check for empty set
    if not mset:
        #print('{}:nothing to set'.format(cur_sess))
        return None

    # Connect to the session on xnat and apply new values
    logging.info(f'{cur_proj}:{cur_sess}:setting:{mset}')
    sess_obj = xnat.select_session(cur_proj, cur_subj, cur_sess)
    sess_obj.attrs.mset(mset)

    result =  {
        'description': 'xnat_relabel_session',
        'result': 'COMPLETE',
        'category': 'xnat_relabel_session',
        'subject': cur_subj,
        'session': cur_sess}

    return result


def relabel_sessions(xnat, project, relabels, overwrite=False, replace=[]):
    results = []

    # get a list of sessions from the project
    sess_uri = '{}&project={}'.format(SESS_URI, project)
    json_data = json.loads(xnat._exec(sess_uri, 'GET'),  strict=False)
    sessions = json_data['ResultSet']['Result']

    # iterate and relabel as needed
    for sess in sessions:
        sess['site'] = sess['xnat:imagesessiondata/acquisition_site']
        result = relabel(xnat, sess, relabels, overwrite, replace)
        if result:
            results.append(result)

    return results
