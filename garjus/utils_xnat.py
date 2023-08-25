import os
import sys
import tempfile
import json
import html
import pathlib
import logging
from zipfile import ZipFile, ZIP_DEFLATED

import dax


logger = logging.getLogger('garjus.utils_xnat')


SCAN_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
project,\
subject_label,\
session_label,\
session_type,\
xnat:imagesessiondata/note,\
xnat:imagesessiondata/date,\
tracer_name,\
xnat:imagesessiondata/acquisition_site,\
xnat:imagesessiondata/label,\
xnat:imagescandata/id,\
xnat:imagescandata/type,\
xnat:imagescandata/quality,\
xnat:imagescandata/file/label'

ASSR_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
project,\
subject_label,\
session_label,\
session_type,\
xnat:imagesessiondata/acquisition_site,\
xnat:imagesessiondata/note,\
xnat:imagesessiondata/date,\
xnat:imagesessiondata/label,\
proc:genprocdata/label,\
proc:genprocdata/procstatus,\
proc:genprocdata/proctype,\
proc:genprocdata/validation/status,\
proc:genprocdata/validation/date,\
proc:genprocdata/validation/validated_by,\
proc:genprocdata/jobstartdate,\
last_modified,\
proc:genprocdata/inputs'
# Including resource labels takes too long: proc:genprocdata/out/file/label'

SGP_URI = '/REST/experiments?xsiType=proc:subjgenprocdata\
&columns=\
project,\
subject_label,\
date,\
label,\
xsiType,\
proc:subjgenprocdata/procstatus,\
proc:subjgenprocdata/proctype,\
proc:subjgenprocdata/validation/status,\
proc:subjgenprocdata/inputs,\
last_modified'

SCAN_RENAME = {
    'project': 'PROJECT',
    'subject_label': 'SUBJECT',
    'session_label': 'SESSION',
    'session_type': 'SESSTYPE',
    'tracer_name': 'TRACER',
    'xnat:imagesessiondata/note': 'NOTE',
    'xnat:imagesessiondata/date': 'DATE',
    'xnat:imagesessiondata/acquisition_site': 'SITE',
    'xnat:imagescandata/id': 'SCANID',
    'xnat:imagescandata/type': 'SCANTYPE',
    'xnat:imagescandata/quality': 'QUALITY',
    'xsiType': 'XSITYPE',
    'xnat:imagescandata/file/label': 'RESOURCES'
}

ASSR_RENAME = {
    'project': 'PROJECT',
    'subject_label': 'SUBJECT',
    'session_label': 'SESSION',
    'session_type': 'SESSTYPE',
    'xnat:imagesessiondata/note': 'NOTE',
    'xnat:imagesessiondata/date': 'DATE',
    'xnat:imagesessiondata/acquisition_site': 'SITE',
    'proc:genprocdata/label': 'ASSR',
    'proc:genprocdata/procstatus': 'PROCSTATUS',
    'proc:genprocdata/proctype': 'PROCTYPE',
    'proc:genprocdata/jobstartdate': 'JOBDATE',
    'proc:genprocdata/validation/status': 'QCSTATUS',
    'proc:genprocdata/validation/date': 'QCDATE',
    'proc:genprocdata/validation/validated_by': 'QCBY',
    'xsiType': 'XSITYPE',
    'proc:genprocdata/inputs': 'INPUTS',
}

SGP_RENAME = {
    'project': 'PROJECT',
    'subject_label': 'SUBJECT',
    'date': 'DATE',
    'label': 'ASSR',
    'xsiType': 'XSITYPE',
    'proc:subjgenprocdata/procstatus': 'PROCSTATUS',
    'proc:subjgenprocdata/proctype': 'PROCTYPE',
    'proc:subjgenprocdata/validation/status': 'QCSTATUS',
    'proc:subjgenprocdata/inputs': 'INPUTS'}

XSI2MOD = {
    'xnat:eegSessionData': 'EEG',
    'xnat:mrSessionData': 'MR',
    'xnat:petSessionData': 'PET'}


def decode_inputs(inputs):
    if inputs:
        inputs = decode_url_json_string(inputs)
        return inputs
    else:
        return {}


def decode_url_json_string(json_string):
    """
    Load a string representing serialised json into
    :param json_string:
    :return:
    """
    strings = json.loads(html.unescape(json_string),
                         object_pairs_hook=parse_json_pairs)
    return strings


def parse_json_pairs(pairs):
    """
    An object hook for the json.loads method. Used in decode_url_json_string.
    :param pairs:
    :return: A dictionary of parsed json
    """
    sink_pairs = []
    for k, v in pairs:
        if isinstance(k, str):
            k = k.encode('utf-8').decode()
        if isinstance(v, str):
            v = v.encode('utf-8').decode()
        sink_pairs.append((k, v))
    return dict(sink_pairs)


MR_EXP_ATTRS = [
    'xnat:experimentData/date',
    'xnat:experimentData/visit_id',
    'xnat:experimentData/time',
    'xnat:experimentData/note',
    'xnat:experimentData/investigator/firstname',
    'xnat:experimentData/investigator/lastname',
    'xnat:imageSessionData/scanner/manufacturer',
    'xnat:imageSessionData/scanner/model',
    'xnat:imageSessionData/operator',
    'xnat:imageSessionData/dcmAccessionNumber',
    'xnat:imageSessionData/dcmPatientId',
    'xnat:imageSessionData/dcmPatientName',
    'xnat:imageSessionData/session_type',
    'xnat:imageSessionData/modality',
    'xnat:imageSessionData/UID',
    'xnat:mrSessionData/coil',
    'xnat:mrSessionData/fieldStrength',
    'xnat:mrSessionData/marker',
    'xnat:mrSessionData/stabilization'
]

OTHER_DICOM_SCAN_ATTRS = [
    'xnat:imageScanData/type',
    'xnat:imageScanData/UID',
    'xnat:imageScanData/note',
    'xnat:imageScanData/quality',
    'xnat:imageScanData/condition',
    'xnat:imageScanData/series_description',
    'xnat:imageScanData/documentation',
    'xnat:imageScanData/frames',
    'xnat:imageScanData/startTime',
    'xnat:imageScanData/scanner/manufacturer',
    'xnat:imageScanData/scanner/model'
]

MR_SCAN_ATTRS = [
    'xnat:imageScanData/type',
    'xnat:imageScanData/UID',
    'xnat:imageScanData/note',
    'xnat:imageScanData/quality',
    'xnat:imageScanData/condition',
    'xnat:imageScanData/series_description',
    'xnat:imageScanData/documentation',
    'xnat:imageScanData/frames',
    'xnat:imageScanData/startTime',
    'xnat:imageScanData/scanner/manufacturer',
    'xnat:imageScanData/scanner/model',
    'xnat:mrScanData/parameters/flip',
    'xnat:mrScanData/parameters/orientation',
    'xnat:mrScanData/parameters/tr',
    'xnat:mrScanData/parameters/ti',
    'xnat:mrScanData/parameters/te',
    'xnat:mrScanData/parameters/sequence',
    'xnat:mrScanData/parameters/imageType',
    'xnat:mrScanData/parameters/scanSequence',
    'xnat:mrScanData/parameters/seqVariant',
    'xnat:mrScanData/parameters/scanOptions',
    'xnat:mrScanData/parameters/acqType',
    'xnat:mrScanData/parameters/pixelBandwidth',
    'xnat:mrScanData/parameters/voxelRes/x',
    'xnat:mrScanData/parameters/voxelRes/y',
    'xnat:mrScanData/parameters/voxelRes/z',
    'xnat:mrScanData/parameters/fov/x',
    'xnat:mrScanData/parameters/fov/y',
    'xnat:mrScanData/parameters/matrix/x',
    'xnat:mrScanData/parameters/matrix/y',
    'xnat:mrScanData/parameters/partitions',
    'xnat:mrScanData/fieldStrength',
    'xnat:mrScanData/marker',
    'xnat:mrScanData/stabilization',
    'xnat:mrScanData/coil'
]

SC_SCAN_ATTRS = [
    'xnat:imageScanData/type',
    'xnat:imageScanData/UID',
    'xnat:imageScanData/note',
    'xnat:imageScanData/quality',
    'xnat:imageScanData/condition',
    'xnat:imageScanData/series_description',
    'xnat:imageScanData/documentation',
    'xnat:imageScanData/frames',
    'xnat:imageScanData/scanner/manufacturer',
    'xnat:imageScanData/scanner/model'
]

PET_EXP_ATTRS = [
    'xnat:experimentData/date',
    'xnat:experimentData/time',
    'xnat:experimentData/note',
    'xnat:experimentData/acquisition_site',
    'xnat:imageSessionData/scanner',
    'xnat:imageSessionData/scanner/manufacturer',
    'xnat:imageSessionData/scanner/model',
    'xnat:imageSessionData/dcmAccessionNumber',
    'xnat:imageSessionData/dcmPatientId',
    'xnat:imageSessionData/dcmPatientName',
    'xnat:imageSessionData/session_type',
    'xnat:imageSessionData/modality',
    'xnat:imageSessionData/study_id',
    'tracer_name',
    'tracer_startTime',
    'tracer_dose',
    'tracer_isotope',
    'tracer_half-life',
]

CT_EXP_ATTRS = [
    'xnat:experimentData/date',
    'xnat:experimentData/visit_id',
    'xnat:experimentData/time',
    'xnat:experimentData/note',
    'xnat:experimentData/investigator/firstname',
    'xnat:experimentData/investigator/lastname',
    'xnat:imageSessionData/scanner/manufacturer',
    'xnat:imageSessionData/scanner/model',
    'xnat:imageSessionData/operator',
    'xnat:imageSessionData/dcmAccessionNumber',
    'xnat:imageSessionData/dcmPatientId',
    'xnat:imageSessionData/dcmPatientName',
    'xnat:imageSessionData/session_type',
    'xnat:imageSessionData/modality',
    'xnat:imageSessionData/UID'
]

PET_SCAN_ATTRS = [
    'xnat:imageScanData/type',
    'xnat:imageScanData/UID',
    'xnat:imageScanData/note',
    'xnat:imageScanData/quality',
    'xnat:imageScanData/condition',
    'xnat:imageScanData/series_description',
    'xnat:imageScanData/documentation',
    'xnat:imageScanData/frames',
    'xnat:imageScanData/scanner',
    'xnat:imageScanData/scanner/manufacturer',
    'xnat:imageScanData/scanner/model',
    'xnat:imageScanData/startTime',
    'xnat:petScanData/parameters/orientation',
    'xnat:petScanData/parameters/originalFileName',
    'xnat:petScanData/parameters/systemType',
    'xnat:petScanData/parameters/fileType',
    'xnat:petScanData/parameters/transaxialFOV',
    'xnat:petScanData/parameters/acqType',
    'xnat:petScanData/parameters/facility',
    'xnat:petScanData/parameters/numPlanes',
    'xnat:petScanData/parameters/frames/numFrames',
    'xnat:petScanData/parameters/numGates',
    'xnat:petScanData/parameters/planeSeparation',
    'xnat:petScanData/parameters/binSize',
    'xnat:petScanData/parameters/dataType'
]

CT_SCAN_ATTRS = [
    'xnat:imageScanData/type',
    'xnat:imageScanData/UID',
    'xnat:imageScanData/note',
    'xnat:imageScanData/quality',
    'xnat:imageScanData/condition',
    'xnat:imageScanData/series_description',
    'xnat:imageScanData/documentation',
    'xnat:imageScanData/frames',
    'xnat:imageScanData/scanner/manufacturer',
    'xnat:imageScanData/scanner/model'
]


def check_attributes(src_obj, dest_obj, dtype=None):
    '''Check that attributes on dest match those on src'''

    if dtype is None:
        dtype = src_obj.datatype()

    if dtype == 'xnat:mrSessionData':
        attr_list = MR_EXP_ATTRS
    elif dtype == 'xnat:mrScanData':
        attr_list = MR_SCAN_ATTRS
    elif dtype == 'xnat:scScanData':
        attr_list = SC_SCAN_ATTRS
    elif dtype == 'xnat:petScanData':
        attr_list = PET_SCAN_ATTRS
    elif dtype == 'xnat:ctScanData':
        attr_list = CT_SCAN_ATTRS
    elif dtype == 'xnat:otherDicomScanData':
        attr_list = OTHER_DICOM_SCAN_ATTRS
    else:
        logger.warning(f'unknown Type:{dtype}')
        return

    for a in attr_list:
        src_v = src_obj.attrs.get(a)
        src_v = src_v.replace("\\", "|")
        dest_v = dest_obj.attrs.get(a)
        if src_v != dest_v:
            logger.warning('mismatch:{a}:src={src_v}, dst={dest_v}')
            dest_obj.attrs.set(a, src_v)


def copy_attrs(src_obj, dest_obj, attr_list):
    """ Copies list of attributes form source to destination"""
    try:
        src_attrs = src_obj.attrs.mget(attr_list)
    except IndexError:
        logger.warning('failed with full attributes, trying minimal set')
        attr_list = OTHER_DICOM_SCAN_ATTRS
        src_attrs = src_obj.attrs.mget(attr_list)

    src_list = dict(list(zip(attr_list, src_attrs)))

    # NOTE: For some reason need to set te again b/c a bug somewhere sets te
    # to sequence name
    te_key = 'xnat:mrScanData/parameters/te'
    if te_key in src_list:
        src_list[te_key] = src_obj.attrs.get(te_key)

    dest_obj.attrs.mset(src_list)


def copy_attributes(src_obj, dest_obj):
    '''Copy attributes from src to dest'''
    src_type = src_obj.datatype()

    if src_type == 'xnat:mrSessionData':
        copy_attrs(src_obj, dest_obj, MR_EXP_ATTRS)
    elif src_type == 'xnat:petSessionData':
        copy_attrs(src_obj, dest_obj, PET_EXP_ATTRS)
    elif src_type == 'xnat:ctSessionData':
        copy_attrs(src_obj, dest_obj, CT_EXP_ATTRS)
    elif src_type == 'xnat:mrScanData':
        copy_attrs(src_obj, dest_obj, MR_SCAN_ATTRS)
    elif src_type == 'xnat:petScanData':
        copy_attrs(src_obj, dest_obj, PET_SCAN_ATTRS)
    elif src_type == 'xnat:ctScanData':
        copy_attrs(src_obj, dest_obj, CT_SCAN_ATTRS)
    elif src_type == 'xnat:scScanData':
        copy_attrs(src_obj, dest_obj, SC_SCAN_ATTRS)
    elif src_type == 'xnat:otherDicomScanData':
        copy_attrs(src_obj, dest_obj, OTHER_DICOM_SCAN_ATTRS)
    else:
        logger.error(f'cannot copy attributes, unsupported type:{src_type}')


def copy_res_zip(src_r, dest_r):
    '''
    Copy a resource from XNAT source to XNAT destination using local cache
    in between
    '''
    try:
        # Download zip of resource
        logger.debug('downloading resource as zip')
        cache_z = src_r.get(tempfile.mkdtemp(), extract=False)

        # Upload zip of resource
        logger.debug('uploading resource as zip')
        dest_r.put_zip(cache_z, extract=True)

        # Delete local zip
        os.remove(cache_z)

    except IndexError:
        logger.error('copy failed:{}:{}'.format(cache_z, sys.exc_info()[0]))
        raise


def is_empty_resource(_res):
    '''Check if resource contains any files'''
    f_count = 0
    for f_in in _res.files().fetchall('obj'):
        f_count += 1
        break

    return f_count == 0


def copy_session(src, dst):
    logger.debug('uploading session attributes')
    dst.create(experiments=src.datatype())
    copy_attributes(src, dst)

    # Process each scan of session
    for src_scan in src.scans().fetchall('obj'):
        scan_label = src_scan.label()

        logger.debug(f'processing scan:{scan_label}')
        dst_scan = dst.scan(scan_label)
        copy_scan(src_scan, dst_scan)


def _file_count(res):
    return int(str(res).split('(')[1].split(' files')[0])


def copy_scan(src_scan, dst_scan):
    scan_type = src_scan.datatype()
    if scan_type == '':
        scan_type = 'xnat:otherDicomScanData'

    dst_scan.create(scans=scan_type)
    copy_attributes(src_scan, dst_scan)

    # Process each resource of scan
    for src_res in src_scan.resources().fetchall('obj'):
        res_label = src_res.label()

        logger.debug(f'processing resource:{res_label}')

        file_count = _file_count(src_res)
        if res_label == 'DICOM' and file_count > 1000:
            logger.debug(f'too many files, upload as DICOMZIP:{file_count}')
            dst_res = dst_scan.resource('DICOMZIP')
            copy_res_dicomzip(src_res, dst_res)
        else:
            dst_res = dst_scan.resource(res_label)
            copy_res(src_res, dst_res)


def copy_res_dicomzip(src_res, dst_res):
    '''
    Copy a DICOM resource from XNAT source to XNAT destination DICOMZIP/NIFTI
    '''
    try:
        # Download zip of resource
        logger.debug('downloading resource as zip')
        cache_z = src_res.get(tempfile.mkdtemp(), extract=False)

        # Upload zip of resource
        logger.debug('Uploading resource as zip, no extract')
        dst_res.put_zip(cache_z, extract=False)

        # Delete local zip
        os.remove(cache_z)

    except IndexError:
        logger.error(f'failed to copy')
        raise


def copy_res(src_res, dst_res):
    try:
        logger.debug('copying resource as zip:'.format(src_res.label()))
        copy_res_zip(src_res, dst_res)
        return
    except Exception as err:
        try:
            logger.debug('failed, trying again:{}'.format(src_res.label()))
            copy_res_zip(src_res, dst_res)
            return
        except Exception:
            logger.error(f'failed twice to copy resource as zip:{err}')


def copy_xnat_session(src, dst):
    (src_proj, src_subj, src_sess) = src.split('/')
    (dst_proj, dst_subj, dst_sess) = dst.split('/')

    with dax.XnatUtils.get_interface() as xnat:
        src_sess_obj = xnat.select_session(src_proj, src_subj, src_sess)
        if not src_sess_obj.exists():
            logger.info('src session does not exist')
            return

        dst_proj_obj = xnat.select_project(dst_proj)
        if not dst_proj_obj.exists():
            logger.info('dest project does not exist, refusing to create')
            return

        dst_subj_obj = dst_proj_obj.subject(dst_subj)
        if not dst_subj_obj.exists():
            logger.info('destination subject does not exist, creating')
            dst_subj_obj.create()
        else:
            logger.info('destination subject exists', dst_subj)

        dst_sess_obj = dst_subj_obj.experiment(dst_sess)
        if dst_sess_obj.exists():
            logger.info('destination session exists, refusing to overwrite')
            return

        logger.info(f'destination session does not exist, creating:{dst_sess}')
        copy_session(src_sess_obj, dst_sess_obj)


def refresh_dicom_catalog(xnat, proj, subj, sess, scan):
    _uri = '/data/services/refresh/catalog?resource='
    _uri += f'/archive/projects/{proj}/subjects/{subj}/experiments/{sess}/scans/{scan}/resources/DICOM'
    logger.info('refreshing dicom catalog')
    xnat.post(_uri)


def upload_files(inputfiles, resource):
    logger.debug(f'uploading:{inputfiles}')
    return dax.XnatUtils.upload_files_to_obj(inputfiles, resource, remove=True)


def upload_file(inputfile, resource):
    logger.debug(f'uploading:{inputfile}')
    return dax.XnatUtils.upload_file_to_obj(inputfile, resource, remove=True)


def upload_dirzip(inputdir, resource):

    with tempfile.TemporaryDirectory() as tempdir:
        fzip = os.path.join(
            tempdir,
            '{}.zip'.format(pathlib.Path(inputdir).name))

        logger.info(f'create zip:{inputdir}')
        _create_zip(inputdir, fzip)

        logger.info(f'upload:{fzip}')
        dax.XnatUtils.upload_file_to_obj(fzip, resource)


def _create_zip(input_dir, output_zip):
    dir_path = pathlib.Path(input_dir)
    with ZipFile(output_zip, mode="w", compression=ZIP_DEFLATED, compresslevel=9) as archive:

        for file_path in dir_path.rglob("*.dcm"):
            archive.write(file_path, arcname=file_path.relative_to(dir_path))


def get_my_projects(xnat):
    #id,name,description,secondary_id,pi,project_invs,project_access,
    #project_access_img,insert_date,insert_user,user_role_6,last_accessed_6,
    #quarantine_status
    """Get result of xnat query."""
    uri = '/data/archive/projects?accessible=true'
    logger.debug(uri)
    json_data = json.loads(xnat._exec(uri, 'GET'), strict=False)
    result = json_data['ResultSet']['Result']
    _roles = ['Owners', 'Members', 'Collaborators']
    return [x['id'] for x in result if x['user_role_6'] in _roles]


#def get_user_favorites(xnat):
#    FAV_URI = '/data/archive/projects?favorite=True'
#    fav_json = get_json(xnat, FAV_URI)
#    data = [x['id'] for x in fav_json['ResultSet']['Result']]

#    return data


#def get_user_projects(xnat, username):

#    uri = '/xapi/users/{}/groups'.format(username)

#    # get from xnat and convert to list
#    data = get_json(xnat, uri)

    # format of group name is PROJECT_ROLE,
    # so we split on the underscore
#    data = sorted([x.rsplit('_', 1)[0] for x in data])

#    return data
