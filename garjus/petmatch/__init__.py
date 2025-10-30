import pandas as pd


SESS_URI = '/REST/experiments?xsiType=xnat:imagesessiondata\
&columns=\
project,\
xnat:imagesessiondata/sharing/share/project,\
subject_label,\
session_label,\
session_type,\
xnat:imagesessiondata/note,\
xnat:imagesessiondata/date,\
tracer_name,\
xnat:imagesessiondata/label,\
xnat:imageSessionData/dcmPatientId'


def match_pets(garjus, project, scans=None):
	df = pd.DataFrame()

	#	scans = garjus.scans(projects=[project])
	#	pets = scans[scans.MODALITY == 'PET']
	#		mris = scans[scans.MODALITY == 'MR']

	# Get all sessions
	uri = SESS_URI            
	uri += f'&project={project}'
	sessions = pd.DataFrame(garjus._get_result(uri))
	sessions = sessions.rename(columns={
	    'project': 'PROJECT',
	    'subject_label': 'SUBJECT',
	    'session_label': 'SESSION',
	    'session_type': 'SESSTYPE',
	    'xnat:imagesessiondata/date': 'DATE',
	    'xsiType': 'XSITYPE',
	    'dcmPatientId': 'VUIISID',
	    'tracer_name': 'TRACER',
	})
	sessions['MODALITY'] = sessions['XSITYPE'].map({
		'xnat:mrSessionData': 'MR',
		'xnat:petSessionData': 'PET'
	})

	# Get list of mri sessions including dcmpatientid
	mris = sessions[sessions.MODALITY == 'MR']
	mris = mris[['PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'DATE', 'VUIISID']].drop_duplicates()

	# Get List of pet sessions including dcmpatientid
	pets = sessions[sessions.MODALITY == 'PET']
	pets = pets[['PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'DATE', 'VUIISID', 'TRACER']].drop_duplicates()

	# Merge on matching subject to get row for every combination of pet with mri
	df = pd.merge(pets, mris, how='left', on=['PROJECT', 'SUBJECT'], suffixes=('_PET', '_MR'))

	# Group by pet and choose first mri 
	df = df.loc[df.groupby('SESSION_PET')['DATE_MR'].idxmin()]

	# TODO: compare to redcap entries???
	#rc = garjus.primary(project)

	return df
