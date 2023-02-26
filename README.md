# garjus

garjus is a helper for imaging projects stored in REDCap and XNAT. It provides
a single point of access and logs activity.

The main Garjus class provides these data access methods that 
all return a Pandas DataFrame:

activity()

assessors()

automations()

issues()

processing()

progress()

scans()

stats(project)



To get the columns in each dataframe:

column_names(type), e.g. column_names('issues') or column_names('scans')




These Garjus methods returns names in a list:

stattypes(project)

scantypes(project)

proctypes(project)

stats_assessors(project)

stats_projects()



garjus progress report - exports assessors, scans, stats for a project,
creates a summary PDF and zip of file of csv files per processing type. Can
be access from REDCap or dashboard.



garjus double entry comparison - compares primary REDCap to secondary REDCap
and uploads results to REDCap.



garjus dashboard - launches a dax dashboard and opens it in a new tab browser



garjus image03 - manages NDA image03 batches in a shared folder such as OneDrive




ToBeCompleted:garjus can run automations including:

xnat_auto_archive - archives scans in XNAT

xnat_session_relabel - modifies labels in XNAT based on a set of rules to set the site and session type

xnat_scan_relabel - relabels scan type in XNAT using a simple map of input to output labels



ToBeCompleted:edat automations

edat_convert - convert files, input is from redcap file field, outputs to redcap file field

edat_limbo2redcap - load files from a local folder

edat_redcap2xnat

edat_etl - extract data from files uploaded to redcap, transform (calculate accuracy, times), load to redcap

nihtoolbox_etl - extract and load

examiner_etl - extract and load

lifedata_etl

lifedata_box2redcap



ToBeCompleted:issues



ToBeCompleted:activity



