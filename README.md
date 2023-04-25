# garjus

Garjus processes imaging data stored in REDCap and XNAT. All related settings are stored in REDCap. Each automation that runs is logged in REDCap. Any issues encountered are recorded in REDCap. Progress snapshots are stored in REDCap. Current views are in the dashboard.


The main Garjus class provides these data access methods that 
all return a Pandas DataFrame:
```
activity()

assessors()

automations()

issues()

processing()

progress()

scans()

stats(project)
```



To get the columns in each dataframe:
```
column_names(type)
e.g. 
column_names('issues')
or
column_names('scans')
```



These Garjus methods returns names in a list:
```
stattypes(project)

scantypes(project)

proctypes(project)

stats_assessors(project)

stats_projects()

```

garjus progress report - exports assessors, scans, stats for a project,
creates a summary PDF and zip of file of csv files per processing type. Can
be access from REDCap or dashboard.


garjus double entry comparison - compares primary REDCap to secondary REDCap
and uploads results to REDCap.



garjus dashboard - launches a dax dashboard and opens it in a new tab browser


garjus image03 - manages NDA image03 batches in a shared folder such as OneDrive



garjus can run automations including:

xnat_auto_archive - archives scans in XNAT

xnat_session_relabel - modifies labels in XNAT based on a set of rules to set the site and session type

xnat_scan_relabel - relabels scan type in XNAT using a simple map of input to output labels



edat automations

edat_convert - convert files, input is from redcap file field, outputs to redcap file field

edat_limbo2redcap - load files from a local folder

edat_redcap2xnat

edat_etl - extract data from files uploaded to redcap, transform (calculate accuracy, times), load to redcap

nihtoolbox_etl - extract and load

examiner_etl - extract and load

lifedata_etl

lifedata_box2redcap



issues - Any issues or errors encountered by garjus are recorded in REDCap.
Issues are automatically resolved when the error or issues is no longer found.
Resolved issues are deleted one week after resolution.



activity - Each complete automation is recorded in activity.



To create a new main REDCap project:
  - upload from zip
  - click user rights, enable API export/import, save changes
  - refresh, click API, click Generate API Token, click Copy
  - go to ~/.redcap.txt
  - paste key, copy & paste PID from gui, name it "main"


To create a new stats REDCap project:
  - Copy an existing project in gui under Other Functionality, click Copy Project
  - Change the project name
  - Confirm checkboxes for Users, Folder
  - Click Copy Project (should take you to new project)
  - In the new project, click user rights, check enable API export/import, click save changes
  - Refresh page, click API, click Generate API Token, click Copy
  - Go to ~/.redcap.txt
  - Paste key, copy & paste ID, name main
  - Paste ID into ccmutils under Main > Project Stats


To add a new primary REDCap project:
  - Copy PID, key to ~/.redcap.txt, name PROJECT primary
  - paste ID into ccmutils under Main > Project P


To add a new secondary REDCap project for double entry comparison:
  - Copy PID, key to ~/.redcap.txt, name PROJECT secondary 
  - paste ID into ccmutils under Main > Project Secondary


