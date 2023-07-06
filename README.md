# garjus

Garjus processes imaging data stored in REDCap and XNAT. All related settings are stored in REDCap. Each automation that runs is logged in REDCap. Any issues encountered are recorded in REDCap. Progress snapshots are stored in REDCap. Current views are in the dashboard.


Garjus is the interface to everything that's stored in XNAT/REDCap. It uses
REDCap to store it's own settings and tracking data. Anytime we want to
access these REDCap or XNAT in python or CLI, we use Garjus in between.
Creating a Garjus instance means setting up the interfaces with XNAT/REDCap.


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

Command-line interface:

```
garjus report - creates a summary PDF

garjus dashboard - launches a dax dashboard and opens it in a new tab browser

garjus image03 - manages NDA image03 batches in a shared folder such as OneDrive

```

### Scanning Automations:

  - xnat\_auto\_archive - archives scans in XNAT

  - xnat\_session\_relabel - modifies labels in XNAT based on a set of rules to set the site and session type

  - xnat\_scan\_relabel - relabels scan type in XNAT using a simple map of input to output labels


### EDAT Automations:

  - edat_convert - convert files, input is from redcap file field, outputs to redcap file field

  - edat_limbo2redcap - load files from a local folder

  - edat_redcap2xnat - copy edat files from REDCap to XNAT

  - edat_etl - extract data from files uploaded to redcap, transform (calculate accuracy, times), load to redcap

  - nihtoolbox_etl - extract and load NIH toolbox outputs

  - examiner_etl - extract and load NIH Examiner outputs

### Issues
Any issues or errors encountered by garjus are recorded in REDCap.
Issues are automatically resolved when the error or issues is no longer found.
Resolved issues are deleted one week after resolution.

### Activity
Each complete automation is recorded in activity.



## Set up

Create a new Garjus main REDCap project:

  - upload from zip (see misc folder)
  - click user rights, enable API export/import, save changes
  - refresh, click API, click Generate API Token, click Copy
  - go to ~/.redcap.txt
  - paste key, copy & paste PID from gui, name it "main"

Create first stats REDCap project:

  - upload from zip (see misc folder)
  - click user rights, check enable API export/import, click save changes
  - Refresh page, click API, click Generate API Token, click Copy
  - Go to ~/.redcap.txt
  - Paste key, copy & paste ID, name
  - Paste ID into ccmutils under Main > Project Stats

Create additional stats REDCap projects:

  - Copy an existing project in gui under Other Functionality, click Copy Project
  - Change the project name
  - Confirm checkboxes for Users, Folder
  - Click Copy Project (should take you to new project)
  - In the new project, click user rights, check enable API export/import, click save changes
  - Refresh page, click API, click Generate API Token, click Copy
  - Go to ~/.redcap.txt
  - Paste key, copy & paste ID, name main
  - Paste ID into ccmutils under Main > Project Stats


Add a new primary REDCap project to link individual study to garjus:
  
  - Copy PID, key to ~/.redcap.txt, name PROJECT primary
  - paste ID into ccmutils under Main > Project Primary


Add a new secondary REDCap project for double entry comparison:
  
  - Copy PID, key to ~/.redcap.txt, name PROJECT secondary 
  - paste ID into ccmutils under Main > Project Secondary

## Quickstart

pip install git+https://github.com/bud42/garjus.git

or

pip install git+https://github.com/bud42/garjus.git@v1.0.0

