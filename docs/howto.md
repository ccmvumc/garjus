Using garjus

To use garjus command-line tools or to open a local dashboard, you will need to create credential files for REDCAp and XNAT. You will need your XNAT user name and password stored in a .netrc file in your home directory. For REDCap, you will just need the Project ID and API Key for the garjus project (also known as ccmutils) as well as each study project you want to access. You will also need separate keys for each stats project you want to access.

Configure XNAT credentials for garjus:
The .netrc file uses the same format as DAX. If you already have credentials for DAX, you can use the same for garjus. The format of each entry is:
machine value login value password value


Configure REDCap credentials for garjus:
Your REDCap keys must be stored in a file in your home directory named .redcap.txt. Each line has three values separated by comma. The first value is the Project ID, the second value is the key, and the third value is the  name. The name field is actually optionally but one entry should be named "main". This will be used as the main garjus REDCap project.

Test:
Test your credentials by running a garjus command such as garjus quicktest which will test all credentials. You can also open a dashboard in your browser with garjus dashboard. Then you can load and test a specific item of data. 


How to get stats data with garjus:
*need access for the study project in REDCap, garjus project in redcap and study project in XNAT.
*open dashboard from command-line run: garjus dashboard to load and view stats
*export from dashboard or use command-line to get stats .csv

### tasks
How to update tasks (also know as build new jobs):
```
garjus update tasks -p PROJECT
```


### analyses
How to download analysis inputs:
```
garjus getinputs ID -p PROJECT
```

How to run an analysis:
```
garjus run ANALYSESID out.zip -p PROJECT
```

For example, to run analyses with ID 2 for project REMBRANDT:
```
garjus run 2 out.zip -p REMBRANDT
```

### copysess
How to copy a session from one project to another:
```
garjus copysess PROJECT1/SUBJECT1/SESSION1 PROJECT2/SUBJECT2/SESSION2
```


### stats
How to extracts stats with one row per subject:
```
garjus stats -p PROJECT -t PROCTYPE -s SESSTYPE OUTPUTFILENAME.csv -a PROJECT_ANALYSESID --persubject
```

For example, to extract the LST stats for the Baseline sessions for only the subjects in Analyses ID 1. And save the csv with one row per subject.
```
garjus stats -p REMBRANDT -t LST_v1 -s Baseline Baseline145_LST_v1.csv -a REMBRANDT_1 --persubject
```


### importdicom


### setsesstype
How to set a the session type of a single session:
```
garjus setsesstype PROJECT/SUBJECT/SESSION SESSTYPE
```
For example, to the session type to Baseline:
garjus setsesstype PROJECT1/SUBJECT1/SESSION1 Baseline


### setsite
How to set a the session type of a single session:
```
garjus setsite PROJECT/SUBJECT/SESSION SITE
```
For example, to the site to VUMC:
garjus setsite PROJECT1/SUBJECT1/SESSION1 VUMC


### retry
How to retry tasks where the job failed on first run:
```
garjus retry -p PROJECT
```

Running retry will set the FAILCOUNT for a task to 1, so if a job fails again, retry will ignore it and not run again.


### delete
How to delete an entire processing type from a project:
```
garjus delete -t PROCTYPE -p PROJECT
```

For example, to delete all instances of assessors with proctype LST_v1 from project REMBRANDT:
```
garjus delete -t LST_v1 -p REMBRANDT
```
Please don't run this unless you're serious.

### --debug
How to turn on debugging with the debug flag:
```
garjus --debug update
```
