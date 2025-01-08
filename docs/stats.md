# stats


Garjus allows quick access to imaging measurements stored in XNAT assessors by maintaing a cache in REDCap. We refer to these measurements as "stats" as they are stored in an XNAT assessor resource named STATS.


## credentials

To use garjus command-line tools, you will need to create credential files for both REDCap and XNAT. Your XNAT user name and password must be stored in a .netrc file in your home directory. For REDCap, you will need the Project ID and API Key for the garjus project (also known as ccmutils) as well as each study project you want to access.

Configure XNAT credentials for garjus: The .netrc file uses the same format as DAX. If you already have credentials for DAX, you can use the same for garjus. The format of each entry is: machine value login value password value

Configure REDCap credentials for garjus: Your REDCap keys must be stored in a file in your home directory named .redcap.txt. Each line has three values separated by comma. The first value is the Project ID, the second value is the key, and the third value is the name. The name field is actually optional, but one entry should be named "main". This will be used as the main garjus REDCap project.

Test your credentials by running garjus quicktest.



## Updating stats cache

The cache is updated when your scheduled garjus update runs, but can also be run manually with: 

```
garjus update stats
```


To limit updates by project:

```
garjus update stats -p REMBRANDT
```


To also by limit by processing assessor type, for example FreeSurfer:

```
garjus update stats -p REMBRANDT -t FS7_v1
```



Any updates will immediately appear in the dashboard by clicking Refresh.



## Exporting stats to csv

We can use the dashboard to export a csv or use the command-line. You must specify an output filename and one or more projects.

```
garjus stats -p REMBRANDT out.csv
```


To also by limit by processing assessor type, for example FreeSurfer:

```
garjus stats -p REMBRANDT -t FS7_v1 out.csv
```


To limit by session type, for example to only include baseline scans:

```
garjus stats -p REMBRANDT -t FS7_v1 out.csv -s Baseline
```


To filter by the subjects from an previous analysis:

```
garjus stats -p REMBRANDT -t FS7_v1 out.csv -s Baseline -a 17
```

This will read the list of subjects from the analysis record in REDCap.


To limit to specific sessions:

```
garjus stats -p REMBRANDT -t FS7_v1 out.csv -s Baseline -e 12345a
```



## Exporting stats as an analysis

Use statshot to store exported csv files as a new analysis. A csv file will be uploaded per processing type across all projects. By default, the output will also include a csv with subject demographics and a report PDF.

```
garjus statshot -p REMBRANDT,D3,DepMIND2,NewhouseMDDHx,R21Perfusion,TAYLOR_CAARE --t FS7_v1,Multi_Atlas_v3
```


To filter the included subjects using an existing analysis:

```
garjus statshot -p REMBRANDT,DepMIND2,NewhouseMDDHx,D3,COGD,TAYLOR_CAARE,R21Perfusion -t FS7_v1,Multi_Atlas_v3 -a REMBRANDT_17 -s Baseline
```



## Adding a new project

A separate stats REDCap project is required for each study project. All stats REDCap share a common set of fields. New projects can be added by copying an existing REDCap project. 
