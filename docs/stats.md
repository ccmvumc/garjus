# stats

Garjus allows quick access to imaging measurements stored in XNAT assessors by maintaing a cache in REDCap. We refer to these measurements as "stats" as they are stored in an XNAT assessor resource named STATS.


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
garjus stats -p REMBRANDT -t FS7_v1 out.csv  -s Baseline

```


To filter by the subjects from an previous analysis:

```
garjus stats -p REMBRANDT -t FS7_v1 out.csv  -s Baseline -a 17

```

This will read the list of subjects from the analysis record in REDCap.


To limit to specific sessions:

```
garjus stats -p REMBRANDT -t FS7_v1 out.csv  -s Baseline -e 12345a
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
