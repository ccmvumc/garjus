# garjus

Garjus manages neuroimaging research projects stored in REDCap and XNAT. Integration with [DAX](https://github.com/VUIIS/dax) allows subject-level [processing](docs/processing.md) as well as group-level multi-project [analyses](docs/analyses.md). All required [settings](docs/settings.md) are stored in REDCap. [Activity](docs/activity.md) is logged in REDCap. Any [issues](docs/issues.md) encountered are recorded in REDCap. [Progress](docs/progress.md) reports and snapshots are stored in REDCap.

If you have data in both XNAT and REDCAp, garjus will likely be useful. For maximum utility, data will be curated to include consistent session types and scan types. Garjus provides tools to help automate this curation process. Typically, a garjus update will be run on an automated scheduled process with cron. An update usually takes a few minutes unless there are new image sessions to import.

Garjus maintains a [stats](docs/stats.md) database of imaging measurements for quick-access and dynamic export.

Other tools include double data entry with [compare](docs/compare.md) and [image03](docs/nda.md) that handles exporting and uploading to the NDA repository. 

All can be utilized in python scripts or via the [command-line interface](docs/cli.md). A current view can be had in the [dashboard](docs/dashboard.md) which provides a single point of access to data in XNAT/REDCap.

At CCMVUMC, garjus is used to automate EEG/MRI/PET image processing as well as management of related data including Eprime, gaitrite, NIH Toolbox, NIH Examiner, Fitbit. Automations are initially triggered when a user enters the scan identifier from the scanner and continue all the way through to analysis.


## Quickstart

If you have data in REDCap and XNAT, but have not configured them for garjus follow the steps in [setup](docs/setup.md). 

To use an existing garjus system, you will simply need to configure your [credentials](docs/credentials.md).

The latest release can be installed from PYPI.

```
pip install garjus
```

## using garjus in python

```
from garjus import Garjus

g = Garjus()
```

The main Garjus class provides data access methods that 
all return a Pandas DataFrame.

```
g.activity()
g.analyses()
g.assessors()
g.automations()
g.issues()
g.phantoms()
g.processing_protocols()
g.progress()
g.scans()
g.subjects()
g.subject_assessors()
g.stats(project)
g.tasks()
```


To get the columns in each dataframe:

```
g.column_names('issues')
g.column_names('scans')
```


These Garjus methods returns names in a list:

```
g.stattypes(project)
g.scantypes(project)
g.proctypes(project)
g.stats_assessors(project)
g.stats_projects()
```

## Command-line interface subcommands
The garjus command-line interface provides mulitple subcommands each with specific options/arguments.

* activity - display activity
* analyses - display analyses
* compare - data entry comparison
* copyscan - copy an imaging scan from one session to another
* copysess - copy an imaging session from one project to another
* dashboard - start a dashboard server and browse to it in a new local web browser tab
* delete - delete a proctype from a project
* download - download files from XNAT assessors
* export - extract stats to csv files
* image03csv - create an NDA image03 formatted csv file for a project and date range
* image03download - download all images for an NDA image03 csv file
* importdicom - import DICOM into XNAT from local file or remote URL such as gstudy
* importnifti - import NIFTI into XNAT from local file
* issues - display current issues
* orphans - find and display assessors without parents
* processing - display current processing
* progress - display list of progress reports
* quicktest - test connections
* report - create a summary PDF of a project
* retry - find jobs that have run once an run them again
* run - run a dax analysis locally
* setsesstype - set the SessionType field of a session
* setsite - set the Site field of a session
* stats - export stats to csv
* statshot - export stats to csv files and save output as new analysis
* subjects - display subjects of a project
* switch - batch change status of assessors
* tasks - show currently running task jobs
* update - run automations, update caches, check for issues
---

Find a problem? Report an issue. Got an idea? Open a discussion. Thanks!
