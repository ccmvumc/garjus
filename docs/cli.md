# Command-line interface

The garjus command-line interface is implemented with the click python package. Like docker or other similar packages, garjus uses sub-commands. Each sub-command has it's own set of options and arguments. 

```
Usage: garjus [OPTIONS] COMMAND [ARGS]...

Options:
  --debug / --no-debug
  --quiet / --no-quiet
  --help                Show this message and exit.

Commands:
  activity
  analyses
  compare
  copyscan
  copysess
  dashboard
  delete
  download
  export
  finish
  getinputs
  getoutputs
  image03csv
  image03download
  importdicom
  importnifti
  issues
  orphans
  pdf
  processing
  progress
  quicktest
  report
  retry
  run
  setsesstype
  setsite
  stats
  statshot
  subjects
  switch
  tasks
  update
```

You can get help for each subcommand.
```
garjus stats --help
Usage: garjus stats [OPTIONS] CSV

Options:
  -p, --projects TEXT   [required]
  -t, --types TEXT
  -s, --sesstypes TEXT
  -a, --analysis TEXT
  --persubject
  -e, --sessions TEXT
  --help                Show this message and exit.
...
```

Use garjus delete to remove assessors from XNAT by project and processing type with optional filters for statuses.

```
garjus delete --help
Usage: garjus delete [OPTIONS]

Options:
  -p, --project TEXT     [required]
  -t, --type TEXT        [required]
  -s, --procstatus TEXT
  -q, --qcstatus TEXT
  --help                 Show this message and exit.
```

This method of deleting is often useful when testing a pipeline and want to quickly purge any test assessor data for the new type.

At times, you will want to apply some or all updates immediately. For example, automations can be run manually to extract a recently uploaded file.

```
garjus update automations -p REMBRANDT -t etl_nihtoolbox
```

stats can be synchronized manually to copy stats from XNAT to REDCap so it's visible in the dashboard.

```
garjus update stats -p REMBRANDT
```

