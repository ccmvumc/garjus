# Automations

Garjus runs various automations to process data in XNAT and REDCap. These are run when a scheduled garjus update runs. The automations help synchronize data between the systems, apply curation rules, end perform data ETL.


### Scanning Automations:

  - xnat\_auto\_archive - archives scans in XNAT

  - xnat\_session\_relabel - modifies labels in XNAT based on a set of rules to set the site and session type

  - xnat\_scan\_relabel - relabels scan type in XNAT using a simple map of input to output labels


### EDAT Automations:

  - edat_convert - convert files, input is from redcap file field, outputs to redcap file field

  - edat_limbo2redcap - load files from a local folder

  - edat_redcap2xnat - copy edat files from REDCap to XNAT

  - edat_etl - extract data from files uploaded to redcap, transform (calculate accuracy, times), load to redcap

### Other ETL:

  - nihtoolbox_etl - extract and load NIH toolbox outputs

  - examiner_etl - extract and load NIH Examiner outputs

  - gaitrite_etl - extract and load gaitrite walkway outputs
