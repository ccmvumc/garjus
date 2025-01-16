# Automations

Garjus runs various automations to process data in XNAT and REDCap. These are run when a scheduled garjus update runs and should finish quickly when nothing has changed.. The automations help synchronize data between the systems, apply curation rules, end perform data extract/transform/load. 


### Scanning Automations:

The scanning automations get image sessions into XNAT and apply rules to relabel them.

  - xnat\_auto\_archive - archives scans in XNAT by matching new scans to REDCap entries

  - xnat\_session\_relabel - modifies labels in XNAT based on a set of rules to set the site and session type

  - xnat\_scan\_relabel - relabels scan type in XNAT using a simple map of input to output labels


### EDAT Automations:

The EDAT automations take each Eprime output file from a shared folder, convert it and upload the text file to a scan on XNAT. 


  - edat_convert - convert files, input is from redcap file field, outputs to redcap file field

  - edat_limbo2redcap - load files from a local folder to REDCap

  - edat_redcap2xnat - copy edat files from file fields in REDCap to EDAT resource on corresponding scan

  - edat_etl - extract data from files uploaded to redcap, transform (calculate accuracy, times), load to REDCap

### Other ETL:

  - nihtoolbox_etl - extract and load NIH toolbox outputs

  - examiner_etl - extract and load NIH Examiner outputs

  - gaitrite_etl - extract and load gaitrite walkway outputs

  
  
### Best Practices
  - Automations that operate on REDCap  check a field to determine if that record has already been processed
  - Store data in XNAT sessions with scan types and session types named consistently using short labels
  - All session labels based on subject label with suffix that indicates session type, e.g.
SUBJECT=9999, SESSION=9999a
SUBJECT=ABCD1, SESSION=ABCD1_m06
  - All subject and session labels unique across the project (XNAT requires this policy)
  - Complete Session Type (usually blank or not useful by default), e.g.:
Baseline, Month6, Week12, Year2
  - Set scan types to succinct names and keep consistent across projects, e.g.:
T1, T2, FLAIR, DTI, fMRI_Resting, fMRI_REST1, fMRI_REST2
  - Use automations for relabling
