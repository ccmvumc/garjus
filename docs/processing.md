# Processing Pipelines

We use the session type and scan type to determine which data are used as pipeline inputs. The session types are typically Baseline and then some timepoints such as Month24 or Week8. Scan types should be succinct such as T1, FLAIR, fMRI, DTI.

### Configure a pipeline on a project
Pipelines are configured per XNAT project in the REDCap Processing form. For each entry, an existing pipeline is selected from a list or set up as a custom pipeline. 


### Testing a new pipeline
If you there's any chance the pipeline will fail,  use filters to only run 1 or a few sessions. When your tests run, then you a remove the filter to run on the remaining.

For session-level pipelines, the filter applies to the session label to run on a subset of sessions. The filter can be a single item to match or multiple comma-separated. Wildcards are supported, so for example to run all Baseline scans for a project where Baseline labels end with "a", we'd set the filter to "*a".

