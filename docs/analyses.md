# analyses

A DAX analysis is configured with a yaml file where we select inputs, specify scripts to run and containers to use for running the scripts.

garjus can run a dax analyses locally. This is helpful for testing your analysis before making it a dax analysis. 
You must specify a project, one or more subjects, the local code repository, and the output directory.

```
garjus run -p REMBRANDT  -r ~/git/ccmvumc-analyses -d ~/TEST-OUTPUT
```

This will use the default processor in the repository. You can also specify a local file as the processor .yaml.

```
garjus run -p REMBRANDT  -r ~/git/ccmvumc-analyses -d ~/TEST-OUTPUT  -y ~/git/ccmvumc-analyses/processors/REMBRANDT-A017/processor.yaml
```


You can limit included subjects which is useful for quicker testing:

```
 garjus run -p REMBRANDT  -r ~/git/ccmvumc-analyses -d ~/TEST-OUTPUT  -s 12345,6789,09876
```


To include a local covariates csv file

```
garjus run -p REMBRANDT  -r ~/git/ccmvumc-analyses -d ~/TEST-OUTPUT  -c covariates.csv
```


## Running with dax

To run an analysis in dax, you create an entry in the analyses form for the primary project. The records are autonumbered. When you're ready to let DAX run the analysis, set the status to QUEUED or Q.



## dashboard

Analyses can be viewed in the garjus dashboard. The analyses tab loads directly from the dax REDCap project.
Click pdf icon to view the report pdf
Cick edit to open the record in REDCap.
Click the output link to view the output data on XNAT.


To hide any records in the dashboard, set the dashboard value to False in the redcap record.
