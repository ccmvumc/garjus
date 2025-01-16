# Dashboard

The garjus dashboard provides a single point of access to data stored between XNAT and REDCap. The webapp can be run locally or on a server. It is built with the [dash](https://dash.plotly.com) python package and can run as a Flask app.

The main dashboard interface is a set of tabs that each contain a table. Some tabs also can show graphs.

When a user logs into the dashboard, the name and password are used to obtain a temporary access key from the XNAT API. The key is then used to query the XNAT API for the logged in users available projects. Those projects are in then available for querying in the QA tab.

Each tab corresponds to a different view into the data in XNAT/REDCap.


## QA

The QA table displays scans and assesors queried from XNAT. The table can also can be viewed by session, subject, or project. 

Demographics can optionally be included in the view. These are pulled from REDCap and must be configured per project.

## Analyses

The Analyses table is a direct view of the Analyses instrument in the DAX REDCap. 

## Processors


## Stats


## Reports



## Issues

## Activity


## QA Dashboard without REDCap, only XNAT

If you have data in XNAT but no REDCap, you can still use the dashboard. The only tab available will be the QC tab. All other tabs use data from REDCap.

The garjus QA dashboard can be used with only XNAT access. First, you'll need credentials in
your home directory. The same as dax, you need a .netrc file in your home directory with machine, login, and password in plain text. This file should only be readable by the owner.
```
machine xnat.vanderbilt.edu
login XNAT_USERNAME
password XNAT_PASSWORD
```
Then install garjus and launch the dashboard. To install in a new python 3 environment:
```
python -m venv venv-garjus
```
Then load the new virtual environment with:
```
source venv-garjus/bin/activate
```
Always good to upgrade pip:
```
pip install pip -U
```
And then install garjus in the venv with:
```
pip install garjus
```
If you encounter an error with scikit learn, you can bypass it with:
```
export SKLEARN_ALLOW_DEPRECATED_SKLEARN_PACKAGE_INSTALL=True && pip install garjus
```
After garjus is successfully installed, you can launch a dashboard with:
```
garjus dashboard
```

This should open a new tab in your web browser at the dashboard main page http://localhost:8050 .
Choose one or more projects from the drop down. The options should include all projects that are accessible to your XNAT account using the credentials in your .netrc file.

