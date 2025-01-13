# Dashboard
The dashboard is built with the dash package.

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

