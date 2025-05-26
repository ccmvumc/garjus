# Dashboard

The garjus dashboard provides a single point of access to data stored between XNAT and REDCap. The webapp can be run locally or on a server. It is built with the [dash](https://dash.plotly.com) python package and can run as a Flask app.

The main dashboard interface is a set of tabs that each contain a table. Most tabs can also display graphs.

When a user logs into the dashboard, the name and password are used to obtain a temporary access key from the XNAT API. This key is then used to query the XNAT API for the logged in users available projects. Those projects then become available for querying in the QA tab.

Each tab corresponds to a different view into the data in XNAT/REDCap.


## QA Dashboard

To install in a new python 3 environment:
```
python -m venv venv-garjus
```
Then load the new virtual environment with:
```
source venv-garjus/bin/activate
```
And then install garjus in the venv with:
```
pip install garjus
```

After successfully installing garjus, the quickest way to run the dashboard is using the login option:
```
garjus dashboard --login
```

This launches the dashboard server and opens a new browser window with the url of the login screen (http://localhost:8050). Here you use your XNAT credentials to log on.
Choose one or more projects from the drop down. The available options include all projects that are accessible to your XNAT account.


### Use DAX Credentials
The dashboard can use the same credentials file as dax. This is a .netrc file in your home directory with machine, login, and password in plain text. This file should only be readable by the owner.
```
machine xnat.vanderbilt.edu
login XNAT_USERNAME
password XNAT_PASSWORD
```

Then you can launch a dashboard that bypasses the login screen:
```
garjus dashboard
```



