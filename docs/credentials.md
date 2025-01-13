# credentials
To use garjus command-line tools or to open a local dashboard, you will need to create credential files for both REDCap and XNAT. Your XNAT user name and password must be stored in a .netrc file in your home directory. For REDCap, you will need the Project ID and API Key for the garjus project as well as each study project you want to access.


### Configure XNAT credentials for garjus
The .netrc file uses the same format as DAX. If you already have credentials for DAX, you can use the same for garjus. The format of each entry is:
machine value login value password value


### Configure REDCap credentials for garjus
Your REDCap keys must be stored in a file in your home directory named .redcap.txt. Each line has three values separated by comma. The first value is the Project ID, the second value is the key, and the third value is the name. The name field is actually optional, but one entry should be named "main". This will be used as the main garjus REDCap project.

### Test your credentials
Test your credentials by running a garjus command such as garjus quicktest which will test all credentials. You can also open a dashboard in your browser with garjus dashboard where you can select a project to confirm access.
