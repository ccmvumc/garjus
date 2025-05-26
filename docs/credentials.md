# Credentials
To use garjus command-line tools or to open a local dashboard, you will need to create credential files for both REDCap and XNAT. Your XNAT user name and password are stored in a .netrc file in your home directory. For REDCap, you need both the Project ID and API Key each project you want to access. This includes the main garjus project, the dax rcq project as well as each study-specific project.


### Configure XNAT credentials for garjus
The .netrc file uses the same format as DAX. If you already have credentials for DAX, you can use the same for garjus. The format of each entry is:
```
machine value
login value
password value
```

### Configure REDCap credentials for garjus
REDCap credentials are stored in a text file in your home directory named .redcap.txt. Each line has three values separated by comma. The first value is the Project ID, the second value is the API key, and the third value is the name. Use "main" in the name field for the main garjus project and "rcq" for the dax rcq project.
```
999999,XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX,main
888888,XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX,rcq
777777,XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX,Project1
666666,XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX,Project2
555555,XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX,Project3
```

### Test your credentials
Test your credentials by running a garjus command such as garjus quicktest which will test all credentials. You can also open a dashboard in your browser with garjus dashboard where you can select a project to confirm access.
```
garjus quicktest
```
