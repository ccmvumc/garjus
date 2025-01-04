### NDA image upload with garjus


### credentials
To use garjus command-line tools or to open a local dashboard, you will need to create credential files for both REDCap and XNAT. Your XNAT user name and password must be stored in a .netrc file in your home directory. For REDCap, you will need the Project ID and API Key for the garjus project (also known as ccmutils) as well as each study project you want to access.


Configure XNAT credentials for garjus:
The .netrc file uses the same format as DAX. If you already have credentials for DAX, you can use the same for garjus. The format of each entry is:
machine value login value password value


Configure REDCap credentials for garjus:
Your REDCap keys must be stored in a file in your home directory named .redcap.txt. Each line has three values separated by comma. The first value is the Project ID, the second value is the key, and the third value is the name. The name field is actually optional, but one entry should be named "main". This will be used as the main garjus REDCap project.

Test:
Test your credentials by running a garjus command such as garjus quicktest which will test all credentials. You can also open a dashboard in your browser with garjus dashboard where you can select a project to confirm access.


### upload
To upload image to NDA, run two garjus commands to prepare the files to upload and then use the NIH tools to do the upload.

To create the csv for the upload (dates are inclusive):

```
garjus image03csv -p PROJECT -s STARTDATE -e ENDDATE

```

To download the images for the upload:

```
garjus image03download -p PROJECT CSVFILE DOWNLOADDIR
```

And finally upload to NDA:

```
vtcmd CSVFILE -b -l DOWNLOADDIR -c COLLECTION -d DATASET -t DATASET -u USER
```



A Full example:

```
cd /tmp
DATASET=REMBRANDT_2022July
mkdir $DATASET
cd $DATASET
garjus image03csv -p REMBRANDT -s 2022-01-01 -e 2022-06-30 --site VUMC
garjus image03download image03.csv image03 -p REMBRANDT
vtcmd /tmp/${DATASET}/image03.csv -b -l /tmp/${DATASET}/image03 -c 3413 -d ${DATASET} -t ${DATASET} -u bdboyd42
```



