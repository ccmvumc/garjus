Using garjus to upload NDA image03.

To use garjus command-line tools or to open a local dashboard, you will need to create credential files for REDCAp and XNAT. You will need your XNAT user name and password stored in a .netrc file in your home directory. For REDCap, you will just need the Project ID and API Key for the garjus project (also known as ccmutils) as well as each study project you want to access.

Configure XNAT credentials for garjus:
The .netrc file uses the same format as DAX. If you already have credentials for DAX, you can use the same for garjus. The format of each entry is:
machine value login value password value


Configure REDCap credentials for garjus:
Your REDCap keys must be stored in a file in your home directory named .redcap.txt. Each line has three values separated by comma. The first value is the Project ID, the second value is the key, and the third value is the  name. The name field is actually optionally but one entry should be named "main". This will be used as the main garjus REDCap project.

Test:
Test your credentials by running a garjus command such as garjus quicktest which will test all credentials. You can also open a dashboard in your browser with garjus dashboard. Then you can load and test a specific item of data. 


### nda
To upload image to NDA, you run two garjus commands to prepare the files to upload and then use the NIH tools to do the upload.

How to create the csv for the upload:
```
garjus image03csv -p PROJECT -s STARTDATE -e ENDDATE

```

How to download the images for the upload:
```
garjus image03download -p PROJECT CSVFILE DOWNLOADDIR
```


A Full example:
```
,:
cd /tmp
mkdir REMBRANDT_2022July
cd REMBRANDT_2022July
garjus image03csv -p REMBRANDT -s 2022-01-01 -e 2022-07-01 --site VUMC
garjus image03download REMBRANDT_image03.csv REMBRANDT_image03 -p REMBRANDT
DATASET=REMBRANDT_2022July && vtcmd /tmp/${DATASET}/REMBRANDT_image03.csv -b -l /tmp/${DATASET}/REMBRANDT_image03 -c 3413 -d ${DATASET} -t ${DATASET} -u bdboyd42
```

