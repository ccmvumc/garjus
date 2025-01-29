# NDA image upload with garjus

Garjus can export XNAT/REDCap data in preparation for upload the NDA repository. Appropriate credentials are required including a REDCap key for each study project.

To upload image to NDA, run two garjus commands to prepare the files to upload and then use the NIH tools to do the upload.

The csv follows the NIH [image03 template](https://nda.nih.gov/api/datadictionary/v2/datastructure/image03/template). The first line of the file must be "image03", the second line contains the column headers, and then subsequent rows are data. Images are exported in DICOM format with a single zip file per scan. 

As required for DICOM format, these are the columns completed in the csv:

```
subjectkey, src_subject_id, interview_date, interview_age, sex, image_file, image_description, scan_type, scan_object, image_file_format, image_modality, transformation_performed
```

For functional task scans, the ```experiment_id``` column is also completed.

For diffusion scans, the ```bvek_bval_files``` column is set to Yes.

To complete the experiment type and scan type fields, garjus pulls from the main REDCap to map XNAT scan types to image03 experiment types and data types. See [setup](docs/setup.md) for more.

In addition to garjus, you will need to install the NDA python package [nda-tools](https://github.com/NDAR/nda-tools).

```
pip install nih-tools
```

### upload

To create the csv for the upload (dates are inclusive):

```
garjus image03csv -p PROJECT -s STARTDATE -e ENDDATE
```

This will export a csv with one row per scan. Note this not per session, you will have multiple rows per session, for e.g. a row for T1, fMRI and DTI scans.

To download the images for the upload:

```
garjus image03download -p PROJECT CSVFILE DOWNLOADDIR
```

Now we have exported the csv and DICOM.zip files locally. The final step is to fun upload with the nda-tools command:

```
vtcmd CSVFILE -b -l DOWNLOADDIR -c COLLECTION -d DATASET -t DATASET -u USER
```

### Common Problems
You must set your credentials for NDA with keyring in python with:

```
import keyring
keyring(system,name,password)
```

Also, your keychain must be unlocked from command-line before running vtcmd, with:

```
security unlock-keychain
```



## Full example:

```
cd /tmp
DATASET=REMBRANDT_2022July
mkdir $DATASET
cd $DATASET
garjus image03csv -p REMBRANDT -s 2022-01-01 -e 2022-06-30 --site VUMC
garjus image03download image03.csv image03 -p REMBRANDT
vtcmd /tmp/${DATASET}/image03.csv -b -l /tmp/${DATASET}/image03 -c 3413 -d ${DATASET} -t ${DATASET} -u bdboyd42
```

