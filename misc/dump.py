import sys

import pydicom


if __name__ == '__main__':
    dicom_path = sys.argv[1]
    print(pydicom.dcmread(dicom_path))
