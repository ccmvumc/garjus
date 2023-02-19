from typing import Optional
from redcap import Project
from pynat import Interface

class Garjus:
    """
    Handles data in xnat and redcap

    Parameters:
        redcap_project (redcap.Project): A REDCap project instance.
        xnat_interface (pyxnat.Interface): A PyXNAT interface.

    Attributes:
        redcap_project (redcap.Project): The REDCap project instance.
        xnat_interface (pyxnat.Interface): The PyXNAT interface.
    """

    def __init__(self, redcap_project: Project, xnat_interface: Interface):
        self.main_redcap = redcap_project
        self.xnat = xnat_interface

    def __init__(self, redcap_project, xnat_interface):

        self.redcap = redcap_project
        
        self.xnat = xnat_interface

    def scans(self, projects=None, scantypes=None, modalities='MR'):
        """Query XNAT for all scans and return a dictionary of scan info."""
        # Code to query XNAT for scans

    def assessors(self, projects=None, proctypes=None, modalities='MR'):
        """Query XNAT for all assessors of type proc:genprocdata and return a dictionary with info."""
        # Code to query XNAT for assessors

    def get_redcap_field(self, record_id: int, field_name: str) -> Optional[str]:
        record = self.redcap_project.export_records(records=[record_id], fields=[field_name])
        value = record[0][field_name]
        return value

    def get_xnat_image(self, project_id: str, subject_id: str, session_id: str, experiment_id: str) -> Optional[bytes]:
        image = self.xnat_interface.get_image(project_id, subject_id, session_id, experiment_id)
        return image
