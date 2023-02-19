class Project:
     """
    Represents a data project and provides methods to access and manipulate project data.

    Parameters:
        name (str): name of project.
        garjus (garjus.Gargus): A garjus object.

    Attributes:
        redcap_project (redcap.Project): The REDCap project instance.
        xnat_interface (pyxnat.Interface): The PyXNAT interface.
    """

    def __init__(self, name, garjus):
        self.name = name
        self.garjus = garjus
        self.redcap_project = redcap_project
        self.xnat_interface = xnat_interface


    def scans(self, scantypes=None):
        return garjus.scans(projects=[self.name], scantypes=scantypes)


    def assessors(self, proctypes=None):
        return garjus.assessors(projects=[self.name], proctypes=proctypes)

    #def get_image(self, image_id):
    #    image = self.xnat.select('/archive/projects', self.project_id, 'subjects', image_id.subject, 'experiments', image_id.experiment, 'scans', image_id.scan, 'resources', image_id.resource, 'files', image_id.filename)
    #    return image.get()

    #def get_images(self):
    #    images = []
    #    project_images = self.redcap.export_records(records=[f"{self.project_id}_images"], format='json')
    #    if project_images:
    #        image_list = project_images[0].get('image_list')
    #        if image_list:
    #            for image_id in image_list.split(','):
    #                images.append(self.get_image(ImageId.from_string(image_id)))
    #    return images
