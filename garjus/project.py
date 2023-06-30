class Project:
     """
    Represents a data project and provides methods to access and manipulate project data.

    Parameters:
        name (str): name of project.
        garjus (garjus.Gargus): A garjus object.

    Attributes:
    """

    def __init__(self, name, garjus):
        self.name = name
        self.garjus = garjus


    def scans(self, scantypes=None):
        return garjus.scans(projects=[self.name], scantypes=scantypes)


    def assessors(self, proctypes=None):
        return garjus.assessors(projects=[self.name], proctypes=proctypes)
