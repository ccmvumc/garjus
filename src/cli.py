import click
import redcap
import pyxnat
from .project import Project
from .automation import Automation

@click.group()
def cli():
    pass

@click.command()
#@click.option("--redcap-url", required=True)
#@click.option("--redcap-token", required=True)
#@click.option("--xnat-url", required=True)
#@click.option("--xnat-username", required=True)
#@click.option("--xnat-password", required=True)
#def run_automation(redcap_url, redcap_token, xnat_url, xnat_username, xnat_password):
#    redcap_project = redcap.Project(redcap_url, redcap_token)
#    xnat_interface = pyxnat.Interface(xnat_url, xnat_username, xnat_password)
#    project = Project(redcap_project, xnat_interface)
#    automation = Automation(project)
#    automation.run()

#cli.add_command(run_automation)
