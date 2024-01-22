import logging
import os

from garjus.compare import make_double_report
from garjus import Garjus


logger = logging.getLogger('test_compare_arousal')

PROJECT = 'REMBRANDT'

FIELDS = [
    'arousal_pre_1calm',
    'arousal_pre_2control',
    'arousal_pre_3happy',
    'arousal_post_1calm',
    'arousal_post_2control',
    'arousal_post_3happy',
    'msit_dia1',
    'msit_dia2',
    'msit_dia3',
    'msit_dia4',
    'msit_pulse1',
    'msit_pulse2',
    'msit_pulse3',
    'msit_pulse4',
    'msit_start',
    'msit_stop',
    'msit_sys1',
    'msit_sys2',
    'msit_sys3',
    'msit_sys4',
    't1_dia1',
    't1_dia2',
    't1_dia3',
    't1_pulse1',
    't1_pulse2',
    't1_pulse3',
    't1_sys1',
    't1_sys2',
    't1_sys3',
]

EVENTS = [
    'baselinemonth_0_arm_2',
    'baselinemonth_0_arm_3',
    'month_8_arm_2',
    'month_8_arm_3'
]


def compare(g, project, fields, events):
    """Create a PDF report of Double Entry Comparison."""
    pdf_file = f'{project}_double.pdf'
    excel_file = f'{project}_double.xlsx'

    if os.path.exists(pdf_file):
        logger.info(f'{pdf_file} exists, delete or rename.')
        return

    if os.path.exists(excel_file):
        logger.info(f'{excel_file} exists, delete or rename.')
        return

    logger.info(f'writing report to file:{pdf_file},{excel_file}.')
    # Get the projects to compare
    proj_primary = g.primary(project)
    proj_secondary = g.secondary(project)
    make_double_report(
        proj_primary,
        proj_secondary,
        pdf_file,
        excel_file,
        fields,
        events)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    logging.info('connecting to redcap')

    g = Garjus()

    compare(g, PROJECT, FIELDS, EVENTS)

    logging.info('Done!')
