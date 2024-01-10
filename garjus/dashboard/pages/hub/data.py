"""dashboard home"""
import logging
from dateutil.relativedelta import relativedelta
from datetime import datetime

from .. import queue, issues, activity, reports
from ....garjus import Garjus


logger = logging.getLogger('dashboard.hub.data')


def _get_reports_data(refresh=False):
    df = reports.data.load_data(refresh=refresh)
    return df


def _get_automations_data(g=None, refresh=False):
    if g is None:
        g = Garjus()

    df = pd.DataFrame()
    return df


def _get_processing_data(g=None, refresh=False):
    if g is None:
        g = Garjus()

    df = g.processing_protocols()
    return df


def _get_activity_data(g=None, refresh=False):
    #df = activity.data.load_data(refresh=True, )
    startdate = datetime.today() - relativedelta(days=7)
    startdate = startdate.strftime('%Y-%m-%d')

    if g is None:
        g = Garjus()

    if not g.redcap_enabled():
        return None

    logger.info(f'loading activity:startdate={startdate}')
    df = g.activity(startdate=startdate)
    df.reset_index(inplace=True)
    df['ID'] = df.index

    return df


def _get_queue_data(refresh=False):
    return queue.data.load_data(refresh=refresh, hidedone=True)


def _get_issues_data(refresh=False):
    return issues.data.load_data(refresh=refresh)


def _load_options(df):
    return list(df.PROJECT.unique())

