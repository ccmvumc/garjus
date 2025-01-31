"""Creates report PDF with zip."""
import logging
import io
from datetime import datetime
import math

import numpy as np
import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
from fpdf import FPDF
from fpdf.enums import XPos, YPos
from PIL import Image


logger = logging.getLogger('garjus.progress.report')


# These are used to set colors of graphs
RGB_DKBLUE = 'rgb(59,89,152)'
RGB_BLUE = 'rgb(66,133,244)'
RGB_GREEN = 'rgb(15,157,88)'
RGB_YELL = 'rgb(244,160,0)'
RGB_RED = 'rgb(219,68,55)'
RGB_PURP = 'rgb(160,106,255)'
RGB_GREY = 'rgb(200,200,200)'
RGB_PINK = 'rgb(255,182,193)'
RGB_LIME = 'rgb(17, 180, 101)'

# Give each status a color to display
QASTATUS2COLOR = {
    'PASS': RGB_GREEN,
    'NQA': RGB_LIME,
    'NPUT': RGB_YELL,
    'FAIL': RGB_RED,
    'NONE': RGB_GREY,
    'JOBF': RGB_PINK,
    'JOBR': RGB_BLUE}

STATUS2RGB = dict(zip(
    ['WAITING', 'PENDING', 'RUNNING', 'COMPLETE', 'FAILED', 'UNKNOWN', 'JOBF'],
    [RGB_GREY, RGB_YELL, RGB_GREEN, RGB_BLUE, RGB_RED, RGB_PURP, RGB_PINK]))

# These are used to make progress reports
ASTATUS2COLOR = {
    'PASS': RGB_GREEN,
    'NPUT': RGB_YELL,
    'FAIL': RGB_RED,
    'NQA': RGB_LIME,
    'NONE': RGB_GREY,
    'COMPLETE': RGB_BLUE,
    'UNKNOWN': RGB_PURP}

HIDECOLS = [
    'assessor_label',
    'PROJECT',
    'SESSION',
    'SUBJECT',
    'AGE',
    'SEX',
    'DEPRESS',
    'TYPE',
    'SITE',
    'SESSTYPE',
    'DATE',
    'ASSR',
    'PROCSTATUS',
    'QCSTATUS',
    'INPUTS',
    'MODALITY',
    'XSITYPE',
    'PROCTYPE',
    'full_path',
    'case',
]


class MYPDF(FPDF):
    """Custom PDF."""

    def set_filename(self, filename):
        """Set the filename."""
        self.filename = filename

    def set_project(self, project, enable_monthly=False):
        """Set the project name."""
        self.project = project
        today = datetime.now().strftime("%Y-%m-%d")
        self.date = today

        if enable_monthly:
            self.title = f'{project} Monthly Report'
            self.subtitle = '{}'.format(datetime.now().strftime("%B %Y"))
        else:
            self.title = f'{project} Report'
            self.subtitle = '{}'.format(datetime.now().strftime("%B %d, %Y"))

    def set_date(self):
        today = datetime.now().strftime("%Y-%m-%d")
        self.date = today

    def footer(self):
        """Return the custom footer."""
        self.set_y(-0.35)
        self.set_x(0.5)

        # Write date, title, page number
        self.set_font('helvetica', size=10)
        self.set_text_color(100, 100, 100)
        self.set_draw_color(100, 100, 100)
        self.line(x1=0.2, y1=10.55, x2=8.3, y2=10.55)
        self.cell(w=1, text=self.date)
        self.cell(w=5, align='C', text=self.title)
        self.cell(w=2.5, align='C', text=str(self.page_no()))


def blank_letter():
    """Blank letter sized PDF."""
    p = MYPDF(orientation="P", unit='in', format='letter')
    p.set_top_margin(0.5)
    p.set_left_margin(0.5)
    p.set_right_margin(0.5)

    return p


def _draw_counts(pdf, subjects):
    # Counts of each type with sums
    project_list = sorted(subjects.PROJECT.unique())
    group_list = subjects.GROUP.unique()
    indent_width = max(2.5 - len(project_list) * 0.5, 0.3)

    pdf.set_fill_color(94, 156, 211)
    _txt = 'Participant Groups'

    # Draw heading
    pdf.set_font('helvetica', size=14)
    pdf.cell(w=7.5, h=0.5, text=_txt, align='C', border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # Header Formatting
    pdf.cell(w=0.7)
    pdf.set_text_color(245, 245, 245)
    pdf.set_line_width(0.01)
    _kwargs = {'w': 0.9, 'h': 0.5, 'border': 1, 'align': 'C', 'fill': True}

    # Column header for each project
    pdf.cell(indent_width)
    for cur_proj in project_list:
        _txt = cur_proj
        if len(_txt) > 6:
            pdf.set_font('helvetica', size=9)
        else:
            pdf.set_font('helvetica', size=12)

        pdf.cell(**_kwargs, text=_txt)

    # Got to next line
    pdf.ln()

    # Row formatting
    pdf.set_fill_color(255, 255, 255)
    pdf.set_text_color(0, 0, 0)
    _kwargs = {'w': 0.9, 'h': 0.5, 'border': 1, 'align': 'C', 'fill': False}
    _kwargs_s = {'w': 0.7, 'h': 0.5, 'border': 1, 'align': 'C', 'fill': False}
    _kwargs_t = {'w': 0.5, 'h': 0.5, 'border': 1, 'align': 'C', 'fill': False}

    # Row for each group
    for cur_group in group_list:
        pdf.cell(w=indent_width)

        dfg = subjects[subjects.GROUP == cur_group]

        # Show the group
        _txt = cur_group
        if _txt == 'UNKNOWN':
            _txt = '?'

        if len(_txt) > 6:
            pdf.set_font('helvetica', size=12)
        else:
            pdf.set_font('helvetica', size=14)

        pdf.cell(**_kwargs_s, text=_txt)

        # Count each type for this group
        pdf.set_font('helvetica', size=14)
        for cur_proj in project_list:
            cur_count = str(len(dfg[dfg.PROJECT == cur_proj]))
            pdf.cell(**_kwargs, text=cur_count)

        if len(project_list) > 1:
            # Total for group
            cur_count = str(len(dfg))
            pdf.cell(**_kwargs_t, text=cur_count, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        else:
            #pdf.cell(w=0, h=0, border=0, text='', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            pdf.ln()

    pdf.set_font('helvetica', size=14)
    if len(group_list) > 1:
        # TOTALS row
        pdf.cell(w=indent_width)
        pdf.cell(w=0.7)
        for cur_proj in project_list:
            cur_count = str(len(subjects[subjects.PROJECT == cur_proj]))
            pdf.cell(**_kwargs, text=cur_count)

        # Grandtotal
        if len(project_list) > 1:
            pdf.cell(**_kwargs_t, text=str(len(subjects)))

    # End by going to next line
    pdf.ln()

    return pdf


def _draw_demog(pdf, subjects):
    # Counts of each type with sums
    project_list = sorted(subjects.PROJECT.unique())
    #sex_list = subjects.SEX.unique()
    sex_list = ['F']
    indent_width = max(2.5 - len(project_list) * 0.5, 0.3)

    pdf.set_fill_color(114, 172, 77)

    _txt = 'Participant Demographics'

    # Draw heading
    pdf.set_font('helvetica', size=14)
    pdf.cell(w=7.5, h=0.5, text=_txt, align='C', border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # Header Formatting
    pdf.cell(w=0.5)
    pdf.set_text_color(245, 245, 245)
    pdf.set_line_width(0.01)
    _kwargs = {'w': 1.0, 'h': 0.5, 'border': 1, 'align': 'C', 'fill': True}

    # Column header for each project
    pdf.cell(indent_width)
    for cur_proj in project_list:
        _txt = cur_proj
        if len(_txt) > 6:
            pdf.set_font('helvetica', size=10)

        pdf.cell(**_kwargs, text=_txt)

    # Got to next line
    pdf.ln()

    # Row formatting
    pdf.set_fill_color(255, 255, 255)
    pdf.set_text_color(0, 0, 0)
    _kwargs = {'w': 1.0, 'h': 0.5, 'border': 1, 'align': 'C', 'fill': False}
    _kwargs_s = {'w': 0.5, 'h': 0.5, 'border': 1, 'align': 'C', 'fill': False}
    _kwargs_t = {'w': 0.5, 'h': 0.5, 'border': 1, 'align': 'C', 'fill': False}


    # Row for each sex
    for cur_sex in sex_list:
        pdf.cell(w=indent_width)

        dfg = subjects[subjects.SEX == cur_sex]

        # Show this
        _txt = f'% {cur_sex}'

        if len(_txt) > 6:
            pdf.set_font('helvetica', size=12)
        else:
            pdf.set_font('helvetica', size=14)

        pdf.cell(**_kwargs_s, text=_txt)

        # Count each type for this
        pdf.set_font('helvetica', size=14)
        for cur_proj in project_list:
            proj_tot = len(subjects[subjects.PROJECT == cur_proj])
            proj_sex = len(dfg[dfg.PROJECT == cur_proj])
            cur_pct = str(int(proj_sex / proj_tot * 100 + 0.5))
            pdf.cell(**_kwargs, text=cur_pct)

        pdf.ln()

    pdf.cell(w=indent_width)
    pdf.cell(**_kwargs_s, text='Age')

    for cur_proj in project_list:
        cur_age = subjects[subjects.PROJECT == cur_proj].AGE.astype(float).mean()
        pdf.cell(**_kwargs, text=str(int(cur_age)))

    # End by going to next line
    pdf.ln()

    return pdf


def _draw_proc(pdf, stats):
    common_count = len(stats.dropna(axis=1).columns)
    if common_count == len(stats.columns):
        common_count = 8 # ASSR,PROJECT,SUBJECT,SESSION,SESSTYPE,SITE,DATE,PROCTYPE

    # Draw heading
    pdf.set_font('helvetica', size=14)
    _txt = 'Processing Types (see additional pages for details)\n' 
    pdf.cell(w=7.5, h=0.5, text=_txt, align='C', border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('helvetica', size=12)

    # Column header
    pdf.cell(1.5)
    pdf.set_line_width(0.01)
    pdf.set_text_color(245, 245, 245)
    pdf.set_fill_color(232, 108, 32)
    pdf.cell(w=2.0, h=0.3, border=1, align='C', text='Type', fill=True)
    pdf.cell(w=1.0, h=0.3, border=1, align='C', text='Rows', fill=True)
    pdf.cell(w=1.0, h=0.3, border=1, align='C', text='Columns', fill=True)
    pdf.ln()

    pdf.set_fill_color(255, 255, 255)
    pdf.set_text_color(0, 0, 0)
    for proc in sorted(stats.PROCTYPE.unique()):
        pdf.cell(1.5)
        pstats = stats[stats.PROCTYPE == proc]
        _txt = proc
        pdf.cell(w=2.0, h=0.3, border=1, text=_txt)
        _txt = str(len(pstats))
        pdf.cell(w=1.0, h=0.3, border=1, align='C', text=_txt)
        _txt = str(len(pstats.dropna(axis=1).columns) - common_count)
        pdf.cell(w=1.0, h=0.3, border=1, align='R', text=_txt, new_x=XPos.LMARGIN, new_y=YPos.NEXT)


def _add_first_page(pdf, info):
    subjects = info['subjects']
    stats = info['stats']

    # Start the page with titles
    pdf.add_page()
    pdf.set_font('helvetica', size=16)
    pdf.cell(w=7.5, h=0.4, align='C', text=pdf.title, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('helvetica', size=12)
    pdf.cell(w=7.5, h=0.4, align='C', text=pdf.subtitle, border='B', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(0.1)

    pdf.set_font('helvetica', size=12)

    # Show settings for proctypes, sesstypes, projects, etc.
    _text = 'Subject data in subjects.csv, processing data in stats/[TYPE].csv\n'
    _text += f'XNAT: {info.get("xnat")}\n'
    _text +=f'REDCap:{info.get("redcap")}\n'
    _text += f'Session Types: '
    _text += ','.join(sorted(list(stats.SESSTYPE.unique())))
    pdf.multi_cell(0, 0.25, text=_text, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='L')

    # Show subject counts by project
    _draw_counts(pdf, subjects)
    pdf.ln(0.2)

    # Show demographics table
    if not subjects.isna().any().any():
        _draw_demog(pdf, subjects)
        pdf.ln(0.2)
    else:
        logger.info('missing Demographics')

    # Show processing types table
    _draw_proc(pdf, stats)

    return pdf

def _add_stats_fmriqa(pdf, stats, info):
    # get the stats data by scan type using inputs field to map to scan

    scans = info['scans']

    assessors = info['assessors'].copy()
    assessors = assessors[assessors.PROCTYPE == 'fmriqa_v4']

    # Extract scan fmri value into a column
    assessors['scan_fmri'] = assessors.apply(
        lambda row: row['INPUTS'].get('scan_fmri'), axis=1)

    df = pd.merge(
        assessors[['ASSR', 'scan_fmri']],
        scans[['full_path', 'SCANTYPE']],
        how='inner',
        left_on='scan_fmri',
        right_on='full_path')

    df = pd.merge(
        stats,
        df[['ASSR', 'SCANTYPE']],
        how='inner',
        left_on='ASSR',
        right_on='ASSR')

    for t in sorted(df.SCANTYPE.unique()):
        if t in ['fMRI_rest', 'fMRI_REST_FSA']:
            continue

        pdf.set_font('helvetica', size=10)
        _add_stats(pdf, df[df.SCANTYPE == t], plot_title=f'Scan Type: {t}')


def _add_stats(pdf, stats, plot_title=None):

    # this returns a PIL Image object
    image = plot_stats(stats, plot_title)
    tot_width, tot_height = image.size

    # Split horizontal image into chunks of width to fit on
    # letter-sized page with crop((left, top, right, bottom))
    chunk_h = 500
    chunk_w = 998
    rows_per_page = 3  # 3 rows per page
    page_count = math.ceil(tot_width / (rows_per_page * chunk_w))

    for p in range(page_count):
        for c in range(rows_per_page):
            # Calculate the starting x for this chunk
            chunk_x = (c * chunk_w) + (p * chunk_w * rows_per_page)
            if chunk_x > tot_width - 10:
                # out of bounds
                continue

            # Get the image from the cropped section
            _img = image.crop((chunk_x, 0, chunk_x + chunk_w, chunk_h))

            # Draw the image on the PDF
            pdf.image(_img, x=0.75, h=3.1)

    return pdf


def _plottable(var):
    try:
        _ = var.replace('', np.nan).dropna().str.strip('%').astype(float)
        return True
    except Exception:
        return False


def plot_stats(df, plot_title=None):
    """Plot stats, one boxlplot per var."""
    box_width = 250
    min_box_count = 4

    logger.debug('plot_stats:{}'.format(len(df)))

    # Check for empty data
    if len(df) == 0:
        logger.debug('empty data, using empty figure')
        fig = go.Figure()
        _png = fig.to_image(format="png")
        image = Image.open(io.BytesIO(_png))
        return image

    # Filter var list to only include those that have data
    var_list = [x for x in df.columns if not pd.isnull(df[x]).all()]

    # Filter var list to only stats variables
    var_list = [x for x in var_list if x not in HIDECOLS]

    # Filter var list to only stats can be plotted as float
    var_list = [x for x in var_list if _plottable(df[x])]

    # skip vars
    var_list = [x for x in var_list if not x.endswith('pathlength')]

    # Determine how many boxplots we're making, depends on how many vars, use
    # minimum so graph doesn't get too small
    box_count = len(var_list)
    site_count = len(df['SITE'].unique())

    if box_count < 3:
        box_width = 500
        min_box_count = 2
    elif box_count < 6:
        box_width = 333
        min_box_count = 3

    if site_count > 3:
        box_width = 500
        min_box_count = 2

    if box_count < min_box_count:
        box_count = min_box_count

    graph_width = box_width * box_count

    # Horizontal spacing cannot be greater than (1 / (cols - 1))
    hspacing = 1 / (box_count * 4)

    logger.debug(f'{box_count}, {min_box_count}, {graph_width}, {hspacing}')

    # Make the figure with 1 row and a column for each var we are plotting
    var_titles = [x[:22] for x in var_list]
    fig = plotly.subplots.make_subplots(
        rows=1,
        cols=box_count,
        horizontal_spacing=hspacing,
        subplot_titles=var_titles)

    # Add box plot for each variable
    for i, var in enumerate(var_list):

        # Create boxplot for this var and add to figure
        logger.debug('plotting var:{}'.format(var))

        _row = 1
        _col = i + 1

        fig.append_trace(
            go.Box(
                y=df[var].replace('', np.nan).dropna().str.strip('%').astype(float),
                x=df['SITE'],
                boxpoints='all',
                text=df['ASSR'],
                boxmean=True,
            ),
            _row,
            _col)

        # Plot horizontal line at median
        _median = df[var].replace('', np.nan).dropna().str.strip('%').astype(float).median()
        fig.add_trace(
            go.Scatter(
                x=df['SITE'],
                y=[_median] * len(df),
                mode='lines',
                name='',
                fillcolor='red',
                line_color='grey'),
            _row, _col)

        fig.update_yaxes(autorange=True)

        if var.startswith('con_') or var.startswith('inc_'):
            logger.debug('setting beta range:{}'.format(var))
            _yaxis = 'yaxis{}'.format(i + 1)
            fig['layout'][_yaxis].update(range=[-1, 1], autorange=False)
        else:
            pass

    # Move the subtitles to bottom instead of top of each subplot
    if len(df['SITE'].unique()) < 4:
        for i in range(len(fig.layout.annotations)):
            fig.layout.annotations[i].update(y=-.15)

    if plot_title:
        fig.update_layout(
            title={'text': plot_title, 'x': 0.5, 'xanchor': 'center'})

    # Customize figure to hide legend and fit the graph
    fig.update_layout(
        showlegend=False,
        autosize=False,
        width=graph_width,
        margin=dict(l=20, r=40, t=40, b=80, pad=0))

    _png = fig.to_image(format="png")
    image = Image.open(io.BytesIO(_png))
    return image


def _add_stats_pages(pdf, info):
    proclib = info['proclib']
    stats = info['stats']
    stattypes = info['stattypes']

    for proctype in stattypes:
        # Limit the data to this proctype
        stat_data = stats[stats.PROCTYPE == proctype]

        if stat_data.empty:
            logger.debug(f'no stats for proctype:{proctype}')
            continue

        logger.debug(f'add stats page:{proctype}')

        # Get descriptions for this processing type
        proc_info = proclib.get(proctype, {})

        # use proclib to filter stats variable names
        _subset = proc_info.get('stats_subset', None)
        if _subset:
            stat_data = stat_data[_subset + ['SITE', 'ASSR']]

        # Now make the page
        pdf.add_page()
        pdf.set_font('helvetica', size=14)
        pdf.cell(text=proctype, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        if proctype == 'fmriqa_v4':
            # stats by scan type using inputs field to map to scan
            _add_stats_fmriqa(pdf, stat_data, info)
        else:
            _add_stats(pdf, stat_data)

        # Build the description
        _text = proc_info.get('short_descrip', '') + '\n'
        _text += 'Inputs: ' + proc_info.get('inputs_descrip', '') + '\n'

        # Append stats descriptions
        for s, t in info['statlib'].get(proctype, {}).items():
            _text += f'{s}: {t}\n'

        # Show the descriptions
        pdf.set_font('helvetica', size=12)
        pdf.multi_cell(0, 0.25, _text, border='LBTR', align="L", new_x=XPos.RIGHT, new_y=YPos.NEXT)

        _url = proc_info.get('procurl', '')
        if _url:
            pdf.set_font('helvetica', size=10)
            pdf.cell(text=_url, link=_url)


def make_export_report(filename, garjus, subjects, stats):
    # Initialize a new PDF letter size and shaped
    pdf = blank_letter()
    pdf.set_filename(filename)
    pdf.set_date()
    pdf.title = f'Data Export Report'
    pdf.subtitle = '{}'.format(datetime.now().strftime("%B %d, %Y"))
    info = {}

    p2p = {
        'TAYLOR_CAARE': 'CAARE',
        'NewhouseMDDHx': 'MDDHx'
    }
    subjects['PROJECT'] = subjects.PROJECT.replace(p2p)
    stats['PROJECT'] = stats.PROJECT.replace(p2p)
    subjects['SEX'] = subjects.SEX.fillna('')

    info['xnat'] = garjus.xnat_host()
    info['redcap'] = 'https://redcap.vumc.org/redcap_v14.8.1/index.php?pid=156730'
    info['proclib'] = garjus.processing_library()
    info['subjects'] = subjects
    info['stats'] = stats
    info['stattypes'] = stats.PROCTYPE.unique()
    info['statlib'] = garjus.stats_library()

    logger.debug('adding first page')
    _add_first_page(pdf, info)

    logger.debug('adding stats pages')
    _add_stats_pages(pdf, info)

    # Save to file
    print(f'saving pdf:{pdf.filename}')
    logger.debug('saving PDF to file:{}'.format(pdf.filename))
    try:
        pdf.output(pdf.filename)
    except Exception as err:
        logger.error('error while saving PDF:{}:{}'.format(pdf.filename, err))
        return False

    return True
