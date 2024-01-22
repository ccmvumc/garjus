"""Compare 1st and 2nd Entry."""
import os
import logging
from datetime import datetime

import pandas as pd
from fpdf import FPDF


# Compares two REDCap projects, as First and Second where Second is the
# subset of fields that should be double-entered. Outputs a PDF file
# and an excel file. The PDF provides an overview/summary of the Excel
# file. The excel file contains the specific missing and conflicting items.
# Output files are named with the specified prefix.


# Names and descriptions of the sheets in the excel output file
SHEETS = [{
'name': 'Mismatches',
'description': 'Mismatches is the list of discrepancies between the First \
and Second REDCap projects with one row per mismatch.'''
},
{
'name': 'MissingSubjects',
'description': 'MissingSubjects is the list of subjects that are found in \
the First REDCap project but are completely missing from the Second.'
},
{
'name': 'MissingEvents',
'description': 'MissingEvents is the list of subject events that are found \
in the First REDCap project, but are completely missing from the Second.'
},
{
'name': 'MissingValues',
'description': 'MissingEvents is the list of values that are found \
in the First REDCap project, but are blank in the Second.'
},
{
'name': 'FieldsCompare',
'description': 'FieldsCompare is the list of fields that are INCLUDED for \
comparison. This list includes all fields in FieldsCommon excluding those \
in Fields2ndNan.'
},
{
'name': 'FieldsCommon',
'description': 'FieldsCommon is the list of fields that are found in both \
First and Second REDCap projects.'
},
{
'name': 'Fields1stOnly',
'description': 'Fields1stOnly is the list of fields that are found only in \
the First REDCap. These fields are EXCLUDED from comparisons.'
},
{
'name': 'Fields2ndOnly',
'description': 'Fields2ndOnly is the list of fields that are found only in \
the Second REDCap project. This should be an empty list.'
},
{
'name': 'Fields2ndNan',
'description': 'Fields2ndNan is the list of fields that are found in both \
REDCap projects, but all values are blank in the Second REDCap. This list \
should be empty.'
}]

IGNORE_FIELDS = ['record_id', 'subj_num', 'ddes_12child', 'entry_consider_list',
    'entryconsid_c_list' 'pe_date', 'physical_specif', 'physical_yesno',
    'pvas_date', 'pvas_interfere', 'pvas_pain'
]


# Our custom PDF file format
class MYPDF(FPDF):
    def footer(self):
        today = datetime.now().strftime("%Y-%m-%d")
        self.date = today
        self.title = 'Double Entry'
        self.subtitle = '{}'.format(datetime.now().strftime("%B %Y"))

        self.set_y(-0.35)
        self.set_x(0.5)

        # Write date, title, page number
        self.set_font('helvetica', size=10)
        self.set_text_color(100, 100, 100)
        self.set_draw_color(100, 100, 100)
        self.line(x1=0.2, y1=10.55, x2=8.3, y2=10.55)
        self.cell(w=1, txt=self.date)
        self.cell(w=5, align='C', txt=self.title)
        self.cell(w=2.5, align='C', txt=f'{self.page_no()} of {{nb}}')


def make_pdf(results, filename):
    logging.debug('making PDF')

    # Initialize a new PDF letter size and shaped
    pdf = MYPDF(orientation="P", unit='in', format='letter')
    pdf.add_page()

    # Give it a title at the top
    title = 'Double Data Entry Comparison'
    pdf.set_font('helvetica', size=18)
    pdf.cell(w=7, h=0.5, txt=title, border=0, ln=1)

    # Iterate the heading section
    for key, val in results['session'].items():
        pdf.set_font('helvetica', size=13)
        pdf.cell(w=1.3, h=.6, txt=key, border=0)
        pdf.set_font('courier', size=14)
        pdf.cell(w=6, h=.6, txt=val, border=1, ln=1)

    # Show results counts section
    counts = results['counts']

    pdf.ln(0.1)
    pdf.set_font('helvetica', size=14)
    pdf.cell(1, 0.4, 'RESULTS:', ln=1)

    txt = 'Matches'
    val = str(counts['matches'])
    pdf.set_font('courier', size=12)
    pdf.cell(w=2, h=.3, txt=txt, border=0)
    pdf.cell(w=1, h=.3, txt=val, border=1, align='C', ln=1)

    txt = 'Mismatches'
    val = str(counts['mismatches'])
    dsc = 'see Mismatches sheet'
    pdf.set_font('courier', size=12)
    pdf.cell(w=2, h=.3, txt=txt, border=0)
    pdf.cell(w=1, h=.3, txt=val, border=1, align='C')
    pdf.set_font('courier', size=9)
    pdf.cell(w=5, h=.3, txt=dsc, border=0, ln=1)

    txt = 'Missing Subjects'
    val = str(counts['missing_subjects'])
    dsc = 'see MissingSubjects sheet'
    pdf.set_font('courier', size=12)
    pdf.cell(w=2, h=.3, txt=txt, border=0)
    pdf.cell(w=1, h=.3, txt=val, border=1, align='C')
    pdf.set_font('courier', size=9)
    pdf.cell(w=5, h=.3, txt=dsc, border=0, ln=1)

    txt = 'Missing Events'
    val = str(counts['missing_events'])
    dsc = 'see MissingEvents sheet'
    pdf.set_font('courier', size=12)
    pdf.cell(w=2, h=.3, txt=txt, border=0)
    pdf.cell(w=1, h=.3, txt=val, border=1, align='C')
    pdf.set_font('courier', size=9)
    pdf.cell(w=5, h=.3, txt=dsc, border=0, ln=1)

    txt = 'Missing Values'
    val = str(counts['missing_values'])
    dsc = 'see MissingValues sheet'
    pdf.set_font('courier', size=12)
    pdf.cell(w=2, h=.3, txt=txt, border=0)
    pdf.cell(w=1, h=.3, txt=val, border=1, align='C')
    pdf.set_font('courier', size=9)
    pdf.cell(w=5, h=.3, txt=dsc, border=0, ln=1)

    pdf.ln(0.25)

    pdf.set_font('helvetica', size=10)
    _txt = 'The sheets in the excel file are:'
    pdf.cell(w=7.5, h=0.3, txt=_txt, border='T', ln=1)

    # Add sheet descriptions
    for s in SHEETS:
        add_sheet_description(pdf, s['name'], s['description'])

    # Save to file
    logging.info(f'saving PDF to file:{filename}')
    try:
        pdf.output(filename)
    except Exception as err:
        logging.error(f'error while saving PDF:{filename}:{err}')


def add_sheet_description(pdf, name, description):
    # Write the name and description to the PDF
    pdf.set_font(style='B', size=8)
    pdf.cell(w=1.2, h=0.4, txt=name, border=0)
    pdf.set_font(style='')
    pdf.multi_cell(w=6.3, h=0.4, txt=description, border='B', ln=1, align='L')


def get_fields(p1, p2):
    # Get all the records so we can check for all nan
    logging.debug(f'exporting p2 records')
    records = p2.export_records()

    common_fields = sorted(list(set(p1.field_names) & set(p2.field_names)))
    p1_only_fields = sorted(list(set(p1.field_names) - set(p2.field_names)))
    p2_only_fields = sorted(list(set(p2.field_names) - set(p1.field_names)))
    p2_used_fields = sorted(list(set({k for d in records for k in d.keys()})))
    p2_nan_fields = sorted(list(set(common_fields) - set(p2_used_fields)))
    compare_fields = sorted(list(set(common_fields) - set(p2_nan_fields)))
    compare_fields = [x for x in compare_fields if x not in IGNORE_FIELDS]

    fields = {
        'compare': compare_fields,
        'common': common_fields,
        'p1_only': p1_only_fields,
        'p2_only': p2_only_fields,
        'p2_nan': p2_nan_fields,
    }

    return fields


def write_sheet(data, writer, name):
    # Format as string to avoid formatting mess
    df = pd.DataFrame(data=data, dtype=str)

    # Sort by all columns in order
    df = df.sort_values(by=list(df.columns))

    # Write the dataframe to the excel file
    df.to_excel(writer, sheet_name=name, index=False)

    # Auto-adjust columns' width
    col_fmt = writer.book.add_format({'num_format': '@'})
    for column in df:
        col_width = max(df[column].astype(str).map(len).max(), len(column))
        col_idx = df.columns.get_loc(column)
        writer.sheets[name].set_column(col_idx, col_idx, col_width, col_fmt)

    # Ignore errors
    _range = '{}{}:{}{}'.format('A', 1, 'F', len(df) + 1)
    writer.sheets[name].ignore_errors({'number_stored_as_text': _range})


def write_excel(info, outfile):
    # Write each sheet to excel
    with pd.ExcelWriter(outfile) as w:
        if info['mismatches']:
            write_sheet(info['mismatches'], w, 'Mismatches')

        if info['missing_subjects']:
            write_sheet(info['missing_subjects'], w, 'MissingSubjects')

        if info['missing_events']:
            write_sheet(info['missing_events'], w, 'MissingEvents')

        if info['missing_values']:
            write_sheet(info['missing_values'], w, 'MissingValues')

        write_sheet(info['fields']['common'], w, 'FieldsCommon')
        write_sheet(info['fields']['compare'], w, 'FieldsCompare')

        write_sheet(info['fields']['p1_only'], w, 'Fields1stOnly')
        write_sheet(info['fields']['p2_only'], w, 'Fields2ndOnly')
        write_sheet(info['fields']['p2_nan'], w, 'Fields2ndNan')


def _boring_record(record, compare_fields):
    for f in compare_fields:
        if record.get(f, False):
            # There's a value
            return False

    return True


def export_all_records(project, step=100, fields=None, events=None):
    records = []

    # Get a list of unique IDs
    _def = project.def_field
    ids = [r[_def] for r in project.export_records(fields=[_def])]
    ids = sorted(list(set(ids)))

    # How many?
    count = len(ids)

    # Load in chunks of step size
    for i in range(0, count, step):
        if fields:
            r = project.export_records(records=ids[i:i + step], fields=fields, events=events)
        else:
            r = project.export_records(records=ids[i:i + step], events=events)

        records.extend(r)

    return records


def compare_projects(p1, p2, compare_fields=None, compare_events=None):
    # Compares two redcap projects and returns the results
    results = {}
    missing_subjects = []
    missing_events = []
    missing_values = []
    mismatches = []
    sec_field = None
    match_count = 0

    # Create index of record ID to subject ID in p1
    def_field = p1.def_field
    sec_field = p1.export_project_info()['secondary_unique_field']
    def_field2 = p2.def_field
    sec_field2 = p2.export_project_info()['secondary_unique_field']

    if sec_field:
        rec = p1.export_records(fields=[def_field, sec_field])
        id2subj1 = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}

        # Create index of subject ID mapped to record ID in p2
        rec = p2.export_records(fields=[def_field2, sec_field2])
        subj2id2 = {x[sec_field2]: x[def_field2] for x in rec if x[sec_field2]}
    else:
        rec = p1.export_records(fields=[def_field])
        id2subj1 = {x[def_field]: x[def_field] for x in rec if x[def_field]}
        rec = p2.export_records(fields=[def_field2])
        subj2id2 = {x[def_field2]: x[def_field2] for x in rec if x.get(sec_field2, False)}

    fields = get_fields(p1, p2)

    if not compare_fields:
        # Determine which fields to compare
        compare_fields = fields['compare']
    else:
        fields['compare'] = compare_fields

    logging.debug(f'compare_fields={compare_fields}')
    logging.debug(f'compare_fields={compare_events}')

    # Find date fields
    date_fields = [x for x in compare_fields if ('date' in x or '_dt' in x)]
    logging.debug(f'date_fields={date_fields}')

    # Get the records from the First project
    logging.debug(f'exporting p1 records')
    records1 = export_all_records(
        p1,
        fields=compare_fields,
        events=compare_events
    )

    # Set subject id
    for i, r in enumerate(records1):
        try:
            rid1 = r[def_field]
            records1[i]['sid'] = id2subj1[rid1]
        except KeyError as err:
            logging.debug(f'blank subject ID for record:{rid1}:{err}')
            records1[i]['sid'] = rid1

    # Sort by subject
    records1 = sorted(records1, key=lambda x: x['sid'])

    p2_events = [x['unique_event_name'] for x in p2.export_events()]
    logging.debug(f'p2_events={p2_events}')

    # Compare each record
    for r1 in records1:
        sid = r1['sid']
        eid = r1['redcap_event_name']
        name1 = str(r1.get('redcap_repeat_instrument', ''))
        num1 = str(r1.get('redcap_repeat_instance', ''))
        r2 = None
        name2 = ''
        num2 = ''

        if eid not in p2_events:
            logging.debug(f'No record in double/second:{sid}:{eid}')
            missing_events.append((sid, eid))
            continue

        if _boring_record(r1, compare_fields):
            # nothing to compare
            continue

        # Check that we already found as missing
        if sid in missing_subjects:
            # Skip this subject, already missing
            logging.debug(f'subject already missing:{sid}')
            continue

        if (sid, eid) in missing_events:
            # Skip this event, already missing
            logging.debug(f'event already missing:{sid},{eid}')
            continue

        # Get id in the secondary redcap project
        try:
            rid2 = subj2id2[sid]
        except KeyError as err:
            logging.debug(f'missing subject:{rid1}:{err}')
            missing_subjects.append(sid)
            continue

        # Get records from secondary redcap for this subject/event
        try:
            records2 = p2.export_records(records=[rid2], events=[eid])
        except Exception:
            import traceback
            traceback.print_exc()
            records2 = []

        # Find the best record to compare
        if len(records2) == 1:
            # Only one so use it
            r2 = records2[0]
            name2 = str(r2.get('redcap_repeat_instrument', ''))
            num2 = str(r2.get('redcap_repeat_instance', ''))
        else:
            # Find a matching date or instance number
            for e2 in records2:
                name2 = str(e2.get('redcap_repeat_instrument', ''))
                num2 = str(e2.get('redcap_repeat_instance', ''))

                if name1 != name2:
                    continue

                # First check specific identifiers
                if r1.get('vasf_timepoint', False):
                    if r1['vasf_timepoint'] == e2['vasf_timepoint']:
                        logging.debug(f'vasf_timepoint match:{sid}:{eid}:{name1}:{num1}:{name2}:{num2}')
                        r2 = e2
                        break
                    else:
                        continue

                # Try to match a date
                for cur_field in date_fields:
                    # Does 1st have a value and does it match 2nd
                    if r1[cur_field] and r1[cur_field] == e2[cur_field]:
                        # This is the record
                        logging.debug(f'date match:{sid}:{eid}:{name1}:{num1}:{cur_field}:{name2}:{num2}')
                        r2 = e2
                        break

                if r2:
                    break

            if r2 is None:
                for e2 in records2:
                    name2 = str(e2.get('redcap_repeat_instrument', ''))
                    num2 = str(e2.get('redcap_repeat_instance', ''))

                    if name1 == name2 and num1 == num2:
                        logging.debug(f'{sid}:{eid}:instance match:{name1}:{num1}:{num2}')
                        r2 = e2
                        break

            if r2 is None:
                logging.debug(f'NO MATCH:{sid}:{eid}:{name1}:{num1}')

        # Check for conflicts in best matching record 2
        if (name1 != name2) or (not name1 and (num1 != num2)):
            logging.debug(f'skipping, name conflict:{sid},{eid},{rid1},{rid2},{name1},{num1},{name2},{num2}')
            r2 = None

        if r2:
            logging.debug(f'compare_records:{sid},{eid},{rid1},{rid2},{name1},{num1},{name2},{num2}')
            (mism, misv, _m) = compare_records(r1, r2, compare_fields)
            mismatches += mism
            missing_values += misv
            match_count += _m
        else:
            _eid = eid

            if name1:
                _eid = f'{_eid}:{name1}'

            if num1:
                _eid = f'{_eid}:{num1}'

            logging.debug(f'No record in double/second:{sid}:{_eid}')
            missing_events.append((sid, _eid))

    # Count results
    results['counts'] = {
        'missing_values': len(missing_values),
        'missing_events': len(missing_events),
        'missing_subjects': len(missing_subjects),
        'mismatches': len(mismatches),
        'matches': match_count,
    }

    # Convert subjects list of dicts, we do this here so we can keep
    # a simple list during the loop to check for already missing
    if missing_subjects:
        missing_subjects = [{'SUBJECT': s} for s in missing_subjects]
    else:
        missing_subjects = None

    # Convert events to list of dicts
    if missing_events:
        _keys = ['SUBJECT', 'EVENT']
        missing_events = [dict(zip(_keys, v)) for v in missing_events]
    else:
        missing_events = None

    # Append results
    results['missing_subjects'] = missing_subjects
    results['missing_events'] = missing_events
    results['missing_values'] = missing_values
    results['mismatches'] = mismatches

    # Append fields information
    results['fields'] = {}
    for k in ['compare', 'common', 'p1_only', 'p2_only', 'p2_nan']:
        if fields[k]:
            results['fields'][k] = [{'FIELD': v} for v in fields[k]]
        else:
            results['fields'][k] = [{'FIELDS': ''}]

    # Get project titles and ids
    p1_info = p1.export_project_info()
    p2_info = p2.export_project_info()
    name1 = '{} ({})'.format(p1_info['project_title'], p1_info['project_id'])
    name2 = '{} ({})'.format(p2_info['project_title'], p2_info['project_id'])

    # Build the info for pdf
    results['session'] = {
        'REDCap 1': name1,
        'REDCap 2': name2,
        'DATE': datetime.now().strftime("%Y-%m-%d"),
    }

    return results


def _simplify(full_string):
    simple_string = str(full_string)

    try:
        # Coerce date format
        simple_string = datetime.strptime(
            simple_string, '%m/%d/%y').strftime('%Y-%m-%d')
    except Exception:
        simple_string = str(full_string)
        simple_string = simple_string.replace(' ', '').replace('&', 'and')
        simple_string = simple_string[0:20].lower()

    return simple_string


def compare_records(r1, r2, fields, show_one_null=False, show_two_null=True):
    mismatches = []
    misvalues = []
    match_count = 0

    for k in sorted(fields):
        # Get value from the first redcap
        try:
            v1 = r1[k]
        except KeyError:
            logging.error(f'r1:KeyError:{k}')
            continue

        # Get the value from the second
        try:
            v2 = r2[k]
        except KeyError:
            logging.error(f'r2:KeyError:{k}')
            continue

        mis = {
            'SUBJECT': r1['sid'],
            'EVENT': r1['redcap_event_name'],
            'FIELD': k,
        }

        mis['REPEAT_INSTRUMENT'] = str(r1.get('redcap_repeat_instrument', ''))
        mis['1stREPEAT_INSTANCE'] = str(r1.get('redcap_repeat_instance', ''))
        mis['2ndREPEAT_INSTANCE'] = str(r2.get('redcap_repeat_instance', ''))

        if v1 == '':
            # First blank
            if show_one_null:
                mismatches.append(mis)
        elif v2 == '':
            # First has value, Second is blank
            if show_two_null:
                misvalues.append(mis)
        else:
            c1 = v1
            c2 = v2

            if k == 'country_birth':
                c1 = c1.replace('.', '').replace('nited ', '')[0:2].lower()
                c2 = c2.replace('.', '').replace('nited ', '')[0:2].lower()
            else:
                try:
                    if float(c1) == float(c2):
                        # Set them to be exactly the same
                        c2 = c1
                except Exception:
                    # Not a float, simplifiy string
                    c1 = _simplify(c1)
                    c2 = _simplify(c2)

            # Now compare the cleaned values
            if c1 == c2:
                match_count += 1
            else:
                # Both have values, but don't match, show the truncated values
                mis['1stVALUE'] = v1[0:50]
                mis['2ndVALUE'] = v2[0:50]
                logging.debug(f'mismatch:{mis}:{c1}:{c2}')
                mismatches.append(mis)

    return (mismatches, misvalues, match_count)


def write_results(results, pdf_file, excel_file):
    # Make the summary PDF
    make_pdf(results, pdf_file)

    # Save excel file with results
    write_excel(results, excel_file)


def test_finish(outdir, outpref):
    info = {}
    excel_file = os.path.join(outdir, f'{outpref}.xlsx')
    pdf_file = os.path.join(outdir, f'{outpref}.pdf')

    info['session'] = {
        'REDCap 1': '1',
        'REDCap 2': '2',
        'DATE': datetime.now().strftime("%Y-%m-%d"),
    }

    info['counts'] = {
        'fields_compare': 0,
        'missing_events': 0,
        'missing_subjects': 0,
        'mismatches': 0,
        'missing_values': 0,
    }

    info['missing_subjects'] = []
    info['missing_events'] = []
    info['mismatches'] = []
    info['missing_values'] = []

    info['fields'] = {
        'compare': [{'FIELD': v} for v in []],
        'common': [{'FIELD': v} for v in []],
        'p1_only': [{'FIELD': v} for v in []],
        'p2_only': [{'FIELD': v} for v in []],
        'p2_nan': [{'FIELD': v} for v in []],
    }

    # Write results
    write_results(info, pdf_file, excel_file)


def run_compare(p1, p2, pdf_file, excel_file, fields=None, events=None):
    # Get the compare results
    results = compare_projects(p1, p2, fields, events)

    # Write output files
    write_results(results, pdf_file, excel_file)
