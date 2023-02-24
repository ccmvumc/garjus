"""

Garjus Issues Management

"""

def delete_old_issues(garjus):
	pass

def update(garjus):
	pass

def matching_issues(issue1, issue2):
	# Matching means both issues are of the same Type
    # on the same Project/Subject
    # and as applicable, the same XNAT Session/Scan
    # and as applicable the same REDCap Event/Field
    keys = [
        'main_name', 'issue_type', 'issue_subject',
        'issue_session', 'issue_scan', 'issue_event', 'issue_field']

    for k in keys:
        if (k in issue1) and (issue1[k] != issue2[k]):
            return False

    return True

