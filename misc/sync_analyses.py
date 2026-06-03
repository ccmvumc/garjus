from garjus import Garjus

# must be run on system with access to both XNAT and REDCap via garjus

# copy analyses PDF/LOG/PBS from XNAT analyses to REDCAp records

# Get analyses records from REDCap

# Iterate each, checking against XNAT

# Copy files from XNAT to REDCap as needed





print('loading analyses')
g = Garjus()

for a in g.analyses(projects=['CHAMP'], download=False):
	print(a)
