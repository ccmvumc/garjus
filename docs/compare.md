# compare

The compare sub-command provides double-entry comparison between two REDCap projects. 

Each study project is configured in the garjus REDCap to have a primary REDCap project and a secondary REDCap project.

Typically, we create the double database by copying the primary database and deleting any instruments/fields that do not require double entry.

When garjus update runs, it will make sure there is a double entry report for the current month.