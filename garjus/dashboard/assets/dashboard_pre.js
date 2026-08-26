let cur_stats_pivot = 'assessors';
const selected_stats_projects = [];
const selected_stats_proctypes = [];
const selected_stats_sesstypes = [];
const selected_stats_measures = [];
const selected_stats_xvariable = [];
const selected_analyses_projects = [];
const selected_analyses_invest = [];
const selected_analyses_status = [];
const selected_processors_projects = [];
const selected_qa_projects = [];
const selected_qa_sesstypes = [];
const grid_instances = {
  'stats': {api: null, resized: false},
  'analyses': {api: null, resized: false},
  'processors': {api: null, resized: false},
  'qa': {api: null, resized: false}
};


function updateRowCounts(gridApi, ident) {
  const total_rows = gridApi.getDisplayedRowCount();
  document.getElementById(ident).textContent = total_rows + " rows";
}


function refreshGrid(gridApi, ident) {
  gridApi.onFilterChanged();
  updateRowCounts(gridApi, ident + "_rowcount");
  gridApi.autoSizeAllColumns();
}


function hideBlankColumns(gridApi) {
  const isBlankValue = (v) => v == "" || v === null || v === "Invalid Number";

  // Get columns
  const cols = gridApi.getAllGridColumns().filter(c => !!c.getColDef().field);

  // Get fields from columns
  const fields = cols.map(c => c.getColDef().field);

  // Initialize results
  const allBlankByField = Object.fromEntries(fields.map(f => [f, true]));

  // Check every cell for blank and track blank by field
  const displayedRowNodes = [];
  gridApi.forEachNodeAfterFilterAndSort(node => displayedRowNodes.push(node));
  for (const node of displayedRowNodes) {
    const row = node.data || {};

    for (const f of fields) {
      if (allBlankByField[f] && !isBlankValue(row[f])) {
        allBlankByField[f] = false;
      }
    }
  }

  // Get hide or show by field as col def
  const next_state = fields.map(field => ({
    colId: field,
    hide: allBlankByField[field] === true
  }));

  // Apply col def update
  gridApi.applyColumnState({state: next_state, applyOrder: true});
}


function updateStatsMeasuresOptions() {
  const all_measures = new Set(Object.values(proc2stats).flat());
  const valid_measures = new Set();

  // Clear selections and options
  select_stats_measures.clear();
  select_stats_measures.clearOptions();

  // Build complete set of measures by adding from each proc type
  selected_stats_proctypes.forEach(proc => {
    (proc2stats[proc] || []).forEach(measure => {valid_measures.add(measure)});
  });

  // Add each measure option to tomselect
  valid_measures.forEach(measure => {
    select_stats_measures.addOption({
      value: measure,
      text: measure
    });
  });
}


function updateStatsXvariableOptions() {
  // Clear current selections and options
  select_stats_xvariable.clear();
  select_stats_xvariable.clearOptions();

  // Set xvariable options to match currently selected measures
  selected_stats_measures.forEach(measure => {
    select_stats_xvariable.addOption({
      value: measure,
      text: measure
    });
  });
}


function qaPivot(pivot_type){
  const all_rows = [];
  const filtered_rows = [];
  const col_defs = [];
  const row_data = [];
  const row_map = new Map();

  gridApi_qa.forEachNode(node => {
    all_rows.push(node.data);
  });

  gridApi_qa.forEachNodeAfterFilter(node => {
    filtered_rows.push(node.data);
  });

  if (pivot_type === 'scans') {
  } else if (pivot_type === 'assessors') {
  } else if (pivot_type === 'sessions') {
    // Show all sessions after applying project and session type filter
    // Apply other filters before creating new columns. how?
    filtered_rows.forEach(row => {
      let sesskey;
      sesskey = [row.PROJECT, row.SUBJECT, row.SESSION].join("|");
    });
  } else if (pivot_type === 'subjects') {
    filtered_rows.forEach(row => {
      let subjkey;
      subjkey = [row.PROJECT, row.SUBJECT, row.SESSION].join("|");
    });
  } else if (pivot_type === 'projects') {
  }

  // Apply new data to grid
  gridApi_qa.setGridOption('rowData', row_data);
  gridApi_qa.setGridOption('columnDefs', col_defs);
}


function statsPivot(pivot_type){
  const filtered_rows = [];
  const col_defs = [];
  const row_map = new Map();
  const subj_cols = ['SUBJECT', 'PROJECT'];
  const sess_cols = ['SESSION', 'SUBJECT', 'PROJECT', 'SESSTYPE', 'DATE'];
  const assr_cols = ['ASSR', 'SESSION', 'SUBJECT', 'PROJECT', 'SESSTYPE', 'PROCTYPE', 'DATE', 'JOBDATE'];

  rowData_STATS.forEach(row => {
    if (selected_stats_proctypes.includes(row.PROCTYPE)) {
      filtered_rows.push(row);
    }
  });

  if (pivot_type === 'assessors') {
    cur_stats_pivot = 'assessors';

    // Get rows
    filtered_rows.forEach(row => {
      let assrkey;
      assrkey = [row.PROJECT, row.SUBJECT, row.SESSION, row.ASSR].join("|");

      if (!row_map.has(assrkey)) {
        row_map.set(assrkey, {
          PROJECT: row.PROJECT,
          SUBJECT: row.SUBJECT,
          SESSION: row.SESSION,
          ASSR: row.ASSR,
          PROCTYPE: row.PROCTYPE,
          SESSTYPE: row.SESSTYPE,
          DATE: row.DATE,
          //JOBDATE: row.JOBDATE,
        });
 
        // Get proctype stats for row
        assr = row_map.get(assrkey);
        proc2stats[row.PROCTYPE].forEach(stat => {
          if (selected_stats_measures.length === 0 || selected_stats_measures.includes(stat)) {
            assr[stat] = row[stat];
          }
        });
      }
    });

    // Get first columns ordered
    assr_cols.forEach(assr => {
      columnDefs_STATS.forEach(column => {
        if (column['field'] === assr) {
          column['hide'] = false;
          col_defs.push(column);
        }
      });
    });

    // Get proctype columns
    selected_stats_proctypes.forEach(proctype => {
      proc2stats[proctype].forEach(stat => {
        if (selected_stats_measures.length === 0 || selected_stats_measures.includes(stat)) {
          columnDefs_STATS.forEach(column => {
            if (column['field'] === stat) {
              column['hide'] = false;
              col_defs.push(column);
            }
          });
        }
      });
    });
  } else if (pivot_type === 'sessions') {
    // Show all sessions after applying project and session type filter
    // Apply other filters before creating new columns. how?
    cur_stats_pivot = 'sessions';
    filtered_rows.forEach(row => {
      let sesskey;
      sesskey = [row.PROJECT, row.SUBJECT, row.SESSION].join("|");

      if (!row_map.has(sesskey)) {
        row_map.set(sesskey, {
          PROJECT: row.PROJECT,
          SUBJECT: row.SUBJECT,
          SESSION: row.SESSION,
          SESSTYPE: row.SESSTYPE,
          DATE: row.DATE,
        }); 
      }

      // Get proctype stats for row
      sess = row_map.get(sesskey);
      selected_stats_proctypes.forEach(proctype => {
        proc2stats[row.PROCTYPE].forEach(stat => {
          sess[stat] = row[stat];
        });
      });
    });

    columnDefs_STATS.forEach(column => {
      if (sess_cols.includes(column['field'])) {
        column['hide'] = false;
        col_defs.push(column);
      }
    });

    // Get proctype columns
    selected_stats_proctypes.forEach(proctype => {
      proc2stats[proctype].forEach(stat => {
        if (selected_stats_measures.length === 0 || selected_stats_measures.includes(stat)) {
          columnDefs_STATS.forEach(column => {
            if (column['field'] === stat) {
              column['hide'] = false;
              col_defs.push(column);
            }
          });
        }
      });
    });
  } else if (pivot_type === 'subjects') {
    cur_stats_pivot = 'subjects';
    filtered_rows.forEach(row => {
      let subjkey = [row.PROJECT, row.SUBJECT].join("|");

      if (!row_map.has(subjkey)) {
        // Initialize row for subject
        row_map.set(subjkey, {
          PROJECT: row.PROJECT,
          SUBJECT: row.SUBJECT
        });
      }

      // Get proctype stats for row
      subj = row_map.get(subjkey);
      selected_stats_proctypes.forEach(proctype => {
        proc2stats[row.PROCTYPE].forEach(stat => {
          subj[stat] = row[stat];
        });
      });
    });

    // TODO: prepend session type to DATE?
    columnDefs_STATS.forEach(column => {
      if (subj_cols.includes(column['field'])) {
        column['hide'] = false;
        col_defs.push(column);
      }
    });

    // Get proctype columns
    // TODO: prepend session type to field and header name
    selected_stats_proctypes.forEach(proctype => {
      proc2stats[proctype].forEach(stat => {
        if (selected_stats_measures.length === 0 || selected_stats_measures.includes(stat)) {
          columnDefs_STATS.forEach(column => {
            if (column['field'] === stat) {
              column['hide'] = false;
              col_defs.push(column);
            }
          });
        }
      });
    });
  }

  // Apply new data to grid
  const row_data = Array.from(row_map.values());
  gridApi_stats.setGridOption('rowData', row_data);
  gridApi_stats.setGridOption('columnDefs', col_defs);

  refreshGrid(gridApi_stats, 'stats');
}
