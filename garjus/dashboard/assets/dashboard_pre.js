let cur_stats_pivot = 'assessors';
let cur_qa_pivot = 'sessions';
let cur_stats_trace = 'all';
let selected_stats_xvariable = null;
const selected_stats_projects = [];
const selected_stats_proctypes = [];
const selected_stats_sesstypes = [];
const selected_stats_measures = [];
const selected_analyses_projects = [];
const selected_analyses_invest = [];
const selected_analyses_status = [];
const selected_processors_projects = [];
const selected_qa_projects = [];
const selected_qa_sesstypes = [];
const selected_qa_proctypes = [];
const selected_qa_scantypes = [];
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
  gridApi.autoSizeAllColumns(false);
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

function updateQASessTypeOptions() {
  const sesstypes = new Set();

  // Clear options
  select_qa_sesstypes.clearOptions();

  // Build complete set by adding sesstypes from each project
  selected_qa_projects.forEach(project => {
    (proj2sesstypes[project] || []).forEach(sesstype => {
      sesstypes.add(sesstype);
    });
  });

  // Add each option to tomselect
  sesstypes.forEach(sesstype => {
    select_qa_sesstypes.addOption({
      value: sesstype,
      text: sesstype
    });
  });
}


function updateQAProcTypeOptions() {
  const proctypes = new Set();

  // Clear options
  select_qa_proctypes.clearOptions();

  // Build complete set by adding proc types from each project
  selected_qa_projects.forEach(project => {
    (proj2procs[project] || []).forEach(proctype => {
      proctypes.add(proctype);
    });
  });

  // Add each option to tomselect
  proctypes.forEach(proctype => {
    select_qa_proctypes.addOption({
      value: proctype,
      text: proctype
    });
  });
}


function updateQAScanTypeOptions() {
  const scantypes = new Set();

  // Clear options
  select_qa_scantypes.clearOptions();

  // Build complete set by adding proc types from each project
  selected_qa_projects.forEach(project => {
    (proj2scantypes[project] || []).forEach(scantype => {
      scantypes.add(scantype);
    });
  });

  // Add each option to tomselect, first make sorted list from
  [...scantypes].sort().forEach(scantype => {
    select_qa_scantypes.addOption({
      value: scantype,
      text: scantype
    });
  });
}


function updateStatsSessTypeOptions() {
  const sesstypes = new Set();

  // Clear options
  select_stats_sesstypes.clearOptions();

  // Build complete set by adding sesstypes from each project
  selected_stats_projects.forEach(project => {
    (proj2sesstypes[project] || []).forEach(sesstype => {
      sesstypes.add(sesstype);
    });
  });

  // Add each option to tomselect
  sesstypes.forEach(sesstype => {
    select_stats_sesstypes.addOption({
      value: sesstype,
      text: sesstype
    });
  });
}


function updateStatsProcTypeOptions() {
  const proctypes = new Set();

  // Clear options
  select_stats_proctypes.clearOptions();

  // Build complete set by adding from each proc type
  selected_stats_projects.forEach(project => {
    (proj2procs[project] || []).forEach(proctype => {
      proctypes.add(proctype);
    });
  });

  // Add each option to tomselect
  proctypes.forEach(proctype => {
    select_stats_proctypes.addOption({
      value: proctype,
      text: proctype
    });
  });
}


function updateStatsMeasuresOptions() {
  const all_measures = new Set(Object.values(proc2stats).flat());
  const valid_measures = new Set();

  // Clear selections and options
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
  const valid_xvariables = new Set();

  // Clear current selections and options
  select_stats_xvariable.clearOptions();

  // Subject attributes
  select_stats_xvariable.addOption({
      value: 'AGE',
      text: 'AGE'
  });
  select_stats_xvariable.addOption({
      value: 'SEX',
      text: 'SEX'
  });
  select_stats_xvariable.addOption({
      value: 'GROUP',
      text: 'GROUP'
  });

  // Build complete set by adding from each proc type
  selected_stats_proctypes.forEach(proc => {
    (proc2stats[proc] || []).forEach(measure => {valid_xvariables.add(measure)});
  });

  // Add each measure option to tomselect
  valid_xvariables.forEach(measure => {
    select_stats_xvariable.addOption({
      value: measure,
      text: measure
    });
  });
}


function qaPivot(pivot_type){
  const rows = [];
  const col_defs = [];
  const row_map = new Map();
  const proj_cols = ['PROJECT']
  const subj_cols = ['SUBJECT', 'PROJECT', 'AGE', 'SEX', 'GROUP'];
  const sess_cols = ['SESSION', 'SUBJECT', 'PROJECT', 'DATE', 'SESSTYPE', 'SITE', 'NOTE'];
  const assr_cols = ['ASSR', 'PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'SITE', 'DATE', 
    'PROCTYPE', 'JOBDATE', 'STATUS', 'PROCSTATUS', 'QCSTATUS', 'PDF', 'LOG', 'TIMEUSED', 'MEMUSED', 'JOBNODE'];
  const scan_cols = ['SESSION', 'SUBJECT', 'PROJECT', 'SESSTYPE', 'SCANID', 'SCANTYPE', 'QUALITY', 'STATUS',
    'DATE', 'MODALITY', 'DURATION', 'TR', 'THICK', 'MB', 'FRAMES', 'NIFTI', 'JSON', 'EDAT'];

  // Row numbering column
  col_defs.push({
    headerName: "#",
    valueGetter: "node.rowIndex + 1",
    width: 20,
    sortable: false,
    filter: false,
    pinned: "left",
    suppressMovable: true,
  });

  rowData_QA.forEach(row => {
    if (selected_qa_projects.includes(row.PROJECT)) {
      rows.push(row);
    }
  });

  if (pivot_type === 'scans') {
    cur_qa_pivot = 'scans';

    // Get rows
    rows.forEach(row => {
      if (
          row.SCANID && 
          (selected_qa_scantypes.length === 0 || selected_qa_scantypes.includes(row.SCANTYPE))
        ) {
        const scankey = [row.PROJECT, row.SUBJECT, row.SESSION, row.SCANID].join("|");

        if (!row_map.has(scankey)) {
          row_map.set(scankey, {
            SESSION: row.SESSION,
            PROJECT: row.PROJECT,
            SUBJECT: row.SUBJECT,
            SESSTYPE: row.SESSTYPE,
            SCANID: row.SCANID,
            SCANTYPE: row.SCANTYPE,
            STATUS: row.STATUS,
            QUALITY: row.QUALITY,
            DATE: row.DATE,
            NOTE: row.NOTE,
            SITE: row.SITE,
            MODALITY: row.MODALITY,
            DURATION: row.DURATION,
            TR: row.TR,
            THICK: row.THICK,
            MB: row.MB,
            FRAMES: row.FRAMES,
            NIFTI: row.NIFTI,
            JSON: row.JSON,
            EDAT: row.EDAT,
            //AGE: row.AGE,
            //SEX: row.SEX,
            //GROUP: row.GROUP,
          });
        }
      }
    });

    // Get columns ordered
    scan_cols.forEach(c => {
      columnDefs_QA.forEach(column => {
        if (column['field'] === c) {
          column['hide'] = false;
          col_defs.push(column);
        }
      });
    });

  } else if (pivot_type === 'assessors') {
    cur_qa_pivot = 'assessors';

    // Get rows
    rows.forEach(row => {
      if (
          row.ASSR && 
          (selected_qa_proctypes.length === 0 || selected_qa_proctypes.includes(row.PROCTYPE))
          ) {
        let assrkey = [row.PROJECT, row.SUBJECT, row.SESSION, row.ASSR].join("|");

        if (!row_map.has(assrkey)) {
          row_map.set(assrkey, {
            PROJECT: row.PROJECT,
            SUBJECT: row.SUBJECT,
            SESSION: row.SESSION,
            ASSR: row.ASSR,
            PROCTYPE: row.PROCTYPE,
            SESSTYPE: row.SESSTYPE,
            DATE: row.DATE,
            JOBDATE: row.JOBDATE,
            PROCSTATUS: row.PROCSTATUS,
            QCSTATUS: row.QCSTATUS,
            STATUS: row.STATUS,
            SITE: row.SITE,
            TIMEUSED: row.TIMEUSED,
            MEMUSED: row.MEMUSED,
            JOBNODE: row.JOBNODE,
            LOG: row.LOG,
            PDF: row.PDF,
            //AGE: row.AGE,
            //SEX: row.SEX,
            //GROUP: row.GROUP,
          });
        }
      }
    });

    // Get columns ordered
    assr_cols.forEach(c => {
      columnDefs_QA.forEach(column => {
        if (column['field'] === c) {
          column['hide'] = false;
          col_defs.push(column);
        }
      });
    });
  } else if (pivot_type === 'sessions') {
    cur_qa_pivot = 'sessions';

    // Get rows
    rows.forEach(row => {
      let sesskey;
      sesskey = [row.PROJECT, row.SUBJECT, row.SESSION].join("|");

      if (!row_map.has(sesskey)) {
        row_map.set(sesskey, {
          PROJECT: row.PROJECT,
          SUBJECT: row.SUBJECT,
          SESSION: row.SESSION,
          DATE: row.DATE,
          SESSTYPE: row.SESSTYPE,
          SITE: row.SITE,
          NOTE: row.NOTE,
          //AGE: row.AGE,
          //SEX: row.SEX,
          //GROUP: row.GROUP,
        });
      }

      // Append scantype status
      sess = row_map.get(sesskey);
      selected_qa_scantypes.forEach(scantype => {
        if (row.SCANTYPE === scantype) {
          if (sess[scantype]) {
            //sess[scantype] += ',' + row.QUALITY;
            sess[scantype] += row.STATUS;
          } else {
            //sess[scantype] = row.QUALITY;
            sess[scantype] = row.STATUS;
          }
        }
      });

      // Append proctype status
      sess = row_map.get(sesskey);
      selected_qa_proctypes.forEach(proctype => {
        if (row.PROCTYPE === proctype) {
          if (sess[proctype]) {
            //sess[proctype] += ',' + row.PROCSTATUS + '-' + row.QCSTATUS;
            sess[proctype] += row.STATUS;
          } else {
            //sess[proctype] = row.PROCSTATUS + '-' + row.QCSTATUS;
            sess[proctype] = row.STATUS;
          }
        }
      });
    });

    // Get first columns ordered
    sess_cols.forEach(c => {
      columnDefs_QA.forEach(column => {
        if (column['field'] === c) {
          column['hide'] = false;
          col_defs.push(column);
        }
      });
    });

    // Add selected scantype columns
    selected_qa_scantypes.forEach(scantype => {
      col_defs.push({
        'field': scantype,
        'headerName': scantype,
      });
    });

    // Add selected proctpe columns
    selected_qa_proctypes.forEach(proctype => {
      col_defs.push({
        'field': proctype,
        'headerName': proctype,
      });
    });

  } else if (pivot_type === 'subjects') {
    cur_qa_pivot = 'subjects';

    rows.forEach(row => {
      let subjkey;
      subjkey = [row.PROJECT, row.SUBJECT].join("|");

      if (!row_map.has(subjkey)) {
        row_map.set(subjkey, {
          PROJECT: row.PROJECT,
          SUBJECT: row.SUBJECT,
          AGE: row.AGE,
          SEX: row.SEX,
          GROUP: row.GROUP,
        });
      }
    });

    // Get first columns ordered
    subj_cols.forEach(c => {
      columnDefs_QA.forEach(column => {
        if (column['field'] === c) {
          column['hide'] = false;
          col_defs.push(column);
        }
      });
    });

  } else if (pivot_type === 'projects') {
    cur_qa_pivot = 'projects';

    rows.forEach(row => {
      let projkey;
      projkey = row.PROJECT

      if (!row_map.has(projkey)) {
        row_map.set(projkey, {
          PROJECT: row.PROJECT,
        });
      }
    });

    // Get first columns ordered
    proj_cols.forEach(c => {
      columnDefs_QA.forEach(column => {
        if (column['field'] === c) {
          column['hide'] = false;
          col_defs.push(column);
        }
      });
    });
  }

  // Apply new data to grid
  gridApi_qa.setGridOption('rowData', Array.from(row_map.values()));
  gridApi_qa.setGridOption('columnDefs', col_defs);
  refreshGrid(gridApi_qa, 'qa');
}


function statsPivot(pivot_type){
  const rows = [];
  const col_defs = [];
  const row_map = new Map();
  const subj_cols = ['SUBJECT', 'PROJECT', 'AGE', 'SEX', 'GROUP'];
  const sess_cols = ['SESSION', 'SUBJECT', 'PROJECT', 'SESSTYPE', 'DATE', 'AGE', 'SEX', 'GROUP'];
  const assr_cols = ['ASSR', 'SESSION', 'SUBJECT', 'PROJECT', 'SESSTYPE',
    'PROCTYPE', 'DATE', 'JOBDATE', 'AGE', 'SEX', 'GROUP'];

  rowData_STATS.forEach(row => {
    if (selected_stats_proctypes.includes(row.PROCTYPE)) {
      rows.push(row);
    }
  });

  if (pivot_type === 'assessors') {
    cur_stats_pivot = 'assessors';

    // Get rows
    rows.forEach(row => {
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
          JOBDATE: row.JOBDATE,
          AGE: row.AGE,
          SEX: row.SEX,
          GROUP: row.GROUP,
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
    rows.forEach(row => {
      let sesskey;
      sesskey = [row.PROJECT, row.SUBJECT, row.SESSION].join("|");

      if (!row_map.has(sesskey)) {
        row_map.set(sesskey, {
          PROJECT: row.PROJECT,
          SUBJECT: row.SUBJECT,
          SESSION: row.SESSION,
          SESSTYPE: row.SESSTYPE,
          DATE: row.DATE,
          AGE: row.AGE,
          SEX: row.SEX,
          GROUP: row.GROUP,
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
        if (
            selected_stats_measures.length === 0 || 
            selected_stats_measures.includes(stat)
            ) {
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
    rows.forEach(row => {
      let subjkey = [row.PROJECT, row.SUBJECT].join("|");

      if (!row_map.has(subjkey)) {
        // Initialize row for subject
        row_map.set(subjkey, {
          PROJECT: row.PROJECT,
          SUBJECT: row.SUBJECT,
          AGE: row.AGE,
          SEX: row.SEX,
          GROUP: row.GROUP,
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
        if (
            selected_stats_measures.length === 0 || 
            selected_stats_measures.includes(stat)
            ) {
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
  gridApi_stats.setGridOption('rowData', Array.from(row_map.values()));
  gridApi_stats.setGridOption('columnDefs', col_defs);

  refreshGrid(gridApi_stats, 'stats');
}


function updateQAGraph() {

}


const plotly_dark_template = {
  layout: {
    paper_bgcolor: '#212529',
    plot_bgcolor: '#212529',
    font: {color: '#dee2e6'},
    xaxis: {
      gridcolor: '#495057',
      zerolinecolor: '#495057',
    },
    yaxis: {
      gridcolor: '#495057',
      zerolinecolor: '#495057',
    }
  }
};


const plotly_darkblue_template = {
  layout: {
    paper_bgcolor: '#212835',
    plot_bgcolor: '#212835',
    font: {color: '#f8fafc'},
    xaxis: {
      gridcolor: '#424853',
      zerolinecolor: '#424853',
    },
    yaxis: {
      gridcolor: '#424853',
      zerolinecolor: '#424853',
    }
  }
};


function updateStatsGraph(trace_type) {
  const graph_ident = 'graph_stats';
  const wrapper = document.getElementById(graph_ident);
  const rows = [];
  const xvar = selected_stats_xvariable;

  // clear div with subplots
  wrapper.replaceChildren();

  // load data to plot
  rowData_STATS.forEach(row => {
    if (
        selected_stats_proctypes.includes(row.PROCTYPE) &&
        (selected_stats_projects.length === 0 || selected_stats_projects.includes(row.PROJECT)) &&
        (selected_stats_sesstypes.length === 0 || selected_stats_sesstypes.includes(row.SESSTYPE))
        ) {
      // All filters applied, include row
      rows.push(row);
    }
  });

  valid_sesstypes = [...new Set(rows.map(row => row.SESSTYPE))];
  valid_proctypes = [...new Set(rows.map(row => row.PROCTYPE))];
  valid_projects = [...new Set(rows.map(row => row.PROJECT))];
  valid_sites = [...new Set(rows.map(row => row.SITE))];

  //console.log(valid_sesstypes);
  //console.log(valid_proctypes);
  //console.log(valid_projects);
  //console.log(valid_sites);

  // new plot per measure selected
  selected_stats_measures.forEach((measure, index) => {
    const plot_id = 'plotly_plot_' + index;
    const traces = [];
    const container = document.createElement('div');
    const layout_template = plotly_darkblue_template;
    const layout = {
      title: {text: measure},
      responsive: true,
      autosize: true,
      margin: {l: 50, r: 50, b: 80, t: 50, pad: 5},
      //yaxis: {title:{text: measure}},
      //xaxis: {title:{text: 'PROJECT'}},
      template: layout_template,
      boxmode: 'group',
    };

    if (trace_type === 'site') {
      valid_sites.forEach(site => {
        const trace_rows = rows.filter(row => row.SITE === site);
        const ydata = trace_rows.map(row => row[measure]);
        const tdata = trace_rows.map(row => row.SESSION);

        if (xvar) {
          const xdata = trace_rows.map(row => row[xvar]);

          // configure plot
          traces.push({
            x: xdata,
            y: ydata,
            type: 'scatter',
            mode: 'markers',
            name: site,
            title: measure,
            text: tdata,
          });

        } else {
          // configure plot
          traces.push({
            x: null,
            y: ydata,
            type: 'box',
            name: site,
            title: measure,
            text: tdata,
            boxpoints: 'all',
            boxmean: true,
          });
        }
      });
    } else if (trace_type === 'project') {
      valid_projects.forEach(project => {
        const trace_rows = rows.filter(row => row.PROJECT === project);
        const ydata = trace_rows.map(row => row[measure]);
        const tdata = trace_rows.map(row => row.SESSION);

        if (xvar) {
          const xdata = trace_rows.map(row => row[xvar]);

          // configure plot
          traces.push({
            x: xdata,
            y: ydata,
            type: 'scatter',
            mode: 'markers',
            name: project,
            title: measure,
            text: tdata,
          });

        } else {
          // configure plot
          traces.push({
            x: null,
            y: ydata,
            type: 'box',
            name: project,
            title: measure,
            text: tdata,
            boxpoints: 'all',
            boxmean: true,
          });
        }
      });
    } else if (trace_type === 'sesstype') {
      valid_sesstypes.forEach(sesstype => {
        const trace_rows = rows.filter(row => row.SESSTYPE === sesstype);
        const ydata = trace_rows.map(row => row[measure]);
        const tdata = trace_rows.map(row => row.SESSION);

        if (xvar) {
          const xdata = trace_rows.map(row => row[xvar]);

          // configure plot
          traces.push({
            x: xdata,
            y: ydata,
            type: 'scatter',
            mode: 'markers',
            name: sesstype,
            title: measure,
            text: tdata,
          });
        } else {
          // configure plot
          traces.push({
            x: null,
            y: ydata,
            type: 'box',
            name: sesstype,
            title: measure,
            text: tdata,
            boxpoints: 'all',
            boxmean: true,
          });
        }
      });
    } else {
      const ydata = rows.map(row => row[measure]);
      const tdata = rows.map(row => row.SESSION);

      if (xvar) {
        const xdata = rows.map(row => row[xvar]);

        // configure plot
        traces.push({
          x: xdata,
          y: ydata,
          type: 'scatter',
          mode: 'markers',
          name: 'All',
          title: measure,
          text: tdata,
        });
      } else {

        // configure plot
        traces.push({
          x: null,
          y: ydata,
          type: 'box',
          name: 'All',
          title: measure,
          text: tdata,
          boxpoints: 'all',
          boxmean: true,
        });
      }
    }

    // insert our new div at end of wrapper div
    container.className = 'graph-container';
    container.id = plot_id;
    wrapper.appendChild(container);

    // plot to newly added div
    Plotly.newPlot(plot_id, traces, layout, {responsive: true});
  });
}
