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
  if (pivot_type === 'scans') {
    console.log('qa pivot to scans')    
  } else if (pivot_type === 'assessors') {
    console.log('qa pivot to assessors')
  } else if (pivot_type === 'sessions') {
    console.log('qa pivot to sessions')
  } else if (pivot_type === 'subjects') {
    console.log('qa pivot to subjects')
  } else if (pivot_type === 'projects') {
    console.log('qa pivot to projects')
  }
}


function statsPivot(pivot_type){
  if (pivot_type === 'assessors') {
    console.log('stats pivot to assessors')
  } else if (pivot_type === 'sessions') {
    console.log('stats pivot to sessions')
  } else if (pivot_type === 'subjects') {
    console.log('stats pivot to subjects')
  }
}
