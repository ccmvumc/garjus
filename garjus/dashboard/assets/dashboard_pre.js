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
}


function hideBlankColumns(gridApi) {
  const isBlankValue = (v) => v == "" || v === null;

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
