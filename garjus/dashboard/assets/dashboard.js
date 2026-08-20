const selected_stats_projects = [];
const selected_stats_proctypes = [];
const selected_stats_sesstypes = [];
const selected_stats_measures = [];
const selected_stats_xvariable = [];


function updateRowCounts(gridApi, ident) {
  const total_rows = gridApi.getDisplayedRowCount();
  //console.log(total_rows);
  document.getElementById(ident).textContent = total_rows + " rows";
}


function refreshGrid(gridApi, ident) {
  gridApi.onFilterChanged();
  hideBlankColumns(gridApi);
  gridApi.autoSizeAllColumns();
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


// Set function to check if there is an external filter to be applied
gridApi_stats.setGridOption(
  "isExternalFilterPresent", 
  () => selected_stats_proctypes.length > 0
);


// Set external filter function
gridApi_stats.setGridOption(
  "doesExternalFilterPass",
  params => {
    return (
      (selected_stats_projects.length === 0 || selected_stats_projects.includes(params.data.PROJECT)) &&
      (selected_stats_proctypes.length === 0 || selected_stats_proctypes.includes(params.data.PROCTYPE)) && 
      (selected_stats_sesstypes.length === 0 || selected_stats_sesstypes.includes(params.data.SESSTYPE)) 
    );
  }
);


// When stats projects changes, clear other dropdowns
select_stats_projects.on('change', function(values) {
  // Clear other dropdowns
  select_stats_proctypes.clear();
  select_stats_sesstypes.clear();
  select_stats_measures.clear();
  select_stats_xvariable.clear();

});


// When stats proctypes changes, filter rows in grid
select_stats_proctypes.on("change", function(values) {

  // Clear it
  selected_stats_proctypes.length = 0;

  // Set values
  selected_stats_proctypes.push(...values);

  // Trigger grid updates
  refreshGrid(gridApi_stats, "stats");
});


// When stats proctypes changes, filter rows in grid
select_stats_projects.on("change", function(values) {

  // Clear it
  selected_stats_projects.length = 0;

  // Set values
  selected_stats_projects.push(...values);

  // Trigger grid updates
  refreshGrid(gridApi_stats, "stats");
});


// When stats sesstype changes, filter rows in grid
select_stats_sesstypes.on("change", function(values) {

  // Clear it
  selected_stats_sesstypes.length = 0;

  // Set values
  selected_stats_sesstypes.push(...values);

  // Trigger grid update
  refreshGrid(gridApi_stats, "stats");

  // Set available options in Measures and Xvariable

});




