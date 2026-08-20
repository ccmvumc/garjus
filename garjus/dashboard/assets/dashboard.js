const selected_stats_proctypes = [];

function hideBlankColumns(gridApi) {
  console.log('hideBlankColumns');

  const isBlankValue = (v) => v == "" || v === null;
  const cols = gridApi.getAllGridColumns().filter(c => !!c.getColDef().field);
  const fields = cols.map(c => c.getColDef().field);

  const allBlankByField = Object.fromEntries(fields.map(f => [f, true]));

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

  const next_state = fields.map(field => ({
    colId: field,
    hide: allBlankByField[field] === true
  }));

  gridApi.applyColumnState({state: next_state, applyOrder: true});
}

gridApi_stats.setGridOption(
  "isExternalFilterPresent", 
  () => selected_stats_proctypes.length > 0
);

gridApi_stats.setGridOption(
  "doesExternalFilterPass",
  params => {
    return selected_stats_proctypes.includes(params.data.PROCTYPE);
  }
);


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

  // Trigger grid to update
  gridApi_stats.onFilterChanged();

  hideBlankColumns(gridApi_stats);

});
