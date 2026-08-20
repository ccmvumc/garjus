'''Text templates for creating html/javascript'''


_MARKDOWN_CELL_RENDERER_JS = r"""
function markdownCellRenderer(params) {
  const v = params.value;
  if (v === null || v === undefined) return '';
  const s = String(v);
  const m = s.match(/^\[(.*)\]\((.*)\)$/);
  if (m) {
    const a = document.createElement('a');
    a.href = m[2];
    a.target = '_blank';
    a.rel = 'noopener';
    a.textContent = m[1];
    return a;
  }
  return s;
}
"""


# Main page with multiple tabs, including javascript function and listeners
main_html_template = '''<!doctype html>
<html lang="en" data-bs-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>dashboard</title>
  <link href="https://cdn.jsdelivr.net/npm/tom-select@2.6.2/dist/css/tom-select.bootstrap5.min.css" rel="stylesheet">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/ag-grid-community/dist/ag-grid-community.min.js"></script>
  <style>
    .ag-grid {
      height: 400px;
      width: 100%;
    }
  </style>
</head>
<body>
  <div class="container-fluid p-3">
    <ul class="nav nav-tabs">TABBUTTONS</ul>
    <div class="tab-content pt-3">TABPANELS</div>
    <h1 align="center">dashboard</h1>
  </div>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/tom-select@2.6.2/dist/js/tom-select.complete.min.js"></script>
  <script>
    GRIDJS
    BUTTONJS
  </script>
</body>
</html>
'''


grid_js_template = '''
    const theme_ID = agGrid.themeQuartz.withPart(agGrid.colorSchemeDarkBlue);

    const columnDefs_ID = COLUMNS;

    const rowData_ID =  ROWS;

    const gridOptions_ID = {
      theme: theme_ID,
      columnDefs: columnDefs_ID,
      rowData: rowData_ID,
      defaultColDef: {
        sortable: true,
        resizable: true,
        filter: false,
      },
    };

    // Find the div for this grid
    const gridElement_ID = document.querySelector("#grid_ID");

    const gridApi_ID = agGrid.createGrid(gridElement_ID, gridOptions_ID);

    function onButtonExport_ID() {
      gridApi_ID.exportDataAsCsv();
    }
'''

tab_button_html_template = '''
    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#panel_ID">LABEL</button></li>
'''

tab_panel_html_template = '''
    <div class="tab-pane" id="panel_ID">
      PANEL
      <div id="grid_ID" class="ag-grid"></div>
    </div>
'''


csv_button_html_template = '''
    <button class="btn btn-primary btn-sm" onclick="onButtonExport_ID()">Export CSV</button>
'''


dropdown_js_template = '''
    const select_ID = new TomSelect("#dropdown_ID", {
      allowEmptyOption: true,
      plugins: ["remove_button", "input_autogrow"]
    });
'''


dropdown_html_template = '''
    <select id="dropdown_ID" multiple class="w-25" placeholder="Select LABEL...">
      OPTIONS
    </select>
'''


dropdown_option_html_template = '''<option value="VALUE">LABEL</option>'''


stats_js_template = '''
   select_stats_projects.on('change', function(values) {
      
      select_stats_proctypes.clear();
      
      select_stats_sesstypes.clear();

      select_stats_measures.clear();

      select_stats_xvariable.clear();


    });
'''


badge_html_template = '''
    <span id="ID" class="badge bg-secondary">0 rows</span>
'''
