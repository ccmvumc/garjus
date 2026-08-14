'''Text templates for creating html/javascript'''


# Main page with multiple tabs
main_html_template = '''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/ag-grid-community/dist/ag-grid-community.min.js"></script>
  <style>
    html, body { height: 100%; margin: 0; }
    .wrap { display: flex; flex-direction: column; }
    .tabs { display: flex; gap: 8px; padding: 12px; border-bottom: 1px solid #ddd; }
    .tab { padding: 8px 12px; border: 1px solid #ccc; background: #f7f7f7; cursor: pointer; }
    .content { flex: 1; padding: 12px; }
    .hidden { display: none; }
    .gridWrap { height: 100%; }
    .tabbutton { color: #888; }
    .tabbutton.active { color: #e8f0fe; }
    .tabpanel { display: none; }
    .tabpanel.active { display: block; }
  </style>
</head>
<body>
  <h1>garjus dashboard</h1>
  <div class="wrap">
    <div class="tabs">  
      TABBUTTONS
    </div>
  </div>
  <div class="content gridWrap">
      TABPANELS
  </div>
  <script>
    TABJS
    function showTab(tabId) {
      document.querySelectorAll(".tabbutton").forEach(b => b.classList.toggle("active", b.dataset.tab === tabId));
      document.querySelectorAll(".tabpanel").forEach(p => p.classList.toggle("active", p.id === "panel-" + tabId));
    }
    document.querySelectorAll(".tabbutton").forEach(b => {
      b.addEventListener("click", () => showTab(b.dataset.tab));
    });
    document.addEventListener("DOMContentLoaded", () => {
      showTab("stats");
    });
    showTab("scans");
  </script>
</body>
</html>
'''


grid_js_template = '''
    const themeID = agGrid.themeAlpine.withPart(agGrid.colorSchemeDark);

    const columnDefsID = COLUMNS;

    const rowDataID =  ROWS;

    const gridOptionsID = {
      theme: themeID,
      columnDefs: columnDefsID,
      rowData: rowDataID,
      defaultColDef: {
        sortable: true,
        filter: true,
        resizeable: true
      }
    };

    document.addEventListener("DOMContentLoaded", () => {
      const gridElementID = document.querySelector("#gridID");
      agGrid.createGrid(gridElementID, gridOptionsID);
    });
'''


tab_button_html_template = '''
    <button class="tabbutton" data-tab="ID">LABEL<span class="cnt">(COUNT)</span></button>
'''


tab_panel_html_template = '''
    <div class="tabpanel" id="panel-ID">
      <div id="gridID" style="height:600px; width:100%;"></div>
    </div>
'''
