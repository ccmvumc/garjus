# Set up REDCap for garjus

### Create a new Garjus main REDCap project

  - upload from zip (see misc folder)
  - click user rights, enable API export/import, save changes
  - refresh, click API, click Generate API Token, click Copy
  - go to ~/.redcap.txt
  - paste key, copy & paste PID from gui, name it "main"

### Create first stats REDCap project

  - upload from zip (see misc folder)
  - click user rights, check enable API export/import, click save changes
  - Refresh page, click API, click Generate API Token, click Copy
  - Go to ~/.redcap.txt
  - Paste key, copy & paste ID, name
  - Paste ID into ccmutils under Main > Project Stats

### Create additional stats REDCap projects

  - Copy an existing project in gui under Other Functionality, click Copy Project
  - Change the project name
  - Confirm checkboxes for Users, Folder
  - Click Copy Project (should take you to new project)
  - In the new project, click user rights, check enable API export/import, click save changes
  - Refresh page, click API, click Generate API Token, click Copy
  - Go to ~/.redcap.txt
  - Paste key, copy & paste ID, name main
  - Paste ID into ccmutils under Main > Project Stats
  - OPTIONAL: Paste the key into ID and skip .redcap.txt


### Add a new primary REDCap project to link individual study to garjus:
  
  - Copy PID, key to ~/.redcap.txt, name PROJECT primary
  - paste ID into ccmutils under Main > Project Primary


### Add a new secondary REDCap project for double entry comparison:
  
  - Copy PID, key to ~/.redcap.txt, name PROJECT secondary 
  - paste ID into ccmutils under Main > Project Secondary
