# confluence_absence_calendar.py

This script is designed for the yearly [absence calendar](https://www.ebi.ac.uk/seqdb/confluence/display/EMBL/Absence+Calendar) update. It will:
- create a new page for each month of the year
- add a new table for the year to the main page (linking out to each month)
- print a table of special days/holiday to help update the calendars

# Before running
1. Install required python modules:
```
pip install -r requirements.txt
```
2. Update teams in `team_definitions.json`
3. While it's very unlikely to change, please ensure the main page's `page_id` is still `41225`. 
**This ID is hard-coded in the `confluence_absence_calendar.py` script**. To check, go to the 
[Absence Calendar](https://www.ebi.ac.uk/seqdb/confluence/display/EMBL/Absence+Calendar) page, 
click on the `...` button at the top right, and click 'Page Information'. The page ID can now
be found in the URL.
