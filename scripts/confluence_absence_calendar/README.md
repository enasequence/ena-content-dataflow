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
4. Optionally, set environment variables `CONFLUENCE_USER` and `CONFLUENCE_PASS`. If these are not set, the script will ask for them at runtime

# Running the script
Simply pass the year and the script will generate the pages directly in confluence:
```
python confluence_absence_calendar.py 2025
```
The script will also print out a table of holiday dates (incl suggested symbols), which should be manually added to each confluence page.
