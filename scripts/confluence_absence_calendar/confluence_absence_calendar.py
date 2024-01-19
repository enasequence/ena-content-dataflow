#!/usr/bin/env python

import os, json, calendar, sys
from atlassian import Confluence
from getpass import getpass
import pandas as pd
from tabulate import tabulate

week_days = ['Mon', 'Tues', 'Wed', 'Thurs', 'Fri']

def get_day_list(year, month):
    day_list = []
    cal = calendar.Calendar()
    month_days = cal.monthdayscalendar(year, month)
    for week in month_days:
        for i in range(5):
            if week[i] > 0:
                day_list.append(week_days[i] + ' ' + str(week[i]))
        day_list.append('')
    if day_list[0] == '':
        day_list = day_list[1:]
    if day_list[-1] == '':
        day_list = day_list[:-1]
    return day_list

def write_header(day_list):
    row = '<tr><td class="confluenceTd"></td>'
    for day in day_list:
        row += f"<td class=\"confluenceTd\"><b>{day}</b></td>"
    row += "</tr>\n"
    return row
    
def write_person_row(person, num_cells, bhs):
    row = f"<tr><td class=\"confluenceTd\"><b>{person}</b></td>"
    for i in range(num_cells):
        row += '<td class="confluenceTd"></td>'
    row += "</tr>\n"
    return row
    
def write_team(team, day_list):
    team_html = ''
    team_html += write_header(day_list)
    for person in team:
        team_html += write_person_row(person, len(day_list), bank_hols)
    return team_html

def get_teams():
    with open('team_definitions.json', 'r') as jf:
        teams = json.load(jf)
    return teams

def write_month(year, month):
    teams = get_teams()
    day_list = get_day_list(year, month)
    month_html = "<table class=\"wrapped confluenceTable\">\n"
    for t in teams:
        month_html += write_team(t, day_list)
    month_html += "</table>\n"
    return month_html

def print_page_summary(page):
    print(f"id:    {page['id']}")
    print(f"title: {page['title']}")
    print(f"link:  {abs_cal['_links']['base']}{abs_cal['_links']['webui']}")
    print('----')

def create_new_pages_per_month(parent_page_id, year):
    month_html_header = open('html_snippets/month_header_section.html', 'r').read()
    this_month_html_header = re.sub('YYYY', str(year), month_html_header)
    month_strs = [
        'January', 'February', 'March', 'April', 'May',
        'June', 'July', 'August', 'September', 'October',
        'November', 'December'
    ]

    for x in range(12):
        month_str = month_strs[x]
        month_html = write_month(year, x+1)

        new_html = f"{this_month_html_header}\n{month_html}"
        new_title = f"{month_str} {year}"
        new_page = confluence.update_or_create(
            title=new_title, 
            body=new_html, 
            parent_id=parent_page_id
        )
        print_page_summary(new_page)

def add_year_table_to_page(page, year):
    page_html = page["body"]["storage"]["value"]
    # soup = BeautifulSoup(page_html, features="lxml")
    year_table = open('html_snippets/year_table.html', 'r').read()
    this_year_table = re.sub('YYYY', str(year), year_table)
    this_year_table = re.sub('\n\s?', '', this_year_table)

    updated_html = f"{this_year_table}{page_html}"
    updated_page = confluence.update_page(
        page_id=page['id'],
        title=page['title'],
        body=updated_html
    )
    print_page_summary(updated_page)

def get_holidays(year):
    # pull information from government website
    local_holidays = pd.json_normalize(
            pd.read_json(path_or_buf = 'https://www.gov.uk/bank-holidays.json').to_dict(),
            record_path=[['england-and-wales', 'events']]
        )[['title', 'date']].astype({
            'title': 'string',
            'date': 'datetime64[ns]'
        }
    )
    # filter to this year
    year_mask = (local_holidays['date'] >= f"{year}-01-01") & (local_holidays['date'] <= f"{year}-12-31")
    holidays_df = local_holidays[year_mask] #.rename(columns={'title':'Holiday Name', 'date':'Date'})

    # find correct symbols to represent holidays and format
    holiday_table = []
    for row in holidays_df.to_dict('records'):
        str_date = row['date'].strftime('%d %b')
        if row['title'] in ['Good Friday', 'Easter Monday']:
            holiday_table.append([str_date, row['title'], "\U0001F423"]) # chick
        elif row['title'] in ['Christmas Day', 'Boxing Day']:
            holiday_table.append([str_date, row['title'], "\U0001F384"]) # christmas tree
        else:
            holiday_table.append([str_date, row['title'], "\U0001F389"]) # party popper
    holiday_table.append([f"27-31 Dec", 'Winter Break', "\U000026C4"]) # snowman

    return holiday_table

# -------------------------- #
# get year from command line #
# -------------------------- #
try:
    year = int(sys.argv[1])
except:
    sys.stderr.write("""
Please pass the year you require.\n
Example: python confluence_absence_calendar.py 2025\n""")
    sys.exit(1)

# -------------------------------------- #
# get confluence credentials and connect #
# -------------------------------------- #
user = os.getenv('CONFLUENCE_USER')
if not user:
    user = input("Confluence username: ")
pw = os.getenv('CONFLUENCE_PASS')
if not pw:
    pw = getpass(f"Password for user '{user}': ")

confluence = Confluence(
    url='https://www.ebi.ac.uk/seqdb/confluence',
    username=user,
    password=pw
)

# --------------------------------------------- #
# get confluence page and add new months + year #
# --------------------------------------------- #
# https://www.ebi.ac.uk/seqdb/confluence/display/EMBL/Absence+Calendar
# NOTE : to get the page ID, open 'Page Information' from the '...' menu and 
# check the URL
page_id = 41225
abs_cal = confluence.get_page_by_id(page_id, expand='body.storage')

create_new_pages_per_month(page_id, year)
add_year_table_to_page(abs_cal, year)

# ------------------------------------------- #
# Print info about bank holidays that need to #
# be added to calendar manually               #
# ------------------------------------------- #
holidays = get_holidays(year)
print(f"\n\n{year} Holidays: PLEASE UPDATE PAGES MANUALLY\n")
print(tabulate(holidays, headers=['Date', 'Holiday Name', 'Symbol'], tablefmt='psql'))
