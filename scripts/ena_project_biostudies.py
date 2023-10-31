# !/usr/bin/env python3

# Copyright [2021-2023] EMBL-European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import requests, xmltodict
import argparse
import pandas as pd
from datetime import datetime, date
import sys

description = """
Description
-----
This script is required for the process of assigning DOI's to ENA projects, via the registration of a Biostudies record.

This script outputs a pagetab file for the (currently) manual creation of a biostudies record, which can then be assigned a DOI via contacting the Biostudies team.
This pagetab file/biostudies record can link to a single or multiple projects. 

"""

usage = """
Usage: ena_project_biostudies.py <ARGUMENTS>
Arguments:
    --project         : (mandatory) name of main ENA project to link to Biostudies record
    --additional_projects   : (optional) additional project accessions to link to created Biostudies entry/ single DOI (default: none)
    --author : (mandatory) author names to include
    --email : (mandatory) author email addresses
    --institution : (mandatory) institution name
    --output_file : (optional) output file name without extension. (default: timestamp)
"""

example = """
Single Project based pagetabs:
    Example: create pagetab file for a single project with one author
    
    python ena_project_biostudies.py --project PRJEB37886 --author 'Joe Bloggs' --email joe@bloggs.ac.uk --institution EBI --output_file test_May_25

Grouped Project pagetabs:
    Example: create pagetab file for a grouped biostudies record with one corresponding author
    
    mandatory arguments = -ap, -t, -d, -c, -r* (current date is taken if -r not provided)
    
    python ena_project_biostudies.py -ap PRJEB402 PRJEB1787 -t 'Grouped Project Title' -d 'this is a grouped description' -c 'new center name' -a 'Joe Bloggs' -e joe@bloggs.ac.uk -i CNN

    Example 2: create pagetab file for a grouped biostudies record with several authors:
    
    python ena_project_biostudies.py -ap PRJEB402 PRJEB1787 -t 'Grouped Project Title' -d 'this is a grouped description' -c 'new center name' -a 'Joe Bloggs' 'Jenny Smith' 'Jane Doe' -e joe@bloggs.ac.uk -i CNN BBC SkyNews
"""
parser = argparse.ArgumentParser(
    description=description+usage+example
)

#parser = argparse.ArgumentParser()
parser.add_argument('-p', '--project', help='Main ENA project accession PRJEB#### to base Biostudies entry on', type=str, nargs='*',
                    required=False)  # main project
parser.add_argument('-ap', '--additional_projects', help='Additional ENA project accessions PRJEB#### to link to created Biostudies entry', type=str, nargs='*',
                    required=False)  # additional projects
parser.add_argument('-a', '--author', help='name of author/s separated by a space', type=str, nargs='*',
                    required=False)  # takes in multiple authors
parser.add_argument('-e', '--email', help='email addresses separated by a space', type=str, nargs='*',
                    required=True)  # takes in multiple email addresses
parser.add_argument('-i', '--institution', help='each institution name to be enclosed in quotes', type=str, nargs='*',
                    required=True)  # takes in multiple institutes
parser.add_argument('-o', '--output_file', help='output file name', type=str, nargs='*',
                    required=False)  # output file


parser.add_argument('-t', '--title', help='optional project title field for grouped Biostudies entries', type=str, nargs='*',
                    required=False)  # project description for grouped biostudies entriess
parser.add_argument('-d', '--description', help='optional project description field for grouped Biostudies entries', type=str, nargs='*',
                    required=False)  # project description for grouped biostudies entriess
parser.add_argument('-c', '--center_name', help='optional center name field for grouped Biostudies entries', type=str, nargs='*',
                    required=False)  # center name for grouped biostudies entries
parser.add_argument('-r', '--release_date', help='optional release date for grouped Biostudies entries', type=str, nargs='*',
                    required=False, default=date.today().strftime("%Y-%m-%d"))  # specific release date for grouped biostudies entries, if not specified then current date taken

args = parser.parse_args()
args_dict = vars(args) #vars() to convert argparse options to a dictionary


##add some text/error to explain which arguements are required if you do not have a main project arguement

#if single project biostudies entry then required args are: -p,
#if grouped project biostudies entry then required args are: -ap, -t, -d, -c, -r* (quotes are required for anything longer than 1 word)

# retrieving project xml data (for main project only)
if args_dict['project']:

    url_start = "https://www.ebi.ac.uk/ena/browser/api/xml"
    project_acc = str(args_dict['project'][0])
    url = "{0}/{1}".format(url_start, project_acc)
    response = requests.get(url)  # get requests retrieve the webpage for display
    data = xmltodict.parse(response.content)  # this function converts the xml content into a dictionary
    project_name = (data['PROJECT_SET']['PROJECT']['NAME'])
    project_title = (data['PROJECT_SET']['PROJECT']['TITLE'])
    project_description = (data['PROJECT_SET']['PROJECT']['DESCRIPTION'])
    center_name = (data['PROJECT_SET']['PROJECT']['@center_name'])


    ## to account for release date not having a fixed index in project xml:
    for index, attribute in enumerate(data['PROJECT_SET']['PROJECT']['PROJECT_ATTRIBUTES']['PROJECT_ATTRIBUTE']):
        if attribute['TAG'] == "ENA-FIRST-PUBLIC":
            #print(index, attribute)
            release_date = (data['PROJECT_SET']['PROJECT']['PROJECT_ATTRIBUTES']['PROJECT_ATTRIBUTE'][index]['VALUE'])
            #print("release date is " + release_date)

elif args_dict['project'] is None: #and (args.title or args.description or args.center_name or args.release_date):
    project_name = str(args_dict['title'][0])
    project_title = str(args_dict['title'][0])
    project_description = str(args_dict['description'][0])
    center_name = str(args_dict['center_name'][0])
    release_date = str(args_dict['release_date'])


## creating two 'key:value' style lists
keys = ["Submission", "Title", "ReleaseDate", "", "Study", "Title", "Description", "Center Name", ""]
values = []
values.extend((None, project_name, release_date, ""))
values.extend((None, project_title, project_description, center_name, ""))


## linking ENA projects (either 1 or several) to biostudies entry
def link_projects(args):
    if args_dict['additional_projects'] is None:
        project_accession = (data['PROJECT_SET']['PROJECT']['@accession'])
        keys.extend(("Link", "Description", "Type", ""))
        values.extend((project_accession, "Raw Data", "ENA", ""))
    elif args_dict['additional_projects']:
        additional_projects = len(args_dict['additional_projects'])
        for a_project in range(additional_projects):
            keys.extend(("Link", "Description", "Type", ""))
            values.extend((args_dict['additional_projects'][a_project], "Raw Data", "ENA", ""))
link_projects(args)

#email addresses for each author not necessary.
#but at least 1 corresponding author with email address
#but each author should have an institutional affiliation
## first author details supplied to script should be corresponding author's
def corresponding_author(args): #max details for corresponding author: name, email, affiliation, institution
    print("corresponding author details:")
    keys.extend(("Author", "Name", "Email", "<affiliation>", ""))
    values.extend(("", args_dict['author'][0], args_dict['email'][0], "o1"))
    print("", "", args_dict['author'][0], args_dict['email'][0])
corresponding_author(args)


## to include multiple authors in pagetab file
authors = len(args_dict['author'])
other_authors = args_dict['author'][1:] #omitting corresponding author from author list

unique_institutions = len(set(args_dict['institution']))
unique_institutions_list = list(set(args_dict['institution']))


## equating each institution to it's own affiliation string and storing this in a list
affiliations = []
for inst in args_dict['institution']:
    affn = "o" + str(args_dict['institution'].index(inst) + 1)
    affiliations.append(affn)

other_affns = affiliations[1:] #omitting corresponding author's affiliation

## adding minimal details for remaining author list: name, affiliation
def add_author_list(args): #minimal details for author list:
    for i in range(len(other_authors)): #omitting corresponding author details
        if authors == unique_institutions:
            affiliation = "o" + str(i + 2) #'frameshifting' for corresponding author
            keys.extend(("Author", "Name", "<affiliation>", ""))  # no email necessary here
            values.extend(("", "", other_authors[i], affiliation))
        else:
            keys.extend(("Author", "Name", "<affiliation>", ""))  # no email necessary here
            values.extend(("", "", other_authors[i], other_affns[i])) #if multiple authors are from the same institution
add_author_list(args)


## adding final organisation block
def organisation_block(args):
    for name in unique_institutions_list:
        keys.extend(("Organization", "Name", ""))
        values.extend(("", "o" + str(unique_institutions_list.index(name) + 1), name)) #NOT alphabetically ordered
organisation_block(args)


## creates a dataframe from zipping tuples together
print(keys)
print(values)

keys_values = zip(keys,values)
# keys_values = list(zip(keys,values)) #list function changes formatting of organisation block above - it wrongly reorders the list of unique institutions alphabetically and thus wrongly assigns the 'o1', 'o2' etc. affiliations



df = pd.DataFrame(data=keys_values)
pd.set_option("display.max_rows", None, "display.max_columns", None)
print(df)


## create the pagetab file - note csv format not accepted!
def create_output_file():
    if args_dict['output_file']:
        filename = str(args_dict['output_file'][0])
        df.to_excel(f"{filename}.xlsx", header=None, index=False) #pagetab file output
    else:
        now = datetime.now()   # datetime object containing current date and time
        dt_string = now.strftime("pagetab_%d-%m-%y_%H_%M_%S")
        df.to_excel(f"{dt_string}.xlsx", header=None, index=False)  # pagetab file output
create_output_file()



