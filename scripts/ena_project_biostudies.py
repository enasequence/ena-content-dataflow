# !/usr/bin/env python3

# Copyright [2021] EMBL-European Bioinformatics Institute
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
from datetime import datetime


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
Example: create pagetab file for a single project with one author
    ena_project_biostudies.py --project PRJEB37886 --author 'Zahra Waheed' --email zahra@ebi.ac.uk --institution EBI --output_file test_May_25

"""
parser = argparse.ArgumentParser(
    description=description+usage+example
)

#parser = argparse.ArgumentParser()
parser.add_argument('-p', '--project', help='Main ENA project accession PRJEB#### to base Biostudies entry on', type=str, nargs='*',
                    required=True)  # main project
parser.add_argument('-ap', '--additional_projects', help='Additional ENA project accessions PRJEB#### to link to created Biostudies entry', type=str, nargs='*',
                    required=False)  # additional projects
parser.add_argument('-a', '--author', help='name of author/s separated by a space', type=str, nargs='*',
                    required=True)  # takes in multiple authors
parser.add_argument('-e', '--email', help='email addresses separated by a space', type=str, nargs='*',
                    required=False)  # takes in multiple email addresses
parser.add_argument('-i', '--institution', help='each institution name to be enclosed in quotes', type=str, nargs='*',
                    required=True)  # takes in multiple institutes
parser.add_argument('-o', '--output_file', help='output file name', type=str, nargs='*',
                    required=False)  # output file
args = parser.parse_args()


## retrieving project xml data (for main project only)
url_start = "https://www.ebi.ac.uk/ena/browser/api/xml"
project_acc = str(args.project[0])
url = "{0}/{1}".format(url_start, project_acc)
response = requests.get(url)  # get requests retrieve the webpage for display
data = xmltodict.parse(response.content)  # this function converts the xml content into a dictionary

## project xml attributes:
project_name = (data['PROJECT_SET']['PROJECT']['NAME'])

## to account for release date not having a fixed index in project xml:
for index, attribute in enumerate(data['PROJECT_SET']['PROJECT']['PROJECT_ATTRIBUTES']['PROJECT_ATTRIBUTE']):
    if attribute['TAG'] == "ENA-FIRST-PUBLIC" :
        #print(index, attribute)
        release_date = (data['PROJECT_SET']['PROJECT']['PROJECT_ATTRIBUTES']['PROJECT_ATTRIBUTE'][index]['VALUE'])
        print("release date is " +  release_date)



project_title = (data['PROJECT_SET']['PROJECT']['TITLE'])
project_description = (data['PROJECT_SET']['PROJECT']['DESCRIPTION'])
center_name = (data['PROJECT_SET']['PROJECT']['@center_name'])
project_accession = (data['PROJECT_SET']['PROJECT']['@accession'])


## creating two 'key:value' style lists
keys = ["Submission", "Title", "ReleaseDate", "", "Study", "Title", "Description", "Center Name", "", "Link", "Description", "Type", ""]
values = []
values.extend((None, project_name, release_date, ""))
values.extend((None, project_title, project_description, center_name, ""))
values.extend((project_accession, "Raw Data", "ENA", ""))


## to link multiple projects to main biostudies entry
if args.additional_projects != None:

    additional_projects = len(args.additional_projects)

    def link_additional_projects(args): #not a required arguement
        for a_project in range(additional_projects):
            keys.extend(("Link", "Description", "Type", ""))
            values.extend((args.additional_projects[a_project], "Raw Data", "ENA", ""))
    link_additional_projects(args)

keys.extend(("Link", "Type"))
values.extend(("<insert DOI here>", "DOI")) #this will be manually added by biostudies team


## to include multiple authors in pagetab file
authors = len(args.author)
unique_institutions = len(set(args.institution))


def create_author_entries(args):
    print()
    print("adding author details below:")
    for i in range(authors):
        keys.extend(("", "Author", "Name", "Email", "<affiliation>", ""))
        values.extend(("", "", args.author[i], args.email[i]))
        print("", args.author[i], args.email[i])
        if authors == unique_institutions:
            affiliation = "o" + str(i + 1)
            values.extend((affiliation, "")) #value for author 'affiliation' key
            keys.extend(("Organization", "Name"))
            values.extend((affiliation, args.institution[i]))
            print(affiliation, args.institution[i])
            ###TODO: how to print organization blocks if some authors are from the same institution?
create_author_entries(args)


## creates a dataframe from zipping tuples together

keys_values = list(zip(keys,values))
print("Keys and values are:", keys_values)


df = pd.DataFrame(data=keys_values)
pd.set_option("display.max_rows", None, "display.max_columns", None)
#print(df)


## create the pagetab file - note csv format not accepted!
def create_output_file():
    if args.output_file:
        filename = str(args.output_file[0])
        df.to_excel(f"{filename}.xlsx", header=None, index=False) #pagetab file output
    else:
        now = datetime.now()   # datetime object containing current date and time
        dt_string = now.strftime("pagetab_%d-%m-%y_%H_%M_%S")
        df.to_excel(f"{dt_string}.xlsx", header=None, index=False)  # pagetab file output
create_output_file()

