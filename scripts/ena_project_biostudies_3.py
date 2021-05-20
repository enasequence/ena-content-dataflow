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


## retrieving project xml data (for main project only)
####TODO: accept project as an argument#####
import requests, xmltodict
url_start = "https://www.ebi.ac.uk/ena/browser/api/xml"
project_acc = "PRJEB39337" #main project accession
url = "{0}/{1}".format(url_start, project_acc)
response = requests.get(url) #get requests retrieve the webpage for display
data = xmltodict.parse(response.content) #this function converts the xml content into a dictionary
#print(data)

## project xml attributes:
import datetime
project_name = (data['PROJECT_SET']['PROJECT']['NAME'])

## to account for release date not having a fixed index in project xml:
for index, attribute in enumerate(data['PROJECT_SET']['PROJECT']['PROJECT_ATTRIBUTES']['PROJECT_ATTRIBUTE']):
    if attribute['TAG'] == "ENA-FIRST-PUBLIC" :
        #print(index, attribute)
        release_date = (data['PROJECT_SET']['PROJECT']['PROJECT_ATTRIBUTES']['PROJECT_ATTRIBUTE'][index]['VALUE'])
        print("release date is " +  release_date)

#release_date = (data['PROJECT_SET']['PROJECT']['PROJECT_ATTRIBUTES']['PROJECT_ATTRIBUTE'][0]['VALUE']) #first public date does not have a fixed index
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


import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-ap', '--additional_projects', help='Additional ENA project accessions PRJEB#### to link to created Biostudies entry', type=str, nargs='*',
                    required=False)  # additional projects
parser.add_argument('-a', '--author', help='name of author/s separated by a space', type=str, nargs='*',
                    required=True)  # takes in multiple authors
parser.add_argument('-e', '--email', help='email addresses separated by a space', type=str, nargs='*',
                    required=True)  # takes in multiple email addresses
parser.add_argument('-i', '--institution', help='each institution name to be enclosed in quotes', type=str, nargs='*',
                    required=True)  # takes in multiple institutes
args = parser.parse_args()

# print(args.author[1]) # how to specify which specific author or email address

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

# e.g bob, dylan, jameela
# bob@ebi.ac.uk, o2, EBI
# dylan@ebi.ac.uk, o3, NCBI
# jameela@uit.ac.uk , o4, uit

authors = len(args.author)
unique_institutions = len(set(args.institution))

# print(vars(args))
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
    ##check if args.institution contains duplicate values
    ##if yes, use the length of unique_institutions (e.g, 3) and do str(u+1) for each institution and attribute it to the corresponding author
    #organisation block#
#    for u in range(unique_institutions):
#        keys.extend(("Organization", "Name", ""))
#        values.extend(("o" + str(u + 1), args.institution[u], "")) # this is breaking the script and giving a TypeError: list indices must be integers or slices, not str


## creates a dataframe from zipping tuples together

keys_values = list(zip(keys,values))
print("Keys and values are:", keys_values)

import pandas as pd
df = pd.DataFrame(data=keys_values)
pd.set_option("display.max_rows", None, "display.max_columns", None)
#print()
#print(df)


## create the pagetab file - note csv format NOT accepted!
df.to_excel('test20May.xlsx', header=None, index=False) #pagetab file output
