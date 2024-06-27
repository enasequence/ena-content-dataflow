#!/usr/bin/python3

# Copyright [2024] EMBL-European Bioinformatics Institute
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

import io
import os
import requests
import numpy as np
import pandas as pd
from pandas import json_normalize

##################
##  USER INPUT  ##
##################
# set the working directory
# check the current working directory
os.getcwd()  # should be 'C:\\Users\\USERNAME\\pathto\\githubrepo\\ena-content-dataflow' on local machine
# set thw working directory to location of scripts and of config file
os.chdir('scripts/assemblytracking/')
# set which project to track - determines the folder where tracking files will be read and written
project = 'DToL'  # DToL or ASG or ERGA

# set the location of the tracking files
tracking_files_path = f'{project}-tracking-files'
tracking_file_path = f'{tracking_files_path}/tracking_file.txt'
# TODO: use argparse function

###################
##  FILE INPUTS  ##
###################
# (optional - from initial import script) to  import list of public assemblies from another source in excel format
#data  =  pd.read_excel('ERGA_Publicly_available_04Jan23.xlsx', sheet_name='Publicly available')
# rename column names to 'dataset' format
#dataset = pd.DataFrame(data, columns=['name', 'submission date', 'accessioned', 'shared to NCBI', 'project', 'analysis ID', 'sample ID', 'GCA ID', 'Contig range', 'Chr range','Assembly type'])

# import tracking file
tracking = pd.read_csv(tracking_file_path, sep='\t',index_col=0)

# import file with new accessions to add to tracking file
data = pd.read_excel(f'{tracking_files_path}/{project} assembly tracking.xlsx', sheet_name='Releasing sequences')

#############
##  MAIN   ##
#############
# Purpose of script - adds new assemblies to the tracking_file.txt in the format used for logging and tracking of
# the progress of the assemblies release.
#TODO: tidy this script into functions:
# get Taxon info and report API errors.
# add to tracking file and indexing - look at improving indexing.




# get index of the latest row in the tracking file and increment this index onto new assemblies to be added to tracking
last_index = tracking['index'].iloc[-1]
# subset input data by slicing dataframe
dataset = data.iloc[:, 0:11] #dataset = data.loc[:,'name':'Assembly type'] #this also works
#apply index to new accessions dataset
dataset.index = dataset.index + last_index + 1


# get taxon information
Taxon = pd.DataFrame()
errors = pd.DataFrame()
df_data_list, status_error_list = [], []
for ind in dataset.index:
    sample = dataset['sample ID'][ind]
    value = {'format': 'json', 'fields': 'scientific_name', 'includeAccessions': sample, 'result': 'sample'}
    r = requests.get('https://www.ebi.ac.uk/ena/portal/api/search', value)
    if r.status_code == 200:
        json_data = r.json()
        df_data = json_normalize(json_data)
        df_data_list.append(df_data)
    else:
        status_code = io.StringIO(str(r.status_code))
        status_error = pd.read_csv(status_code, names=['status code'])
        status_error['accession'] = sample
        status_error_list.append(status_error)
Taxon = pd.concat(df_data_list, ignore_index=True)
# subset columns
Taxon.columns = ['taxon','sample ID']
Taxon.index = Taxon.index + last_index + 1

#initial import version (commented out)
#Taxon.columns = ['sample ID', 'tax_id', 'taxon']
#Taxon1 = Taxon.drop(['sample ID', 'tax_id'], axis=1)

Taxon1 = Taxon.drop(['sample ID'], axis=1)
dataset1 = dataset.join(Taxon1)
print(status_error_list)

# to import tracking file and duplicate lines for each data type
# format file with the new accessions to add to the tracking file
dataset2 = dataset1.loc[dataset1.index.repeat(3),:].reset_index(drop=False)
dataset2.loc[(dataset2.index == 0), 'accession type'] = 'Contigs'
dataset2.loc[(dataset2.index % 3 == 0), 'accession type'] = 'Contigs'
dataset2.loc[(dataset2.index == 1), 'accession type'] = 'Chromosomes'
dataset2.loc[((dataset2.index + 2) % 3 == 0), 'accession type'] = 'Chromosomes'
dataset2.loc[(dataset2.index == 2), 'accession type'] = 'GCA'
dataset2.loc[((dataset2.index + 1) % 3 == 0), 'accession type'] = 'GCA'
# to add column with data type
dataset2['accessions'] = np.where(dataset2['accession type'].str.contains("Contigs"), dataset2['Contig range'],
                                  np.where(dataset2['accession type'].str.contains("Chromosomes"), dataset2['Chr range'],
                                           np.where(dataset2['accession type'].str.contains("GCA"), dataset2['GCA ID'], '')))
# to remove lines with blanks for Chromosomes or Contigs
dataset2 = dataset2[dataset2['accessions'].str.len() > 0]
#add version information to the tracking file
dataset2['version'] = 1
for ind in dataset2.index:
    name = dataset2['name'][ind]
    version = name[name.rfind(".") + 1].split()[0]
    dataset2['version'][ind]= version

# to add info on tracking to the file (use Y for all options here with initial import)
dataset2['Public in ENA'] = "N"
dataset2['Public in NCBI'] = "N"
dataset2['Linked to Project'] = "N"
dataset2['Linked to Sample'] = "N"
dataset2['publicly available date'] = ""
# convert columns from string to datetime (step1 only)
dataset2['submission date'] = pd.to_datetime(dataset2['submission date']).dt.strftime('%d/%m/%Y')
dataset2['accessioned'] = pd.to_datetime(dataset2['accessioned']).dt.strftime('%d/%m/%Y')
dataset2['shared to NCBI'] = pd.to_datetime(dataset2['shared to NCBI']).dt.strftime('%d/%m/%Y')
dataset3 = dataset2.drop(['GCA ID', 'Contig range', 'Chr range'], axis=1)

####################
##  FILE OUTPUTS  ##
####################
# (optional - from initial import script) save new dataset with taxon information added
#dataset3.to_csv('tracking_file.txt', sep="\t")

#join the new accessions with existing tracking file
frames = [tracking, dataset3]
tracking_new = pd.concat(frames, ignore_index=True)
#tracking_new = tracking_new.drop(['Unnamed: 0'], axis=1) #not needed...

tracking_new.to_csv(tracking_file_path, sep="\t")

