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
import openpyxl

# 1) set the working directory (depends if tracking ASG or DToL or ERGA)

#os.chdir('c:\Data\EMBL-EBI\DToL\Assembly_tracking')
#os.chdir('c:\Data\EMBL-EBI\ASG\Assembly_tracking')
#os.chdir('c:\Data\EMBL-EBI\ERGA\Assembly_tracking')
#os.chdir('c:/Users/jasmine/Documents/ASG/Assembly tracking')

# import tracking file
tracking = pd.read_csv('tracking_file.txt', sep='\t')
last_index_row = tracking.iloc[-1]
last_index = last_index_row['index']

# import file with new accessions to add to tracking file
#data = pd.read_excel('DToL assembly tracking.xlsx', sheet_name='Releasing sequences')
data = pd.read_excel('ASG assembly tracking.xlsx', sheet_name='Releasing sequences')
#data = pd.read_excel('ERGA assembly tracking.xlsx', sheet_name='Releasing sequences')
dataset = pd.DataFrame(data, columns=['name', 'submission date', 'accessioned', 'shared to NCBI', 'project', 'analysis ID', 'sample ID', 'GCA ID', 'Contig range', 'Chr range','Assembly type'])
dataset.index = dataset.index + last_index + 1

# get taxon information
Taxon = pd.DataFrame([])
errors = pd.DataFrame([])
df_data_list = []
status_error_list = []
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
Taxon.columns = ['taxon','sample ID']
Taxon.index = Taxon.index + last_index + 1
print(Taxon)
Taxon1 = Taxon.drop(['sample ID'], axis=1)
dataset1 = dataset.join(Taxon1)
print(status_error_list)
print(dataset1)

# format file with the new accessions to add to the tracking file
dataset2 = dataset1.loc[dataset1.index.repeat(3),:].reset_index(drop=False)
dataset2.loc[(dataset2.index == 0), 'accession type'] = 'Contigs'
dataset2.loc[(dataset2.index % 3 == 0), 'accession type'] = 'Contigs'
dataset2.loc[(dataset2.index == 1), 'accession type'] = 'Chromosomes'
dataset2.loc[((dataset2.index + 2) % 3 == 0), 'accession type'] = 'Chromosomes'
dataset2.loc[(dataset2.index == 2), 'accession type'] = 'GCA'
dataset2.loc[((dataset2.index + 1) % 3 == 0), 'accession type'] = 'GCA'
dataset2['accessions'] = np.where(dataset2['accession type'].str.contains("Contigs"), dataset2['Contig range'],
                                  np.where(dataset2['accession type'].str.contains("Chromosomes"), dataset2['Chr range'],
                                           np.where(dataset2['accession type'].str.contains("GCA"), dataset2['GCA ID'], '')))
dataset2 = dataset2[dataset2['accessions'].str.len() > 0]

#add version information to the tracking file
dataset2['version'] = 1
for ind in dataset2.index:
    name = dataset2['name'][ind]
    version = name[name.rfind(".") + 1].split()[0]
    dataset2['version'][ind]= version

# to add info on tracking to the file
dataset2['Public in ENA'] = "N"
dataset2['Public in NCBI'] = "N"
dataset2['Linked to Project'] = "N"
dataset2['Linked to Sample'] = "N"
dataset2['publicly available date'] = ""
dataset2['submission date'] = pd.to_datetime(dataset2['submission date']).dt.strftime('%d/%m/%Y')
dataset2['accessioned'] = pd.to_datetime(dataset2['accessioned']).dt.strftime('%d/%m/%Y')
dataset2['shared to NCBI'] = pd.to_datetime(dataset2['shared to NCBI']).dt.strftime('%d/%m/%Y')
dataset3 = dataset2.drop(['GCA ID', 'Contig range', 'Chr range'], axis=1)

#join the new accessions with existing tracking file
frames = [tracking, dataset3]
tracking_new = pd.concat(frames, ignore_index=True)
tracking_new = tracking_new.drop(['Unnamed: 0'], axis=1)
tracking_new.to_csv('tracking_file.txt', sep="\t")
