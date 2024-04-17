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

# 1) set the working directory (depends if tracking ASG or DToL or ERGA)

#os.chdir('c:\Data\EMBL-EBI\DToL\Assembly_tracking')
#os.chdir('c:\Data\EMBL-EBI\ASG\Assembly_tracking')
#os.chdir('c:\Data\EMBL-EBI\ERGA\Assembly_tracking')
#os.chdir('c:/Users/jasmine/Documents/ASG/Assembly tracking')

# to import list of values to query from excel file
#data = pd.read_excel('ERGA_Publicly_available_04Jan23.xlsx', sheet_name='Publicly available')
dataset = pd.DataFrame(data, columns=['name', 'submission date', 'accessioned', 'shared to NCBI', 'project', 'analysis ID', 'sample ID', 'GCA ID', 'Contig range', 'Chr range','Assembly type'])
print(dataset)

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
Taxon.columns = ['sample ID', 'tax_id', 'taxon']
Taxon1 = Taxon.drop(['sample ID', 'tax_id'], axis=1)
dataset1 = dataset.join(Taxon1)
print(status_error_list)

# to import tracking file and duplicate lines for each data type
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
# to add info on tracking to the file
dataset2['Public in ENA'] = "Y"
dataset2['Public in NCBI'] = "Y"
dataset2['Linked to Project'] = "Y"
dataset2['Linked to Sample'] = "Y"
dataset2['publicly available date'] = ""
dataset3 = dataset2.drop(['GCA ID', 'Contig range', 'Chr range'], axis=1)
#   df_data.index = pd.RangeIndex(index)
dataset3.to_csv('tracking_file.txt', sep="\t")