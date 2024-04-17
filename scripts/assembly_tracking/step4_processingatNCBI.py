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

# import packages
import io
import os
import requests
import numpy as np
import pandas as pd
from pandas import json_normalize
from xml.etree import ElementTree

# 1) set the working directory (depends if tracking ASG or DToL or ERGA)

#os.chdir('c:\Data\EMBL-EBI\DToL\Assembly_tracking')
#os.chdir('c:\Data\EMBL-EBI\ASG\Assembly_tracking')
#os.chdir('c:\Data\EMBL-EBI\ERGA\Assembly_tracking')
#os.chdir('c:/Users/jasmine/Documents/ASG/Assembly tracking')


# import tracking file
tracking = pd.read_csv('tracking_file.txt', sep='\t')
tracking = tracking.drop(['Unnamed: 0'], axis=1)

# create sub dataframe with accessions not public at NCBI
dataset_NCBI = tracking[tracking["Public in NCBI"] == "N"]
print(dataset_NCBI)

# NCBI API function for GCA
def get_GCA(field):
    base_url = 'https://api.ncbi.nlm.nih.gov/datasets/v1/genome/accession/'
    headers = {'api-Key': '15ac387fa012874cd38967399f2570e80408'}
    params = {'filters.reference_only': 'false', 'filters.assembly_source': 'all', 'filters.has_annotation': 'false',
              'filters.exclude_atypical': 'false',
              'filters.assembly_level': ['contig', 'chromosome', 'complete_genome', 'scaffold']}
    v_range = pd.DataFrame([])
    e_range = pd.DataFrame([])
    GCA_data_list = []
    status_error_list = []
    for ind in dataset_NCBI.index:
        if dataset_NCBI['Assembly type'][ind] == "clone or isolate" or dataset_NCBI['Assembly type'][ind] == "Metagenome-Assembled Genome (MAG)":
            if dataset_NCBI['accession type'][ind] == "GCA":
#                name = dataset_NCBI['name'][ind]
#                version = name[name.find(".")+1].split()[0]
                version = dataset_NCBI['version'][ind]
                print(version)
                value = dataset_NCBI[field][ind] + "." + str(version)
                print(value)
                url = base_url + str(value)
                r = requests.get(url, headers=headers, params=params)
                print(r)
                if r.status_code == 200:
                    json_data = r.json()
                    print(json_data)
                    if not json_data:
                        print(value, "not available")
                    else:
                        project = json_normalize(json_data,
                                            record_path=['assemblies', 'assembly', 'bioproject_lineages', 'bioprojects'],
                                            meta=[['assemblies', 'assembly', 'assembly_accession'],
                                                    ['assemblies', 'assembly', 'biosample_accession']])
                        GCA_data = project.head(1)
                        GCA_data.rename(
                            columns={'accession': 'project_ID', 'assemblies.assembly.assembly_accession': 'GCA_accession.v',
                            'assemblies.assembly.biosample_accession': 'Sample_ID'}, inplace=True)
#                    GCA_data = GCA_data.drop(['title', 'parent_accessions'], axis=1)
                        GCA_data['i'] = ind
                        GCA_data['accession'] = dataset_NCBI[field][ind]
                        GCA_data_list.append(GCA_data)
                else:
                    status_code = io.StringIO(str(r.status_code))
                    status_error = pd.read_csv(status_code, names=['status code'])
                    status_error['accession'] = value
                    print(status_error)
                    status_error_list.append(status_error)
    if not GCA_data_list:
        print("no accessions")
    else:
        v_range = pd.concat(GCA_data_list, ignore_index=True)
    if not status_error_list:
        print("no errors")
    else:
        e_range = pd.concat(status_error_list, ignore_index=True)
    return v_range, e_range

# validation function for GCA
def validation(range):
    for ind in range.index:
        i_accession = range['i'][ind]
        dataset_row = dataset_NCBI.loc[i_accession]
        range['project_OK'] = np.where(range['project_ID'][ind] == dataset_row['project'], 'True', 'False')
        range['sample_OK'] = np.where(range['Sample_ID'][ind] == dataset_row['sample ID'], 'True', 'False')
    return range

# NCBI API function for contigs and chromosomes
def get_seq(field):
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?'
    headers = {'api-Key': '15ac387fa012874cd38967399f2570e80408'}
    for ind in dataset_NCBI.index:
        if dataset_NCBI['Assembly type'][ind] == "clone or isolate" or dataset_NCBI['Assembly type'][ind] == "Metagenome-Assembled Genome (MAG)":
            if dataset_NCBI['accession type'][ind] == field:
                accession_range = dataset_NCBI['accessions'][ind]
                accession = accession_range.split("-", 1)[0]
                print(accession)
                params = {'db': 'nucleotide', 'id': accession}
                r = requests.get(url, headers=headers, params=params)
                print(r)
                summary = ElementTree.fromstring(r.content)
                result = summary.find('DocSum')
                print(result)
                if r.status_code == 200:
                    if result is not None:
                        tracking.loc[:, 'Public in NCBI'][ind] = 'Y'

# check contigs
get_seq('Contigs')

# check chromosomes
get_seq('Chromosomes')

# check GCA
get_GCA('accessions')
v_range, e_range = get_GCA('accessions')
GCA = pd.DataFrame(v_range)
GCA_re = pd.DataFrame(e_range)
print(GCA)

# compare ids between GCA and tracking info
validation(GCA)
range = validation(GCA)
GCA = pd.DataFrame(GCA)
print(GCA)
GCA.to_csv('GCA_ncbi.txt', sep="\t")

# update info on tracking file for GCA
for ind in GCA.index:
    accession = GCA['accession'][ind]
    if GCA['project_OK'][ind] == "True":
        if GCA['sample_OK'][ind] == "True":
            tracking.loc[tracking['accessions'] == accession, 'Public in NCBI'] = "Y"

# save updated tracking file
tracking.to_csv('tracking_file.txt', sep="\t")