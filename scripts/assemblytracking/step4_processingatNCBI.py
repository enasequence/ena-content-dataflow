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
from xml.etree import ElementTree
import configparser

# NCBI API function for GCA - need to add 'api-Key': to 'headers'
def get_GCA(field):
    base_url = 'https://api.ncbi.nlm.nih.gov/datasets/v1/genome/accession/'
    headers = {'api-Key': ncbi_api_key}
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

# NCBI API function for contigs and chromosomes  - need to add 'api-Key': to 'headers'
def get_seq(field):
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?'
    headers = {'api-Key': ncbi_api_key}
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

##################
##  USER INPUT  ##
##################
#TODO: use argparse function

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
#set the location of the config file
config_file_path = 'config_private.yaml'

###################
##  FILE INPUTS  ##
###################
# import tracking file
tracking = pd.read_csv(tracking_file_path , sep='\t',index_col=0)

#############
##  MAIN   ##
#############
#TODO: update NCBI datasets and eutils to NCBI datasets v2 API - https://www.ncbi.nlm.nih.gov/datasets/docs/v2/reference-docs/rest-api/

# get the NCBI API key from the config file
config = configparser.ConfigParser()
config.read(config_file_path)
ncbi_api_key = config['NCBI_DETAILS']['datasets_api_key']

# create sub dataframe with accessions not public at NCBI
dataset_NCBI = tracking[tracking["Public in NCBI"] == "N"]

# check contigs
get_seq('Contigs')

# check chromosomes
get_seq('Chromosomes')

# check GCA
GCA, GCA_re = get_GCA('accessions')

# compare ids between GCA and tracking info
GCA = validation(GCA)


# update info on tracking file for GCA
for ind in GCA.index:
    accession = GCA['accession'][ind]
    if GCA['project_OK'][ind] == "True" and GCA['sample_OK'][ind] == "True":
            tracking.loc[tracking['accessions'] == accession, 'Public in NCBI'] = "Y"

####################
##  FILE OUTPUTS  ##
####################

GCA.to_csv(f'{tracking_files_path}/GCA_ncbi.txt', sep="\t")

# save updated tracking file
tracking.to_csv(f'{tracking_files_path}/tracking_file.txt', sep="\t")