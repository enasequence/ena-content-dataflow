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

import io, os, sys, argparse, requests
import datetime as dt
import numpy as np
import pandas as pd
from pandas import json_normalize

# Purpose of script - adds new assemblies to the tracking_file.txt in the format used for logging and tracking of
# the progress of the assemblies release.
# TODO: tidy this script into functions:
# 1- get Taxon info and report API errors.
# 2 - add to tracking file and indexing - look at improving indexing.

#############
##  MAIN   ##
#############

def get_taxon(releasing_seq):
    # get taxon information
    df_data_obj, status_error_list = [], []

    # get scientific name by inputting sample ID list
    for biosample_id in releasing_seq['sample ID']:
        params = {'format': 'json',
                  'fields': 'scientific_name',
                  'includeAccessions': biosample_id,
                  'result': 'sample'}
        r = requests.get('https://www.ebi.ac.uk/ena/portal/api/search', params)
        if r.status_code == 200:
            json_data = r.json()
            #print('json_data', type(json_data), json_data)
            taxon_sample = {'taxon': json_data[0]['scientific_name'],
                        'sample ID': json_data[0]['sample_accession']
                       }
            df_data_obj.append(taxon_sample)
        else:
            status_code = io.StringIO(str(r.status_code))
            status_error = pd.read_csv(status_code, names=['status code'])
            status_error['accession'] = sample
            status_error_list.append(status_error)
    # print out error info
    if status_error_list is None:
        print('no taxon API errors')
    else:
        print('ENA taxonomy API error:', status_error_list)

    # add taxon info to rel_seq sheet by matching sample ID
    taxon_info = pd.DataFrame(df_data_obj)
    releasing_seq_tx = releasing_seq.merge(taxon_info, how='left', left_index=True, right_index=True)
    releasing_seq_tx = releasing_seq_tx.drop(columns=['sample ID_y'])
    releasing_seq_tx = releasing_seq_tx.rename(columns={"sample ID_x": "sample ID"})

    return releasing_seq_tx


def add_to_tracking(releasing_seq_tx, tracking):
    # to import tracking file and duplicate lines for each data type

    # get index of the latest row in the tracking file and increment this index onto new assemblies to be added to tracking
    last_index = tracking['index'].iloc[-1]
    releasing_seq_tx.index = releasing_seq_tx.index + last_index + 1

    # (optional - from initial import script) to import list of public assemblies from another source to track
    # releasing_seq_tx  =  pd.read_excel('ERGA_Publicly_available_04Jan23.xlsx', sheet_name='Publicly available')
    # rename column names to 'dataset' format
    # releasing_seq_tx = pd.DataFrame(releasing_seq_tx, columns=['name', 'submission date', 'accessioned',
    # 'shared to NCBI', 'project', 'analysis ID', 'sample ID', 'GCA ID', 'Contig range', 'Chr range','Assembly type'])

    # format file with the new accessions to add to the tracking file
    dataset2 = releasing_seq_tx.loc[releasing_seq_tx.index.repeat(3), :].reset_index(drop=False)
    dataset2.loc[(dataset2.index == 0), 'accession type'] = 'Contigs'
    dataset2.loc[(dataset2.index % 3 == 0), 'accession type'] = 'Contigs'
    dataset2.loc[(dataset2.index == 1), 'accession type'] = 'Chromosomes'
    dataset2.loc[((dataset2.index + 2) % 3 == 0), 'accession type'] = 'Chromosomes'
    dataset2.loc[(dataset2.index == 2), 'accession type'] = 'GCA'
    dataset2.loc[((dataset2.index + 1) % 3 == 0), 'accession type'] = 'GCA'
    # to add column with data type
    dataset2['accessions'] = np.where(dataset2['accession type'].str.contains("Contigs"), dataset2['Contig range'],
                                      np.where(dataset2['accession type'].str.contains("Chromosomes"),
                                               dataset2['Chr range'],
                                               np.where(dataset2['accession type'].str.contains("GCA"),
                                                        dataset2['GCA ID'], '')))
    # to remove lines with blanks for Chromosomes or Contigs
    dataset2 = dataset2[dataset2['accessions'].str.len() > 0]
    # add version information to the tracking file
    dataset2['version'] = 1
    for ind in dataset2.index:
        name = dataset2['name'][ind]
        version = name[name.rfind(".") + 1].split()[0]
        version = int(version)
        dataset2.loc[ind, 'version'] = version

    # to add info on tracking to the file (use Y for all options here with initial import)
    dataset2['Public in ENA'] = "N"
    dataset2['Public in NCBI'] = "N"
    dataset2['Linked to Project'] = "N"
    dataset2['Linked to Sample'] = "N"
    dataset2['publicly available date'] = ""
    # convert columns from string to datetime (step1 only)
    dataset2 = dataset2.drop(['GCA ID', 'Contig range', 'Chr range'], axis=1)


    #join the new accessions with existing tracking file
    tracking_new = pd.concat([tracking, dataset2], ignore_index=True)
    tracking_new.to_csv(tracking_file_path, sep="\t")
    print(len(releasing_seq_tx), 'new assemblies added to tracking file with scientific name')



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="step 1 script argparser")
    parser.add_argument('-p', '--project', help="Project to track DToL, ASG or ERGA", default="none")
    parser.add_argument('-w', '--workingdir', help="location of tracking file folders",
                        default="scripts/assemblytracking/")
    opts = parser.parse_args()

    # set thw working directory to location of scripts and of config file and of tracking folders
    os.chdir(opts.workingdir)
    # set the location of the tracking files
    tracking_files_path = f'{opts.project}-tracking-files'
    tracking_file_path = f'{tracking_files_path}/tracking_file.txt'

    print('''
    ------------------------------------------
      running step1 - add assemblies to file
    ------------------------------------------
    ''')

    # import file with new assemblies
    releasing_seq = pd.read_csv(f'{tracking_files_path}/Releasing_sequences.csv', header=0)
    releasing_seq_tx = get_taxon(releasing_seq)
    # import tracking file and add assemblies
    tracking = pd.read_csv(tracking_file_path, sep='\t', index_col=0)  # import tracking file
    add_to_tracking(releasing_seq_tx, tracking)


