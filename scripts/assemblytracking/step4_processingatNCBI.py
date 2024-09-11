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

import io, os, requests, sys, argparse, configparser, json
import numpy as np
import pandas as pd
from pandas import json_normalize
from xml.etree import ElementTree


# NCBI API function for GCA (API docs: https://www.ncbi.nlm.nih.gov/datasets/docs/v2/reference-docs/rest-api/)
def check_GCA(field, dataset_NCBI):
    # checks for availability all GCA in tracking file that have 'N' for public in NCBI
    base_url = 'https://api.ncbi.nlm.nih.gov/datasets/v2alpha/genome/accession/'
    headers = {'api-Key': ncbi_api_key}
    params = {'filters.reference_only': 'false', 'filters.assembly_source': 'all', 'filters.has_annotation': 'false',
              'filters.exclude_atypical': 'false',
              'filters.assembly_level': ['contig', 'chromosome', 'complete_genome', 'scaffold']}

    v_range = pd.DataFrame()
    e_range = pd.DataFrame()
    GCA_data_list = []
    status_error_list = []
    for ind in dataset_NCBI.index:
        if (dataset_NCBI['Assembly type'][ind] == "clone or isolate" \
                or dataset_NCBI['Assembly type'][ind] == "Metagenome-Assembled Genome (MAG)") \
                and dataset_NCBI['accession type'][ind] == "GCA":
            version = dataset_NCBI.loc[ind, 'version']
            gc_id = dataset_NCBI.loc[ind, field]
            value = gc_id + "." + str(version)
            url = base_url + str(value) + '/dataset_report'
            r = requests.get(url, headers=headers, params=params)
            if r.status_code == 200:
                json_data = r.json()
                if not json_data:
                    print(value, "not available")
                else:
                    data = json_data
                    parent_accessions = [proj['accession'] for proj in data['reports'][0]['assembly_info']['bioproject_lineage'][0]['bioprojects']]
                    gc_v = data['reports'][0]['current_accession']
                    gca = gc_v.split('.')[0]
                    # noinspection PyDictCreation
                    GCA_data = {'project_ID': data['reports'][0]['assembly_info']['bioproject_accession'],
                               'title': data['reports'][0]['assembly_info']['bioproject_lineage'][0]['bioprojects'][0]['title'],
                               'parent_accessions': parent_accessions,
                               'GCA_accession.v': gc_v,
                               'Sample_ID': data['reports'][0]['assembly_info']['biosample']['accession'],
                               'i': ind,
                               'accession': gca}
                    GCA_data['accession'] = dataset_NCBI.loc[ind, field]
                    GCA_data_list.append(GCA_data)
            else:
                status_code = io.StringIO(str(r.status_code))
                status_error = pd.read_csv(status_code, names=['status code'])
                status_error['accession'] = value
                print(status_error)
                status_error_list.append(status_error)
    print('status_error_list', status_error_list)
    print('GCA_data_list', GCA_data_list)
    if not GCA_data_list:
        print("no new GCA accessions public at NCBI")
    else:
        print(len(GCA_data_list), "new GCA accessions public at NCBI")
        v_range = pd.DataFrame(GCA_data_list)
    if not status_error_list:
        print("no errors")
    else:
        e_range = pd.concat(status_error_list, ignore_index=True)
    return v_range, e_range

# validation function for GCA
def validation(range, dataset_NCBI):
    for ind in range.index:
        i_accession = range['i'][ind]
        dataset_row = dataset_NCBI.loc[i_accession]
        range['project_OK'] = np.where(range['project_ID'][ind] == dataset_row['project'], 'True', 'False')
        range['sample_OK'] = np.where(range['Sample_ID'][ind] == dataset_row['sample ID'], 'True', 'False')
    return range

# NCBI API function for contigs and chromosomes  - need to add 'api-Key': to 'headers'
def get_seq(field, dataset_NCBI):
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?'
    headers = {'api-Key': ncbi_api_key}
    for ind in dataset_NCBI.index:
        if (dataset_NCBI.loc[ind, 'Assembly type'] == "clone or isolate" \
                or dataset_NCBI.loc[ind,'Assembly type'] == "Metagenome-Assembled Genome (MAG)") and \
                dataset_NCBI.loc[ind,'accession type'] == field:
            accession_range = dataset_NCBI.loc[ind,'accessions']
            accession = accession_range.split("-", 1)[0]
            #print(accession)
            params = {'db': 'nucleotide', 'id': accession}
            r = requests.get(url, headers=headers, params=params)
            #print(r)
            summary = ElementTree.fromstring(r.content)
            result = summary.find('DocSum')
            #print(result)
            if r.status_code == 200 and result is not None:
                tracking.loc[ind, 'Public in NCBI'] = 'Y'


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="sql_processingatENA")
    parser.add_argument('-p', '--project', help="Project to track DToL, ASG or ERGA", default="none")
    parser.add_argument('-w', '--workingdir', help="location of tracking file folders",
                        default="scripts/assemblytracking/")
    parser.add_argument('-c', '--config', help="config file path", default="config_private.yaml")
    opts = parser.parse_args()
    print('''
    --------------------------------------
    running step4 - procesing at NCBI
    --------------------------------------
        ''')
    # set the working directory
    # check the current working directory
    os.chdir(opts.workingdir)
    # set which project to track - determines the folder where tracking files will be read and written
    project = opts.project  # DToL or ASG or ERGA

    # set the location of the tracking files
    tracking_files_path = f'{project}-tracking-files'
    tracking_file_path = f'{tracking_files_path}/tracking_file.txt'
    tracking = pd.read_csv(tracking_file_path, sep='\t', index_col=0)
    config_file_path = opts.config

    # get the NCBI API key from the config file
    config = configparser.ConfigParser()
    config.read(config_file_path)
    ncbi_api_key = config['NCBI_DETAILS']['datasets_api_key']

    # create sub dataframe with accessions not public at NCBI
    dataset_NCBI = tracking[tracking["Public in NCBI"] == "N"]
    #dataset_NCBI = tracking
    print('Total GCAs to check at NCBI:', len(dataset_NCBI))

    # check contigs
    get_seq('Contigs', dataset_NCBI)

    # check chromosomes
    get_seq('Chromosomes', dataset_NCBI)

    # check GCA
    GCA, GCA_re = check_GCA('accessions', dataset_NCBI)

    # compare ids between GCA and tracking info
    GCA = validation(GCA, dataset_NCBI)

    # update info on tracking file for GCA
    for ind in GCA.index:
        accession = GCA.loc[ind,'accession']
        if GCA.loc[ind,'project_OK' == "True"] and GCA[ind,'sample_OK'] == "True":
            tracking.loc[tracking['accessions'] == accession, 'Public in NCBI'] = "Y"
        else:
            pass

    ####################
    ##  FILE OUTPUTS  ##
    ####################

    GCA.to_csv(f'{tracking_files_path}/GCA_ncbi.txt', sep="\t")

    # save updated tracking file
    tracking.to_csv(f'{tracking_files_path}/tracking_file.txt', sep="\t")

