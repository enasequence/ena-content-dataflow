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

import io, os, requests, sys, argparse
import numpy as np
import pandas as pd
from pandas import json_normalize

# purpose of scripts - chckes ENA portal API for links between assembly, sequences and projects.
#TODO: check if I'm checking version for chromosomes and GCAs


# Portal API function
def get_links(field, type):
    v_range = pd.DataFrame([])
    e_range = pd.DataFrame([])
    df_data_list = []
    status_error_list = []
    for accession in field:
        if accession.startswith("PRJ"):
            value = "study/"
        elif accession.startswith("SAM"):
            value = "sample/"
        url = base_url + value
        params = {'format': 'json', 'accession': accession, 'result': type}
        r = requests.get(url, params=params)
        if r.status_code == 200:
            json_data = r.json()
            if not json_data:
                print (accession, "Not available")
            else:
                df_data = json_normalize(json_data)
                df_data['project/sample ID'] = accession
                df_data_list.append(df_data)
        else:
            status_code = io.StringIO(str(r.status_code))
            status_error = pd.read_csv(status_code, names=['status code'])
            status_error['accession'] = accession
            print(status_error)
            status_error_list.append(status_error)
    if not df_data_list:
        print("no links")
    else:
        v_range = pd.concat(df_data_list, ignore_index=True)
    if not status_error_list:
        pass
    else:
        e_range = pd.concat(status_error_list, ignore_index=True)
    return v_range, e_range


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="step 3 script argparser")
    parser.add_argument('-p', '--project', help="Project to track DToL, ASG or ERGA", default="none")
    parser.add_argument('-w', '--workingdir', help="location of tracking file folders",
                        default="scripts/assemblytracking/")
    opts = parser.parse_args()
    print('''
    --------------------------------------
         running step3 - ENA linking
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

    #############
    ##  MAIN   ##
    #############
    # base url for portal API
    base_url = 'https://www.ebi.ac.uk/ena/portal/api/links/'

    # create sub dataframe with accessions not linked at ENA
    tracking = pd.read_csv(tracking_file_path, sep='\t', index_col=0)  # import tracking file
    dataset_ENA = tracking[(tracking["Linked to Project"] == "N") | (tracking["Linked to Sample"] == "N")]
    Sample = dataset_ENA['sample ID'].unique()
    Project = dataset_ENA['project'].unique()

    # Project links to wgs_set
    print("Project links to contigs")
    Project_contigs, Project_contigs_re = get_links(Project, 'wgs_set')

    # Project links to chromosomes
    print("Project links to chromosomes")
    Project_chr, Project_chr_re = get_links(Project, 'sequence')

    Project_chr_range = pd.DataFrame()
    if Project_chr.empty == True:
        print("no project chromosome links")
    else:
        Project_chr['accession_last'] = Project_chr['accession']
        aggregation_functions = {'accession': 'min', 'accession_last': 'max'}
        Project_chr_range = Project_chr.groupby(Project_chr['project/sample ID'], as_index=False).aggregate(
            aggregation_functions)
        Project_chr_range['chr_range'] = Project_chr_range['accession'] + "-" + Project_chr_range['accession_last']

    # Project links to GCAs
    print("Project links to GCA")
    Project_GCA, Project_GCA_re = get_links(Project, 'assembly')

    # Project links to Analysis
    print("Project links to metagenomes")
    Project_analysis, Project_analysis_re = get_links(Project, 'analysis')

    # Sample links to wgs_set
    print("Sample links to contigs")
    Sample_contigs, Sample_contigs_re = get_links(Sample, 'wgs_set')

    # Sample links to chromosomes
    print("Sample links to chromosomes")
    Sample_chr, Sample_chr_re = get_links(Sample, 'sequence')

    Sample_chr_range = pd.DataFrame()
    if Sample_chr.empty == True:
        print("no sample chromosome links")
    else:
        Sample_chr['accession_last'] = Sample_chr['accession']
        aggregation_functions = {'accession': 'min', 'accession_last': 'max'}
        Sample_chr_range = Sample_chr.groupby(Sample_chr['project/sample ID'], as_index=False).aggregate(
            aggregation_functions)
        Sample_chr_range['chr_range'] = Sample_chr_range['accession'] + "-" + Sample_chr_range['accession_last']

    # Sample links to GCA
    print("Sample links to GCA")
    Sample_GCA, Sample_GCA_re = get_links(Sample, 'assembly')

    # Sample links to Analysis
    print("Sample links to metagenomes")
    Sample_analysis, Sample_analysis_re = get_links(Sample, 'analysis')

    # update info on tracking file
    for ind in dataset_ENA.index:
        if dataset_ENA['Assembly type'][ind] == "primary metagenome" or dataset_ENA['Assembly type'][
            ind] == "binned metagenome":
            accession = dataset_ENA['analysis ID'][ind]
            if Project_analysis.empty == False:
                tracking.loc[:, 'Linked to Project'][ind] = np.where(
                    accession in set(Project_analysis['analysis_accession']), 'Y', 'N')
            if Sample_analysis.empty == False:
                tracking.loc[:, 'Linked to Sample'][ind] = np.where(
                    accession in set(Sample_analysis['analysis_accession']), 'Y', 'N')
        else:
            if dataset_ENA['accession type'][ind] == "Contigs":
                accession = dataset_ENA['accessions'].str[:8][ind]
                if Project_contigs.empty == False:
                    tracking.loc[:, 'Linked to Project'][ind] = np.where(
                        accession in set(Project_contigs['accession'].str[:8]), 'Y', 'N')
                if Sample_contigs.empty == False:
                    tracking.loc[:, 'Linked to Sample'][ind] = np.where(
                        accession in set(Sample_contigs['accession'].str[:8]), 'Y', 'N')
            if dataset_ENA['accession type'][ind] == "GCA":
                accession = dataset_ENA['accessions'][ind]
                if Project_GCA.empty == False:
                    tracking.loc[:, 'Linked to Project'][ind] = np.where(accession in set(Project_GCA['accession']),
                                                                         'Y', 'N')
                if Sample_GCA.empty == False:
                    tracking.loc[:, 'Linked to Sample'][ind] = np.where(accession in set(Sample_GCA['accession']), 'Y',
                                                                        'N')
            if dataset_ENA['accession type'][ind] == "Chromosomes":
                accession = dataset_ENA['accessions'][ind]
                if Project_chr.empty == False:
                    tracking.loc[:, 'Linked to Project'][ind] = np.where(
                        accession in set(Project_chr_range['chr_range']), 'Y', 'N')
                if Sample_chr.empty == False:
                    tracking.loc[:, 'Linked to Sample'][ind] = np.where(accession in set(Sample_chr_range['chr_range']),
                                                                        'Y', 'N')

    ####################
    ##  FILE OUTPUTS  ##
    ####################

    # save updated tracking file
    tracking.to_csv(f'{tracking_files_path}/tracking_file.txt', sep="\t")

    # save additional output(s)
    Project_chr_range.to_csv(f'{tracking_files_path}/Project_chr_range.txt', sep="\t")
    # Project_links_re.to_csv('project_links_errors.txt', sep="\t")

