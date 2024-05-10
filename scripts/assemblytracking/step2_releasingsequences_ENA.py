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

# browser API function for project and sample
def get_accessions(field):
    v_range = pd.DataFrame()
    e_range = pd.DataFrame()
    df_data_list, status_error_list = [], []
    for ind in dataset_ENA.index:
        value = dataset_ENA[field][ind]
        url = base_url + str(value)
        r = requests.get(url)
        if r.status_code == 200:
            json_data = r.json()
            df_data = json_normalize(json_data['summaries'])
            df_data['i'] = ind
            df_data_list.append(df_data)
        else:
            status_code = io.StringIO(str(r.status_code))
            status_error = pd.read_csv(status_code, names=['status code'])
            status_error['accession'] = value
            print(status_error)
            status_error_list.append(status_error)
    if not df_data_list:
        print("no accessions")
    else:
        v_range = pd.concat(df_data_list, ignore_index=True)
    if not status_error_list:
        print("no errors")
    else:
        e_range = pd.concat(status_error_list, ignore_index=True)
    return v_range, e_range

# browser API function for data
def get_data(field):
    v_range = pd.DataFrame()
    e_range = pd.DataFrame()
    df_data_list, status_error_list = [], []
    for ind in dataset_ENA.index:
        if dataset_ENA['Assembly type'][ind] == "clone or isolate" or dataset_ENA['Assembly type'][ind] == "Metagenome-Assembled Genome (MAG)":
            if dataset_ENA['accession type'][ind] == field:
                value = dataset_ENA['accessions'][ind]
                url = base_url + str(value)
                r = requests.get(url)
                if r.status_code == 200:
                    json_data = r.json()
                    df_data = json_normalize(json_data['summaries'])
                    df_data['i'] = ind
                    df_data_list.append(df_data)
                else:
                    status_code = io.StringIO(str(r.status_code))
                    status_error = pd.read_csv(status_code, names=['status code'])
                    status_error['accession'] = value
                    print(status_error)
                    status_error_list.append(status_error)
    if not df_data_list:
        print("no accessions")
    else:
        v_range = pd.concat(df_data_list, ignore_index=True)
    if not status_error_list:
        print("no errors")
    else:
        e_range = pd.concat(status_error_list, ignore_index=True)
    return v_range, e_range

# validation function
def validation(range):
    range['version_OK'] = "True"
    range['project_OK'] = ""
    range['sample_OK'] = ""
    range['taxon_prj_OK'] = ""
    range['taxon_sp_OK'] = ""
    p_error, project_error_list, s_error, sample_error_list = [], [], [], []

    for ind in range.index:
        i_accession = range['i'][ind]
        dataset_row = dataset_ENA.loc[i_accession]
        if range['dataType'][ind] == "CONTIGSET" or range['dataType'][ind] == "ASSEMBLY":
            accession = range['accession'][ind]
            version_range = range['version'][ind]
            version_r = int(version_range)
            version = dataset_row['version']
            print(accession, version_r, version)
            if version_r == version:
                range['version_OK'][ind] = "True"
            else:
                range['version_OK'][ind] = "False"
        if range['dataType'][ind] == "CONTIGSET" or range['dataType'][ind] == "SEQUENCE":
            range['project_OK'] = np.where(range['project'][ind] == dataset_row['project'], 'True', 'False')
            range['sample_OK'] = np.where(range['sample'][ind] == dataset_row['sample ID'], 'True', 'False')
            project_row = Project[Project['i'] == i_accession]
            if project_row.empty:
                range['taxon_prj_OK'][ind] = "Error"
                project_id = (range['project'][ind], i_accession)
                project_error_list.append(project_id)
            else:
                taxon_prj = project_row['taxon'].values[0]
                range['taxon_prj_OK'][ind] = np.where(range['taxon'][ind] == taxon_prj, 'True', 'False')
            sample_row = Sample[Sample['i'] == i_accession]
            if sample_row.empty:
                range['taxon_sp_OK'][ind] = "Error"
                sample_id = (range['sample'][ind], i_accession)
                sample_error_list.append(sample_id)
            else:
                taxon_sp = sample_row['taxon'].values[0]
                range['taxon_sp_OK'][ind] = np.where(range['taxon'][ind] == taxon_sp, 'True', 'False')
    if not project_error_list:
        print("No project errors")
    else:
        p_error = project_error_list
        print("##WARNING PROJECT ERRORS##")
    if not sample_error_list:
        print("No sample errors")
    else:
        s_error = sample_error_list
        print("##WARNING SAMPLE ERRORS##")
    return range, p_error, s_error

##################
##  USER INPUT  ##
##################
# TODO: use argparse function
# set the working directory
# check the current working directory
os.getcwd()  # should be 'C:\\Users\\USERNAME\\pathto\\githubrepo\\ena-content-dataflow' on local machine
# set thw working directory to location of scripts and of config file
os.chdir('scripts/assemblytracking/')
# set which project to track - determines the folder where tracking files will be read and written
project = 'ASG'  # or ASG or ERGA

# set the location of the tracking files
tracking_files_path = f'{project}-tracking-files'
tracking_file_path = f'{tracking_files_path}/tracking_file.txt'

###################
##  FILE INPUTS  ##
###################
tracking = pd.read_csv(tracking_file_path, sep='\t',index_col=0)

#############
##  MAIN   ##
#############
# TODO: NOT CHECKING VERSION FOR CHROMOSOMES - THINK ON HOW TO DO THIS

# create sub dataframe with accessions not public at ENA
dataset_ENA = tracking[tracking["Public in ENA"] == "N"]
print(dataset_ENA.info())

# base url for browser API
base_url = 'https://www.ebi.ac.uk/ena/browser/api/summary/'

# to query the Browser API for taxID of project and export to a data frame
print("Project")
Project, Project_re = get_accessions('project')
# Project data frame written out to txt at this point - moved to end - inexplicable why saved

# to query the Browser API for taxID of sample and export to a data frame
print("Sample")
get_accessions('sample ID')
Sample, Sample_re = get_accessions('sample ID')
print(Sample)

# to query the Browser API for summary records of Contigs and export to a data frame
print("Contigs")
get_data('Contigs')
Contig_range, Contig_range_re = get_data('Contigs')

# compare ids between project, sample, taxon and Contig_range
validation(Contig_range)
Contig_range, Contig_Project_errors, Contig_sample_errors = validation(Contig_range)
print(Contig_range)

# read out contig_range, contig_project_errors, contig_sample_erros to csv file - moved to end of script

# update info on tracking file for Contigs
for ind in Contig_range.index:
    accession = Contig_range['contigs'][ind]
    if (Chr_range['version_OK'][ind] == "True" and Chr_range['project_OK'][ind] == "True" and
            Chr_range['sample_OK'][ind] == "True" and Chr_range['taxon_prj_OK'][ind] == "True" and
            Chr_range['taxon_sp_OK'][ind] == "True"):
        tracking.loc[tracking['accessions'] == accession, 'Public in ENA'] = "Y"
        tracking.loc[tracking['accessions'] == accession, 'publicly available date'] = pd.to_datetime('today').strftime('%d/%m/%Y')
print(tracking)

# to query the Browser API for summary records of Chr_range and export it to a data frame
print("Chr range")
Chr_range, Chr_range_re = get_data('Chromosomes')

# compare ids between project, sample, taxon and Chr_range
Chr_range, Chr_Project_errors, Chr_sample_errors = validation(Chr_range)
#try to merge identical lines in the errors files

# read out chr_range, chr_project_errors, chr_sample_erros to csv file - moved to end of script

# update info on tracking file for Chromosomes
for ind in Chr_range.index:
    i_accession = Chr_range['i'][ind]
    Chr_range_i = Chr_range[Chr_range['i'] == i_accession]
    first_accession = Chr_range_i['accession'].iloc[0]
    last_accession = Chr_range_i['accession'].iloc[-1]
    accession = first_accession + "-" + last_accession
    print(accession)
    if (Chr_range['version_OK'][ind] == "True" and Chr_range['project_OK'][ind] == "True" and
            Chr_range['sample_OK'][ind] == "True" and Chr_range['taxon_prj_OK'][ind] == "True" and
            Chr_range['taxon_sp_OK'][ind] == "True"):
        tracking.loc[tracking['accessions'] == accession, 'Public in ENA'] = "Y"
        tracking.loc[tracking['accessions'] == accession, 'publicly available date'] = pd.to_datetime('today').strftime('%d/%m/%Y')
print(tracking)

# to query the Browser API for summary records of GCAs and export to a data frame
print("GCA")
GCA, GCA_re = get_data('GCA')

# compare ids between project, sample, taxon and GCA
GCA, p_error, s_error = validation(GCA)

# write GCA to file at this point - this has been moved to end of script to include with outputs

# update info on tracking file for GCAs
for ind in GCA.index:
    accession = GCA['accession'][ind]
    if GCA['version_OK'][ind] == "True":
        tracking.loc[tracking['accessions'] == accession, 'Public in ENA'] = "Y"
        tracking.loc[tracking['accessions'] == accession, 'publicly available date'] = pd.to_datetime('today').strftime('%d/%m/%Y')
print(tracking)

#to query the Browser API for summary records of analysis (metagenomes and binned metagenomes) and export to a data frame
print("metagenomes")
analysis = pd.DataFrame()
analysis_errors = pd.DataFrame()
df_analysis_list, status_error_list_a  = [], []

for ind in dataset_ENA.index:
    if dataset_ENA['Assembly type'][ind] == "primary metagenome" or dataset_ENA['Assembly type'][ind] == "binned metagenome":
        value = dataset_ENA['analysis ID'][ind]
        url = base_url + str(value)
        r = requests.get(url)
        if r.status_code == 200:
            json_data = r.json()
            df_data = json_normalize(json_data['summaries'])
            df_data['i'] = ind
            df_analysis_list.append(df_data)
        else:
            status_code = io.StringIO(str(r.status_code))
            status_error = pd.read_csv(status_code, names=['status code'])
            status_error['accession'] = value
            print(status_error)
            status_error_list_a.append(status_error)
    if not df_analysis_list:
        print("no analysis")
    else:
        analysis = pd.concat(df_analysis_list, ignore_index=True)
    if not status_error_list_a:
        print("no errors")
    else:
        analysis_errors = pd.concat(status_error_list_a, ignore_index=True)
print(analysis)

# update info on tracking file for Analysis
for ind in analysis.index:
    accession = analysis['accession'][ind]
    tracking.loc[tracking['analysis ID'] == accession, 'Public in ENA'] = "Y"
    tracking.loc[tracking['analysis ID'] == accession, 'publicly available date'] = pd.to_datetime('today').strftime('%d/%m/%Y')
print(tracking)


####################
##  FILE OUTPUTS  ##
####################
# some of the file outputs list errors and need to be reported if present as part of this intermediate step.

# read out Project
Project_save_path = f'{tracking_files_path}/Project.txt'
Project.to_csv(Project_save_path, sep="\t")

# read out contig_range, contig_project_errors, contig_sample_erros to csv file - moved to end of script - presumably
# includes manual intervention for any errors identified at this point.
Contig_range.to_csv(f'{tracking_files_path}/Contig_range.txt', sep="\t")
Contig_Project_errors.to_csv(f'{tracking_files_path}/Contig_project_errors.txt', sep="\t")
Contig_sample_errors.to_csv(f'{tracking_files_path}/Contig_sample_errors.txt', sep="\t")

# read out chr_range, chr_project_errors, chr_sample_erros to csv file - moved to end of script
Chr_range.to_csv(f'{tracking_files_path}/Chr_range.txt', sep="\t")
Chr_Project_errors.to_csv(f'{tracking_files_path}/Chr_project_errors.txt', sep="\t")
Chr_sample_errors.to_csv(f'{tracking_files_path}/Chr_sample_errors.txt', sep="\t")

# write GCA to file at this point - this has been moved to end of script again
GCA.to_csv(f'{tracking_files_path}/, sep="\t")


# save updated tracking file
tracking.to_csv(tracking_file_path, sep="\t")
