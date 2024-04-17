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

# import tracking file
tracking = pd.read_csv('tracking_file.txt', sep='\t')
tracking = tracking.drop(['Unnamed: 0'], axis=1)

# base url for browser API
base_url = 'https://www.ebi.ac.uk/ena/browser/api/summary/'

# create sub dataframe with accessions not public at ENA
dataset_ENA = tracking[tracking["Public in ENA"] == "N"]
print(dataset_ENA)


# browser API function for project and sample
def get_accessions(field):
    v_range = pd.DataFrame([])
    e_range = pd.DataFrame([])
    df_data_list = []
    status_error_list = []
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
    v_range = pd.DataFrame([])
    e_range = pd.DataFrame([])
    df_data_list = []
    status_error_list = []
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
    p_error = []
    project_error_list = []
    s_error = []
    sample_error_list = []
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


# to query the Browser API for taxID of project and export to a data frame
print("Project")
get_accessions('project')
v_range, e_range = get_accessions('project')
Project = pd.DataFrame(v_range)
Project_re = pd.DataFrame(e_range)
print(Project)
Project.to_csv('Project.txt', sep="\t")

# to query the Browser API for taxID of sample and export to a data frame
print("Sample")
get_accessions('sample ID')
v_range, e_range = get_accessions('sample ID')
Sample = pd.DataFrame(v_range)
Sample_re = pd.DataFrame(e_range)
print(Sample)

# to query the Browser API for summary records of Contigs and export to a data frame
print("Contigs")
get_data('Contigs')
v_range, e_range = get_data('Contigs')
Contig_range = pd.DataFrame(v_range)
Contig_range_re = pd.DataFrame(e_range)

# compare ids between project, sample, taxon and Contig_range
validation(Contig_range)
range, p_error, s_error = validation(Contig_range)
Contig_range = pd.DataFrame(range)
Contig_Project_errors = pd.DataFrame(p_error)
Contig_sample_errors = pd.DataFrame(s_error)
print(Contig_range)
Contig_range.to_csv('Contig_range.txt', sep="\t")
Contig_Project_errors.to_csv('Contig_project_errors.txt', sep="\t")
Contig_sample_errors.to_csv('Contig_sample_errors.txt', sep="\t")

# update info on tracking file for Contigs
for ind in Contig_range.index:
    accession = Contig_range['contigs'][ind]
    if Contig_range['version_OK'][ind] == "True":
        if Contig_range['project_OK'][ind] == "True":
            if Contig_range['sample_OK'][ind] == "True":
                if Contig_range['taxon_prj_OK'][ind] == "True":
                    if Contig_range['taxon_sp_OK'][ind] == "True":
                        tracking.loc[tracking['accessions'] == accession, 'Public in ENA'] = "Y"
                        tracking.loc[tracking['accessions'] == accession, 'publicly available date'] = pd.to_datetime('today').strftime('%d/%m/%Y')
print(tracking)

# to query the Browser API for summary records of Chr_range and export it to a data frame
print("Chr range")
get_data('Chromosomes')
v_range, e_range = get_data('Chromosomes')
Chr_range = pd.DataFrame(v_range)
Chr_range_re = pd.DataFrame(e_range)

# compare ids between project, sample, taxon and Chr_range
validation(Chr_range)
range, p_error, s_error = validation(Chr_range)
Chr_range = pd.DataFrame(range)
Chr_Project_errors = pd.DataFrame(p_error)
Chr_sample_errors = pd.DataFrame(s_error)
#try to merge identical lines in the errors files
print(Chr_range)
Chr_range.to_csv('Chr_range.txt', sep="\t")
Chr_Project_errors.to_csv('Chr_project_errors.txt', sep="\t")
Chr_sample_errors.to_csv('Chr_sample_errors.txt', sep="\t")

# update info on tracking file for Chromosomes
for ind in Chr_range.index:
    i_accession = Chr_range['i'][ind]
    Chr_range_i = Chr_range[Chr_range['i'] == i_accession]
    first_accession = Chr_range_i['accession'].iloc[0]
    last_accession = Chr_range_i['accession'].iloc[-1]
    accession = first_accession + "-" + last_accession
    print(accession)
    if Chr_range['version_OK'][ind] == "True":
        if Chr_range['project_OK'][ind] == "True":
            if Chr_range['sample_OK'][ind] == "True":
                if Chr_range['taxon_prj_OK'][ind] == "True":
                    if Chr_range['taxon_sp_OK'][ind] == "True":
                        tracking.loc[tracking['accessions'] == accession, 'Public in ENA'] = "Y"
                        tracking.loc[tracking['accessions'] == accession, 'publicly available date'] = pd.to_datetime('today').strftime('%d/%m/%Y')
print(tracking)

# to query the Browser API for summary records of GCAs and export to a data frame
print("GCA")
get_data('GCA')
v_range, e_range = get_data('GCA')
GCA = pd.DataFrame(v_range)
GCA_re = pd.DataFrame(e_range)

# compare ids between project, sample, taxon and GCA
validation(GCA)
range, p_error, s_error = validation(GCA)
GCA = pd.DataFrame(range)
print(GCA)
GCA.to_csv('GCA.txt', sep="\t")

# update info on tracking file for GCAs
for ind in GCA.index:
    accession = GCA['accession'][ind]
    if GCA['version_OK'][ind] == "True":
        tracking.loc[tracking['accessions'] == accession, 'Public in ENA'] = "Y"
        tracking.loc[tracking['accessions'] == accession, 'publicly available date'] = pd.to_datetime('today').strftime('%d/%m/%Y')
print(tracking)

#to query the Browser API for summary records of analysis (metagenomes and binned metagenomes) and export to a data frame
print("metagenomes")
analysis = pd.DataFrame([])
analysis_errors = pd.DataFrame([])
df_analysis_list = []
status_error_list_a = []
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

# save updated tracking file
tracking.to_csv('tracking_file.txt', sep="\t")


#NOT CHECKING VERSION FOR CHROMOSOMES - THINK ON HOW TO DO THIS