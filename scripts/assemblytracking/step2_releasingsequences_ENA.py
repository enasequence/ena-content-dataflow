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


# purpose of script - uses ena browser API to check for links to sample and project in browser
# TODO: NOT CHECKING VERSION FOR CHROMOSOMES - THINK ON HOW TO DO THIS


# browser API function for project and sample
def get_accessions(field, base_url):
    dataset_ENA = tracking[tracking["Public in ENA"] == "N"]
    v_range = pd.DataFrame()
    e_range = pd.DataFrame()
    df_data_list, status_error_list = [], []
    for i, row in dataset_ENA.iterrows():
        value = row[field]
        url = base_url + str(value)
        r = requests.get(url)
        if r.status_code == 200:
            json_data = r.json()
            df_data = json_normalize(json_data['summaries'])
            df_data['i'] = i
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
def get_data(field, base_url):
    dataset_ENA = tracking[tracking["Public in ENA"] == "N"]
    v_range = pd.DataFrame()
    e_range = pd.DataFrame()
    df_data_list, status_error_list = [], []
    for ind, row in dataset_ENA.iterrows():
        if (row['Assembly type'] == "clone or isolate" \
            or row['Assembly type'] == "Metagenome-Assembled Genome (MAG)") \
            and row['accession type'] == field:
                value = row['accessions']
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
                    print(value,' has status_error:')
                    print(status_error)
                    status_error_list.append(status_error)
    if not df_data_list:
        pass
    else:
        v_range = pd.concat(df_data_list, ignore_index=True)
    if not status_error_list:
        print('no errors')
    else:
        e_range = pd.concat(status_error_list, ignore_index=True)
    return v_range, e_range

# validation function
def validation(range, Project, Sample):
    dataset_ENA = tracking[tracking["Public in ENA"] == "N"]
    range['version_OK'] = "True"
    range['project_OK'] = ""
    range['sample_OK'] = ""
    range['taxon_prj_OK'] = ""
    range['taxon_sp_OK'] = ""
    p_error, project_error_list, s_error, sample_error_list = [], [], [], []

    for ind, row in range.iterrows():
        i_accession = row['i']
        if row['dataType'] == "ASSEMBLY":  # validate GCAs
            dataset_row = dataset_ENA.loc[(dataset_ENA['index'] == i_accession) & (dataset_ENA['accession type'] == 'GCA')]
            version_range = row['version']
            version_r = int(version_range)
            version = dataset_row['version']
            if version_r == version:
                row['version_OK'] = "True"
            else:
                row['version_OK'] = "False"

        elif row['dataType'] == "SEQUENCE":  # validate chromosomes
            dataset_row = dataset_ENA.loc[i_accession]
            range.loc[ind, 'project_OK'] = np.where(row['project'] == dataset_row['project'], 'True', 'False')
            range.loc[ind, 'sample_OK'] = np.where(row['sample'] == dataset_row['sample ID'], 'True', 'False')
            project_row = Project[Project['i'] == i_accession]
            if project_row.empty:
                row['taxon_prj_OK'] = "Error"
                project_id = (row['project'], i_accession)
                project_error_list.append(project_id)
            else:
                taxon_prj = project_row['taxon'].values[0]
                range.loc[ind, 'taxon_prj_OK'] = np.where(row['taxon'] == taxon_prj, 'True', 'False')
            sample_row = Sample[Sample['i'] == i_accession]
            if sample_row.empty:
                row['taxon_sp_OK'] = "Error"
                sample_id = (row['sample'], i_accession)
                sample_error_list.append(sample_id)
            else:
                taxon_sp = sample_row['taxon'].values[0]
                range.loc[ind, 'taxon_sp_OK'] = np.where(row['taxon'] == taxon_sp, 'True', 'False')

        elif row['dataType'] == "CONTIGSET": # validate contigs
            dataset_row = dataset_ENA.loc[i_accession]
            range.loc[ind, 'project_OK'] = np.where(row['project'] == dataset_row['project'], 'True', 'False')
            range.loc[ind, 'sample_OK'] = np.where(row['sample'] == dataset_row['sample ID'], 'True', 'False')
            project_row = Project[Project['i'] == i_accession]
            if project_row.empty:
                row['taxon_prj_OK'] = "Error"
                project_id = (row['project'], i_accession)
                project_error_list.append(project_id)
            else:
                taxon_prj = project_row['taxon'].values[0]
                range.loc[ind, 'taxon_prj_OK'] = np.where(row['taxon'] == taxon_prj, 'True', 'False')
            sample_row = Sample[Sample['i'] == i_accession]
            if sample_row.empty:
                row['taxon_sp_OK'] = "Error"
                sample_id = (row['sample'], i_accession)
                sample_error_list.append(sample_id)
            else:
                taxon_sp = sample_row['taxon'].values[0]
                range.loc[ind, 'taxon_sp_OK'] = np.where(row['taxon'] == taxon_sp, 'True', 'False')
    if not project_error_list:
        pass
    else:
        p_error = project_error_list
        print("##WARNING PROJECT ERRORS##")
    if not sample_error_list:
        pass
    else:
        s_error = sample_error_list
        print("##WARNING SAMPLE ERRORS##")
    return range, p_error, s_error


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="step 2 script argparser")
    parser.add_argument('-p', '--project', help="Project to track DToL, ASG or ERGA", default="none")
    parser.add_argument('-w', '--workingdir', help="location of tracking file folders",
                        default="scripts/assemblytracking/")
    opts = parser.parse_args()

    os.chdir(opts.workingdir) # set the working directory
    # set the location of the tracking files
    tracking_files_path = f'{opts.project}-tracking-files' # set the location of the tracking files
    tracking_file_path = f'{tracking_files_path}/tracking_file.txt'

    print('''
    --------------------------------------------
      running step2 - releasing sequences ENA
    --------------------------------------------
    ''')

    #############
    ##  MAIN   ##
    #############
    # base url for ENA browser API
    base_url = 'https://www.ebi.ac.uk/ena/browser/api/summary/'
    tracking = pd.read_csv(tracking_file_path, sep='\t', index_col=0)  # import tracking file
    # to query the Browser API for taxID of project and export to a data frame
    print("Project")
    Project, Project_re = get_accessions('project', base_url)
    # to query the Browser API for taxID of sample and export to a data frame
    print("Sample")
    Sample, Sample_re = get_accessions('sample ID', base_url)

    # to query the Browser API for summary records of Contigs and export to a data frame
    print("Contigs")
    Contig_range, Contig_range_re = get_data('Contigs', base_url)
    # compare ids between project, sample, taxon and Contig_range
    Contig_range, Contig_Project_errors, Contig_sample_errors = validation(Contig_range, Project, Sample)
    Contig_range.to_csv(f'{tracking_files_path}/Contig_range.csv')  #, sep='\t'

    # to query the Browser API for summary records of Chr_range and export it to a data frame
    print("Chr range")
    Chr_range, Chr_range_re = get_data('Chromosomes', base_url)
    Chr_range, Chr_Project_errors, Chr_sample_errors = validation(Chr_range, Project, Sample)  # compare ids between project, sample, taxon and Chr_range
    Chr_range.to_csv(f'{tracking_files_path}/Chr_range.csv')

    # update info on tracking file for Contigs
    for ind, row in Contig_range.iterrows():
        accession = row['contigs']
        if (row['version_OK'] == "True" and row['project_OK'] == "True" and row['sample_OK'] == "True"
                and row['taxon_prj_OK'] == "True" and  row['taxon_sp_OK'] == "True"):
            tracking.loc[tracking['accessions'] == accession, 'Public in ENA'] = "Y"
            tracking.loc[tracking['accessions'] == accession, 'publicly available date'] = pd.to_datetime('today').strftime('%d/%m/%Y')
        else:
            pass

    # update info on tracking file for Chromosomes
    for ind in Chr_range.index:
        i_accession = Chr_range['i'][ind]
        Chr_range_i = Chr_range[Chr_range['i'] == i_accession]
        first_accession = Chr_range_i['accession'].iloc[0]
        last_accession = Chr_range_i['accession'].iloc[-1]
        accession = first_accession + "-" + last_accession

        if (Chr_range['version_OK'][ind] == "True" and Chr_range['project_OK'][ind] == "True" and
                Chr_range['sample_OK'][ind] == "True" and Chr_range['taxon_prj_OK'][ind] == "True" and
                Chr_range['taxon_sp_OK'][ind] == "True"):
            tracking.loc[tracking['accessions'] == accession, 'Public in ENA'] = "Y"
            tracking.loc[tracking['accessions'] == accession, 'publicly available date'] = pd.to_datetime(
                'today').strftime('%d/%m/%Y')
        else:
            pass


    # to query the Browser API for summary records of GCAs and export to a data frame
    print("GCA")
    GCA, GCA_re = get_data('GCA', base_url)
    GCA, p_error, s_error = validation(GCA, Project, Sample)  # compare ids between project, sample, taxon and GCA
    GCA.to_csv(f'{tracking_files_path}/GCA.csv')

    # update info on tracking file for GCAs
    for ind in GCA.index:
        accession = GCA['accession'][ind]
        if GCA['version_OK'][ind] == "True":
            tracking.loc[tracking['accessions'] == accession, 'Public in ENA'] = "Y"
            tracking.loc[tracking['accessions'] == accession, 'publicly available date'] = pd.to_datetime(
                'today').strftime('%d/%m/%Y')
        else:
            pass

    # to query the Browser API for summary records of analysis (metagenomes and binned metagenomes) and export to a data frame
    print("metagenomes")
    dataset_ENA = tracking[tracking["Public in ENA"] == "N"]
    analysis = pd.DataFrame()
    analysis_errors = pd.DataFrame()
    df_analysis_list, status_error_list_a = [], []
    for ind, row in dataset_ENA.iterrows():
        if row['Assembly type'] == "primary metagenome" or row['Assembly type'] == "binned metagenome":
            value = row['analysis ID']
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
            pass
        else:
            analysis = pd.concat(df_analysis_list, ignore_index=True)
        if not status_error_list_a:
            pass
        else:
            analysis_errors = pd.concat(status_error_list_a, ignore_index=True)


    # update info on tracking file for Analysis
    for ind in analysis.index:
        accession = analysis['accession'][ind]
        tracking.loc[tracking['analysis ID'] == accession, 'Public in ENA'] = "Y"
        tracking.loc[tracking['analysis ID'] == accession, 'publicly available date'] = pd.to_datetime(
            'today').strftime('%d/%m/%Y')


    ####################
    ##  FILE OUTPUTS  ##
    ####################
    # some of the file outputs list errors and need to be reported if present as part of this intermediate step.

    # read out Project
    Project_save_path = f'{tracking_files_path}/Project.txt'
    Project.to_csv(Project_save_path, sep="\t")

    # read out contig_range, contig_project_errors, contig_sample_erros to csv file
    Contig_range.to_csv(f'{tracking_files_path}/Contig_range.txt', sep="\t")
    # read out chr_range, chr_project_errors, chr_sample_erros to csv file
    Chr_range.to_csv(f'{tracking_files_path}/Chr_range.txt', sep="\t")
    # write GCA to file at this point
    GCA.to_csv(f'{tracking_files_path}/GCA.txt', sep="\t")

    # save updated tracking file
    tracking.to_csv(tracking_file_path, sep="\t")

