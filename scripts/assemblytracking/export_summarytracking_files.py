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

import os, sys, argparse
import pandas as pd
import datetime


# TODO: see if this can just be one function in another script

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="sql_processingatENA")
    parser.add_argument('-p', '--project', help="Project to track DToL, ASG or ERGA", default="none")
    parser.add_argument('-w', '--workingdir', help="location of tracking file folders",
                        default="scripts/assemblytracking/")
    opts = parser.parse_args()
    '''
    --------------------------------------
    running export summary
    --------------------------------------
        '''
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
    # import GCA Public file
    PublicGCA_old = pd.read_csv(f'{tracking_files_path}/Public_GCAs.txt', sep='\t')

    # create summary public assemblies dataframe
    tracking = pd.read_csv(tracking_file_path, sep='\t', index_col=0) # import the tracking file
    GCA = tracking[tracking['accession type'] == "GCA"]
    PMET = tracking[tracking['Assembly type'] == 'primary metagenome']
    BMET = tracking[tracking['Assembly type'] == 'binned metagenome']
    GCA_MET = pd.concat([GCA, PMET, BMET], ignore_index=True)
    del GCA, PMET, BMET

    GCA_public = GCA_MET[GCA_MET['Public in ENA'] == "Y"]
    today = datetime.date.today().strftime('%d/%m/%Y')
    PublicGCA_new = GCA_public[GCA_public['publicly available date'] == today]
    #timestring = '17/09/2024'
    #date = timestring.strptime('%d/%m/%Y')
    #PublicGCA_new = GCA_public[GCA_public['publicly available date'] >= date]

    # drop columns - replace multiple column drops with integer based range dropping
    PublicGCA_new = PublicGCA_new.drop(PublicGCA_new.iloc[:, 13:17], axis=1)
    PublicGCA_new.rename(columns={'accessions': 'GCA ID'}, inplace=True)

    PublicGCA_new['contig range'] = ""
    PublicGCA_new['chromosome range'] = ""
    for ind in PublicGCA_new.index:
        index = PublicGCA_new["index"][ind]
        tracking_set = tracking[tracking['index'] == index]
        #print(tracking_set)
        for i in tracking_set.index:
            if tracking_set['accession type'][i] == "Contigs":
                PublicGCA_new['contig range'][ind] = tracking_set["accessions"][i]
            if tracking_set['accession type'][i] == "Chromosomes":
                PublicGCA_new['chromosome range'][ind] = tracking_set['accessions'][i]

    # join the new accessions with existing tracking file
    PublicGCA = pd.concat([PublicGCA_old, PublicGCA_new], ignore_index=True)
    PublicGCA = PublicGCA.drop(['Unnamed: 0'], axis=1)

    # create summary of assemblies being released per phase
    GCAnotPublic = GCA_MET[GCA_MET['Public in ENA'] == "N"]
    GCAnotPublic = GCAnotPublic.drop(['Public in ENA'], axis=1)
    GCAnotPublic = GCAnotPublic.drop(['Public in NCBI'], axis=1)
    GCAnotPublic = GCAnotPublic.drop(['Linked to Project'], axis=1)
    GCAnotPublic = GCAnotPublic.drop(['Linked to Sample'], axis=1)
    GCAnotPublic['contig range'] = ""
    GCAnotPublic['chromosome range'] = ""
    GCAnotPublic['phase'] = ""
    for ind in GCAnotPublic.index:
        index = GCAnotPublic["index"][ind]
        tracking_set = tracking[tracking['index'] == index]
        #print(tracking_set)
        for i in tracking_set.index:
            if tracking_set['accession type'][i] == "Contigs":
                GCAnotPublic['contig range'][ind] = tracking_set["accessions"][i]
            if tracking_set['accession type'][i] == "Chromosomes":
                GCAnotPublic['chromosome range'][ind] = tracking_set['accessions'][i]
            if tracking_set['accession type'][i] == "GCA" and tracking_set['Public in NCBI'][i] == "Y":
                GCAnotPublic['phase'][ind] = "Releasing GCAs"
            if tracking_set['accession type'][i] == "GCA" and tracking_set['Public in NCBI'][i] == "N":
                if GCAnotPublic['phase'][ind] == "":
                    GCAnotPublic['phase'][ind] = "Processing at NCBI"
            if tracking_set['accession type'][i] != "GCA" and tracking_set['Public in ENA'][i] == "N":
                GCAnotPublic['phase'][ind] = "Releasing sequences"
    # see if these if shouldn't be elif
    Releasing_GCA = GCAnotPublic[GCAnotPublic['phase'] == "Releasing GCAs"]
    Processing_NCBI = GCAnotPublic[GCAnotPublic['phase'] == "Processing at NCBI"]
    Releasing_seq = GCAnotPublic[GCAnotPublic['phase'] == "Releasing sequences"]

    ## gets publicly available csv with publicly available assembly records listed and the date
    ## essentially plan here is to parse full tracking file and tidy it all up...
    #publicly_av_obj = []
    dup_name = pd.DataFrame()
    tracking = pd.read_csv(tracking_file_path, sep='\t', index_col=0)  # import the tracking file
    tracking_index_unq = tracking['name'].unique()
    for x in tracking_index_unq:
        sub_df = tracking.loc[tracking['name'] == x]
        num_rows = sub_df.shape[0]
        if num_rows > 3:
            dup_name = pd.concat([dup_name,sub_df])
    dup_name.to_csv(f'{tracking_files_path}/dup_name.csv')

    ####################
    ##  FILE OUTPUTS  ##
    ####################

    # save summary tracking file
    PublicGCA.to_csv(f'{tracking_files_path}/Public_GCAs.txt', sep="\t")
    Releasing_GCA.to_csv(f'{tracking_files_path}/Releasing_GCAs.txt', sep="\t")
    Processing_NCBI.to_csv(f'{tracking_files_path}/Processing_NCBI.txt', sep="\t")
    Releasing_seq.to_csv(f'{tracking_files_path}/Releasing_sequences.txt', sep="\t")




