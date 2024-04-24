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

import os
import numpy as np
import pandas as pd

# 1) set the working directory (depends if tracking ASG or DToL or ERGA)

#os.chdir('c:\Data\EMBL-EBI\DToL\Assembly_tracking')
#os.chdir('c:\Data\EMBL-EBI\ASG\Assembly_tracking')
#os.chdir('c:\Data\EMBL-EBI\ERGA\Assembly_tracking')
#os.chdir('c:/Users/jasmine/Documents/ASG/Assembly tracking')

# import GCA Public file
PublicGCA_old = pd.read_csv('Public_GCAs.txt', sep='\t')
#last_index_row = PublicGCA_old[-1]
#last_index = last_index_row['index']

# import tracking file
tracking = pd.read_csv('tracking_file.txt', sep='\t')
tracking = tracking.drop(['Unnamed: 0'], axis=1)

# create summary public assemblies dataframe
GCA = tracking[tracking['accession type'] == "GCA"]
PMET = tracking[tracking['Assembly type'] == 'primary metagenome']
BMET = tracking[tracking['Assembly type'] == 'binned metagenome']
frames = [GCA, PMET, BMET]
GCA_MET = pd.concat(frames, ignore_index=True)
GCA_MET.sort_values(by=['index'])
GCA_public = GCA_MET[GCA_MET['Public in ENA'] == "Y"]
PublicGCA_new = GCA_public[GCA_public['publicly available date'] == "26/03/2024"]
#PublicGCA_new = GCA_public[GCA_public['publicly available date'] == pd.to_datetime('today')]
PublicGCA_new = PublicGCA_new.drop(['Public in ENA'], axis=1)
PublicGCA_new = PublicGCA_new.drop(['Public in NCBI'], axis=1)
PublicGCA_new = PublicGCA_new.drop(['Linked to Project'], axis=1)
PublicGCA_new = PublicGCA_new.drop(['Linked to Sample'], axis=1)
PublicGCA_new = PublicGCA_new.drop(['accession type'], axis=1)
PublicGCA_new.rename(columns={'accessions': 'GCA ID'}, inplace=True)
PublicGCA_new['contig range'] = ""
PublicGCA_new['chromosome range'] = ""
for ind in PublicGCA_new.index:
    index = PublicGCA_new["index"][ind]
    tracking_set = tracking[tracking['index'] == index]
    print(tracking_set)
    for i in tracking_set.index:
        if tracking_set['accession type'][i] == "Contigs":
            PublicGCA_new['contig range'][ind] = tracking_set["accessions"][i]
        if tracking_set['accession type'][i] == "Chromosomes":
            PublicGCA_new['chromosome range'][ind] = tracking_set['accessions'][i]

#join the new accessions with existing tracking file
frames = [PublicGCA_old, PublicGCA_new]
PublicGCA = pd.concat(frames, ignore_index=True)
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
    print(tracking_set)
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

# save summary tracking file
PublicGCA.to_csv('Public_GCAs.txt', sep="\t")
Releasing_GCA.to_csv('Releasing_GCAs.txt', sep="\t")
Processing_NCBI.to_csv('Processing_NCBI.txt', sep="\t")
Releasing_seq.to_csv('Releasing_sequences.txt', sep="\t")