#!/usr/bin/python

# Copyright [2021-2024] EMBL-European Bioinformatics Institute
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
#
# Authored by Carla Cummins @carlacummins


# Use this script to generate portal-compatible CSV files from JSON
# pathogen definition files

import pandas as pd
import sys

# read JSON file
jsonfile = sys.argv[1]
df_j = pd.read_json(jsonfile)

# detect and extract columns of interest for portal
try:
    df_j['classification']
    columns = ['taxon_id','name','classification','taxon_rank','source']
    headers = ['Taxonomy ID','Scientific Name','Type','Rank','Source']
except KeyError:
    columns = ['taxon_id','name','taxon_rank','source']
    headers = ['Taxonomy ID','Scientific Name','Rank','Source']

df_c = df_j.to_csv(columns=columns, header=headers, index=False)
print(df_c.strip())