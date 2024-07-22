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
# pathogen definition files from various sources

import pandas as pd
import sys
import json
from ena_utils import *
from difflib import SequenceMatcher as SM
from tqdm import tqdm

priority_sources = ['WHO']
if '--no-validate' in sys.argv:
    validate_taxon_ids = False
    sys.argv.remove('--no-validate')
else:
    validate_taxon_ids = True

def print_summary(df, tag):
    a = f"{'-'*10} {tag} {'-'*10}"
    print(a)
    print(df.groupby(['classification', 'source']).size().to_frame().to_string())
    print(f"{'-'*len(a)}\n")

def raise_taxon_error(taxon_id, ratio, given, got):
    sys.stderr.write(f"""

ERROR: Name doesn't seem to match for taxon id {taxon_id} (similarity ratio = {ratio})
Given : {given}
Got   : {got}


    """)
    sys.exit(1)

# read JSON file
jsonfiles = sys.argv[1:]
all_taxa = []
all_classifications = {}
for jsonfile in jsonfiles:
    with open(jsonfile) as jfh:
        json_data = json.load(jfh)

    # validate and format each taxon record
    if validate_taxon_ids:
        print(f"Validating taxon IDs against taxonomy for {jsonfile}")
    else:
        print(f"Loading taxa from {jsonfile}")

    c = 0
    for t in tqdm(json_data['taxa']):
        # check name vs taxonomy
        if validate_taxon_ids:
            qt = query_taxonomy_by_id(t['taxon_id'])
            name_similarity = SM(None, t['name'], qt['scientificName']).ratio()
            if name_similarity < 0.75:
                raise_taxon_error(t['taxon_id'], name_similarity, t['name'], qt['scientificName'])

        # formatting
        t['source'] = json_data['source_short_name']
        t['taxon_rank'] = t['taxon_rank'].capitalize()
        t['classification'] = t['classification'].capitalize()

        # record all classifications found here
        all_classifications[t['classification']] = 1

        c+=1
        all_taxa.append(t)
    print(f"---> complete ({c} taxa loaded)")

# load into pandas
pathogens_df = pd.DataFrame.from_dict(all_taxa)
pathogens_df = pathogens_df.drop(columns=['notes', 'hazard_group', 'hse_hazard_group', 'common_name']).drop_duplicates()


# set headers for CSV files
columns = ['taxon_id','name','classification','taxon_rank','source']
headers = ['Taxonomy ID','Scientific Name','Type','Rank','Source']

# filter priority sources before grouping
priority_df = pathogens_df.query("source == @priority_sources")
grp_priority_df = priority_df.groupby(['taxon_id', 'name','taxon_rank', 'classification'])['source'].apply(', '.join).reset_index()
grp_priority_df.to_csv('priority.csv', columns=columns, header=headers, index=False)

grp_pathogens_df = pathogens_df.groupby(['taxon_id', 'name','taxon_rank', 'classification'])['source'].apply(', '.join).reset_index()
print_summary(grp_pathogens_df, 'Summary Stats')
print_summary(grp_priority_df, 'Priority Pathogens')

for cl in all_classifications.keys():
    this_class_df = pathogens_df.query(f"classification == '{cl}'")
    grouped_df = this_class_df.groupby(['taxon_id', 'name','taxon_rank', 'classification'])['source'].apply(', '.join).reset_index()
    grouped_df.to_csv(f"{cl.lower()}.csv", columns=columns, header=headers, index=False)