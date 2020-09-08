#!/usr/bin/env python3.7

# Copyright [2020] EMBL-European Bioinformatics Institute
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

import sys, requests, json

"""
This script is designed to help to detect issues with data display on the ENA browser.
For a given set of project accessions, it fetches the file report from the ENA web API 
and reports on the following:
1. the taxon id and count of each sample in the given project(s)
2. the FastQ file counts, to check that a consistent number of files are
   present for each run. Reports an error if different file counts are detected.

"""

debug = False
project_accs = sys.argv[1:]
try:
    project_accs[0]
except IndexError:
    print("Usage: check_project_reads.py <project_id1> <project_id2> ... <project_idN>\n")

if '--debug' in project_accs:
    debug = True
    project_accs.remove('--debug')

for project_acc in project_accs:
    url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?accession={project_acc}&result=read_run&fields=sample_accession,experiment_accession,run_accession,tax_id,scientific_name,fastq_ftp,sra_ftp&format=json&download=false"

    content = requests.get(url)
    try:
        data = json.loads(content.content)
    except json.decoder.JSONDecodeError:
        print(f"Project {project_acc} is empty!")
        sys.exit(0)

    if debug:
        print(f"{data}\n\n")

    run_count = len(data)
    print(f"{project_acc} showing {run_count} runs\n")

    tax_id_summary = {}
    file_count_summary = {}
    for run in data:
        try:
            tax_id_summary[f"{run['scientific_name']} (taxon_id {run['tax_id']})"] += 1
        except KeyError:
            tax_id_summary[f"{run['scientific_name']} (taxon_id {run['tax_id']})"] = 1
        
        fastq_files_count = run['fastq_ftp'].count('.fastq.gz')
        try:
            file_count_summary[fastq_files_count].append(run['run_accession'])
        except KeyError:
            file_count_summary[fastq_files_count] = [run['run_accession']]

    print("Taxon ID Summary:")
    for k in tax_id_summary.keys():
        print(f"\t- {tax_id_summary[k]} samples with taxonomy {k}")
    print("\n")

    print("FastQ File Check:")
    if len(file_count_summary) == 1:
        this_file_count = list(file_count_summary.keys())[0]
        print(f"\t- All runs have consistent file counts : {this_file_count}")
    else:
        print("\t- Inconsistent file counts detected!!")
        for c in file_count_summary:
            print(f"\t\t* {c} files : runs {file_count_summary[c]}")
    print("\n")
