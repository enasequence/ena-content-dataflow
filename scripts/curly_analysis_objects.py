#!/usr/bin/python

# Copyright [2021] EMBL-European Bioinformatics Institute
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



import requests

### User must point to correct absolute path for input file ####
### Input file should be formatted as one sample accession per line ###

with open('C:/Users/ocathail.EBI/PycharmProjects/curly/input_list.txt.txt', 'r') as in_file:
    sample_list = []
    for line in in_file:
        line = line.strip('\n')
        sample_list.append(line)
    string1 = "sample_accession%3D%22"
    string2 = "%22%20OR%20"
    new_sample_list = [string1 + x + string2 for x in sample_list]
    second_fix = str(new_sample_list[-1:]).replace('%20OR%20', '')
    new_sample_list.pop()
    full_query = ''.join(new_sample_list)
    size = len(full_query)
    final_query = full_query[:size - 8]

url = "https://www.ebi.ac.uk/ena/portal/api/search"
headers = {'Content-Type': 'application/x-www-form-urlencoded'}
payload = {'result': 'analysis', 'query': str(final_query), 'format': 'tsv'}

r = requests.post(url, data=payload, headers=headers)
results = r.text 

with open('output.txt', 'w', encoding="utf-8") as out_file:
    out_file.write(results)
