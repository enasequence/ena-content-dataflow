# !/usr/bin/env python3

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

import argparse
import requests
import sys
import pandas as pd
import json
from datetime import datetime, date
import os
from argparse import RawTextHelpFormatter
import config

description = """
This script links source (e.g. ENA) biosamples with target (e.g. EGA) biosamples, using the "derived from" relationships field
The script currently supports a 1:1 linking of source to target biosample
"""

usage = """
Usage: biosamples_relationships.py <OPTIONS>
Options:
    -s, --spreadsheet : (mandatory) filename for spreadsheet (csv or .txt) containing source and target biosample accessions, with 1 source and 1 corresponding target accession per line.
    -prod            : (optional) link biosamples in production (if -prod not specified, biosamples will be linked in development by default)
"""

example = """
Example 1: link biosamples in development environment:
    python3 biosamples_relationships.py -s test_sample_accs.txt

Example 2: link biosamples in production:
    python3 biosamples_relationships.py -s test_sample_accs.txt -prod
"""

parser = argparse.ArgumentParser(
    description=description+usage+example, formatter_class=RawTextHelpFormatter
)

parser.add_argument('-s', '--spreadsheet', help='input spreadsheet containing source and target biosample accessions', type=str,
                    required=True)
parser.add_argument('-prod', '--production', help='Biosamples production environment', action='store_true')  # 'dev' env is default if prod not specified
args = parser.parse_args()

##TODO: Dipayan mentioned adding the "webinSubmissionAccountId" in the sample json when updating it to retain original owner of sample - to be fixed by mid-Aug

#TODO: how can we connect this script to the creation of the ENA 'sample derived from' attribute?

root_user = config.root_user
root_pwd = config.root_pwd
#print(root_user)
#print(root_pwd)

# creating output dir
def prepare_directory():
    global path
    path = "./biosamples_output"
    try:
        os.mkdir(path) #code inside the try block will execute when there is no error in the program
        print("Successfully created directory %s " % os.path.abspath(path))
    except OSError:
        print("Creation of directory %s failed" % path) #code here will execute when there is an error in the try block
prepare_directory()

# obtaining Webin auth token
auth_url = 'https://wwwdev.ebi.ac.uk/ena/submit/webin/auth/token'
header = {"Content-Type": "application/json"}
data = json.dumps(dict([("authRealms", [ "ENA" ]), ("password", root_pwd), ("username", root_user)]))
print(data)

response = requests.post(auth_url, headers=header, data=data)
token = response.content.decode('utf-8') #to decode from bytes (indicated by b') to string
print("Token is " + token)

if response.status_code != 200:
    print("\n Error generating token, see below:")
    print(response.content)
    sys.exit()

# input list of source + target accessions:
df = pd.read_csv(args.spreadsheet, sep='\t')
print("Input accessions list:\n")
print(df)

source_accs = df['ena_biosample_id'].tolist()
target_accs = df['ega_biosample_id'].tolist()


print()
for row in range(len(df)):
    print("linking ENA Biosample " + df.iloc[row,0] + " with EGA Biosample " + df.iloc[row,1])

# download SOURCE/ENA sample metadata:
if args.production:
    biosamples_start = 'https://www.ebi.ac.uk/biosamples/samples/'
else:
    biosamples_start = 'https://wwwdev.ebi.ac.uk/biosamples/samples/'

for i in range(len(df)):
    ena_bs, ega_bs = source_accs[i], target_accs[i]
    biosamples_url = "{0}{1}".format(biosamples_start, ena_bs)
    r = requests.get(biosamples_url) #no auth token needed
    #print(r.text)
    with open(os.path.join(path, f"{ena_bs}.json"), "w") as f:
        f.write(r.text) #saves sample json object to file

# edit SOURCE/ENA sample metadata to include relationships block
    with open(os.path.join(path, f"{ena_bs}.json")) as f:
        file_data = json.load(f) #load existing json data and returns it as a dictionary
        #print(file_data)
    file_data.pop("_links") #removes links array (will be added automatically after updating the biosample)
     #note the below code assumes a 1:1 mapping between ENA:EGA biosamples
    array = {"relationships": [{"source": ena_bs, "type": "derived from", "target": ega_bs}]}  #.tolist() preserves order, so mapping between ENA->EGA biosample accession should also be preserved
    file_data.update(array)

    with open(os.path.join(path, f"linked_{ena_bs}.json"), 'w') as f:
        json.dump(file_data, f, indent=1) #converts python dictionary back into json string
       # NOTE: The above overwrites original json file - so if it originally had a relationships array this will now be modified
       # otherwise, relationship block is appended to end of json file

# submit updated Biosample json file:
    webin_auth = "?authProvider=WEBIN"
    update_url = "{0}{1}".format(biosamples_url, webin_auth)
    headers = {"Content-Type": "application/json;charset=UTF-8", "Accept": "application/hal+json", "Authorization": "Bearer {0}".format(token)}
    r = requests.put(update_url, headers=headers, data=open(os.path.join(path, f"linked_{ena_bs}.json"), 'rb'))

# error messages:
    if r.status_code == 200:
        print('Biosamples successfully linked. Source sample: ' + ena_bs + ' derived from target sample: ' + ega_bs)
        print(r.text)
    else:
        print('Biosamples linking failed. See error file')
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%y_%H_%M_%S")
        with open(os.path.join(path, f"error_{dt_string}.txt"), "wb") as e:
            error_file = e.write(r.content)
