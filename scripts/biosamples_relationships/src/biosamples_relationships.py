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

# -------- #
# Hard-coded variables
# -------- #
output_dir_name = 'biosamples_output'
# Whether to do a check or not if "-prod" is given
interactive_check_production = True
# Key names within the credentials file
user_str = 'username'
pwd_str = 'password'
# Parts of the URLs and response headers
auth_url = 'https://wwwdev.ebi.ac.uk/ena/submit/webin/auth/token'
header = {"Content-Type": "application/json"}
webin_auth = "?authProvider=WEBIN"
# Column headers to be expected at the input file:
source_col_name = "source_biosample_id"
target_col_name = "target_biosample_id"

# -------- #
# Parsing given arguments
# -------- #
description = """
This script links source (e.g. viral ENA) biosamples with target (e.g. human EGA) biosamples, using the "derived from" relationships field (i.e. 'source sample derived from target sample').
The script currently supports a 1:1 linking of source to target biosample. 
"""

example = """
Example 1: link biosamples in development environment:
    python3 biosamples_relationships.py -s test_sample_accs.txt
Example 2: link biosamples in production:
    python3 biosamples_relationships.py -s test_sample_accs.txt -prod
"""

parser = argparse.ArgumentParser(prog = "biosamples_relationships.py",
                                 description = description,
                                 formatter_class = RawTextHelpFormatter,
                                 epilog = example

)

parser.add_argument('-s', '--spreadsheet-file',
                    dest = 'spreadsheet',
                    help='(required) filename for spreadsheet (csv or .txt) containing source and target biosample accessions, with 1 source and 1 corresponding target accession per line.', 
                    type=str,
                    required=True)

parser.add_argument('-c', '--credentials-file',
                    dest = 'credentials_file',
                    nargs = '?', # 0 or 1 arguments
                    default = 'credentials.json',
                    help = """(required) JSON file containing the credentials (either root or original owner credentials - see data/test_credentials.json for its format) for the linkage to be pushed (default: 
                    "credentials.json")""",
                    required=True)

parser.add_argument('-prod', '--production', help='(optional) link biosamples in production (if -prod not specified, biosamples will be linked in development by default).', action='store_true')  # 'dev' env is default if prod not specified

parser.add_argument('--verbose',
                    action='store_true',
                    default = False,
                    help="""A boolean switch to add verbosity to the scripts (printing initial token, source and target lists...)""")


args = parser.parse_args()

# -------- #
# Input checks
# -------- #
# Check that credentials file exist and is a json file:
if not os.path.isfile(args.credentials_file):
    print(f"- ERROR in biosamples_relationships.py: given credentials file '{args.credentials_file}' does not exist.", file=sys.stderr)
    sys.exit()
if not os.path.splitext(args.credentials_file)[1] == ".json":
    print(f"- ERROR in biosamples_relationships.py: given credentials file '{args.credentials_file}' is not a JSON file.", file=sys.stderr)
    sys.exit()

# Check that input file exists and is a .csv/.txt file:
if not os.path.isfile(args.spreadsheet):
    print(f"- ERROR in biosamples_relationships.py: given input file '{args.spreadsheet}' does not exist.", file=sys.stderr)
    sys.exit()

inp_file_ending = os.path.splitext(args.spreadsheet)[1]
if not inp_file_ending == ".txt" and not inp_file_ending == ".csv":
    print(f"- ERROR in biosamples_relationships.py: given input file '{args.spreadsheet}' is not a '.txt' or '.csv' file.", file=sys.stderr)
    sys.exit()

# If the chosen instance is production, check interactively that the user did intend to modify it in production.
if interactive_check_production and args.production:
    u_response = input("\n- WARNING: Changes will be applied to the production instance of BioSamples ('-prod' was given as an argument). Do you wish to proceed? [y/n]")
    if not u_response == "y":
        print("\t Aborting script.")
        sys.exit()

# -------- #
# Code blocks
# -------- #
def print_v(text_to_print):
    """ Function to print a given text based on chosen verbosity.
    """
    if args.verbose:
        print(text_to_print)

# Reading credentials
def load_json_file(file):
    """ Function to load (and return) a JSON file (given as input) as a dictionary. 
    """
    with open(file, "r") as f:
        loaded_dict = json.load(f)
        
    return loaded_dict

credentials_dict = load_json_file(file = args.credentials_file)
credentials_user = credentials_dict[user_str]
credentials_pwd = credentials_dict[pwd_str]

# Check if output_dir where the output_xml will reside exists, create it if not.
#   Get the root directory of the tool.
root_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
output_dir = os.path.join(root_dir, output_dir_name)
dir_exists = os.path.isdir(output_dir)
if not dir_exists and output_dir != '':
    try:
        os.makedirs(output_dir)
        print_v(f"Successfully created output directory '{output_dir}'.")
    except:
        print(f"\n- ERROR in biosamples_relationships.py: could not create output directory '{output_dir}'. Aborting script.", file=sys.stderr)
        exit()   

# Obtaining Webin auth token
data = json.dumps(dict([("authRealms", [ "ENA" ]), ("password", credentials_pwd), ("username", credentials_user)]))
response = requests.post(auth_url, headers = header, data = data)
token = response.content.decode('utf-8') # To decode from bytes (indicated by b') to string

headers = {"Content-Type": "application/json;charset=UTF-8", "Accept": "application/hal+json", "Authorization": "Bearer {0}".format(token)}
print_v(f"Obtained token is:\n{token}\n")

if response.status_code != 200:
    response_content = response.content.decode('utf-8')
    print(f"\n - ERROR when generating token. Aborting script. See response's content below:\n{response_content}", file=sys.stderr)
    sys.exit()

#! TBD - Add XLSX as input
# input list of source + target accessions:
df = pd.read_csv(args.spreadsheet, sep='\t')
print_v(f"Input accession list (sources and targets that will be linked):\n{df}\n")
source_accs = df[source_col_name].tolist()
target_accs = df[target_col_name].tolist()

# Download SOURCE/ENA sample metadata:
if args.production:
    biosamples_start = 'https://www.ebi.ac.uk/biosamples/samples/'
else:
    biosamples_start = 'https://wwwdev.ebi.ac.uk/biosamples/samples/'

#! TBD - Sort input and group sources together, avoiding repeated calls for
#          the same source and different targets. 
for i in range(len(df)):
    # Iterate over the dataframe and get the source and target BSD IDs
    source_bs, target_bs = source_accs[i], target_accs[i]
    biosamples_url = "{0}{1}".format(biosamples_start, source_bs)
    r = requests.get(biosamples_url) # No auth token needed
    source_json_file = os.path.join(output_dir, f"{source_bs}.json")
    with open(source_json_file, "w") as f:
        f.write(r.text) # Saves sample json object to file
    
# Edit SOURCE/ENA sample metadata to include relationships block
    file_data = json.loads(r.text)
    file_data.pop("_links") # Removes links array (will be added automatically after updating the biosample)
    new_relationship = {"source": source_bs, "type": "derived from", "target": target_bs}

    # We check if the "relationships" node is already in the json file.
    if "relationships" in file_data:
        rel_list = file_data["relationships"]
    else:
        rel_list = []

    # We append the new relationship to the list (only if it's not already there) and update the JSON file
    if new_relationship in rel_list:
        print_v(f"- WARNING: Relationship of source sample '{source_bs}' derived from target sample '{target_bs}' already existed in the downloaded JSON file. Skipping.")
        continue
    
    rel_list.append(new_relationship)
    rel_array = {"relationships": rel_list}

    file_data.update(rel_array)

    edited_source_file = os.path.join(output_dir, f"linked_{source_bs}.json")
    with open(edited_source_file, 'w') as f:
        json.dump(file_data, f, indent = 1) # Converts python dictionary back into json string

# Submit updated Biosample json file:    
    update_url = "{0}{1}".format(biosamples_url, webin_auth)  
    r = requests.put(update_url, headers = headers, data = json.dumps(file_data))

# error messages:
    if r.status_code == 200:
        print_v(f"-- Biosamples successfully linked. Source sample '{source_bs}' derived from target sample '{target_bs}'.")
    else:
        print(f"- ERROR: Biosamples linking failed (error code {r.status_code}) for Source sample '{source_bs}' derived from target sample '{target_bs}'. See error file", file=sys.stderr)
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%y_%H_%M_%S")
        with open(os.path.join(output_dir, f"error_{source_bs}_{target_bs}_{dt_string}.log"), "wb") as e:
            error_file = e.write(r.content)