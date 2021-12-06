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
# Environmental variables
env_user_str = 'bsd_username'
env_pwd_str = 'bsd_password'
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

parser = argparse.ArgumentParser(prog="biosamples_relationships.py",
                                 description=description,
                                 formatter_class=RawTextHelpFormatter,
                                 epilog=example

                                 )

parser.add_argument('-s', '--spreadsheet-file',
                    dest='spreadsheet',
                    help='(required) filename for spreadsheet (csv or .txt) containing source and target biosample accessions, with 1 source and 1 corresponding target accession per line.',
                    type=str,
                    required=True)

parser.add_argument('-c', '--credentials-file',
                    dest='credentials_file',
                    nargs='?',  # 0 or 1 arguments
                    help=f"""(optional) JSON file containing the credentials (either root or original owner credentials - see data/test_credentials.json for its format) for the linkage to be pushed (default: "credentials.json"). If not given, environment variables '{env_user_str}' and '{env_pwd_str}' will be used.""",
                    required=False)

parser.add_argument('-u', '--unlink-samples',
                    dest='unlink',
                    action='store_true',
                    help=f"""(optional) remove relationships field from source samples""",
                    required=False)

parser.add_argument('-prod', '--production',
                    help='(optional) link biosamples in production (if -prod not specified, biosamples will be linked in development by default).',
                    action='store_true')  # 'dev' env is default if prod not specified

parser.add_argument('--verbose',
                    action='store_true',
                    default=False,
                    help="""A boolean switch to add verbosity to the scripts (printing initial token, source and target lists...)""")

args = parser.parse_args()

# -------- #
# Input checks
# -------- #
# Check that credentials file exist and is a json file:
if not args.credentials_file == None and not os.path.isfile(args.credentials_file):
    print(f"- ERROR in biosamples_relationships.py: given credentials file '{args.credentials_file}' does not exist.",
          file=sys.stderr)
    sys.exit()
if not args.credentials_file == None and not os.path.splitext(args.credentials_file)[1] == ".json":
    print(
        f"- ERROR in biosamples_relationships.py: given credentials file '{args.credentials_file}' is not a JSON file.",
        file=sys.stderr)
    sys.exit()

# If a credentials file is not given, check the environmental variables
if args.credentials_file == None:
    if not env_user_str in os.environ:
        print(
            f"- ERROR in biosamples_relationships.py: no credentials file was given (option '-c'), but environmental variable '{env_user_str}' was not found either.",
            file=sys.stderr)
        sys.exit()
    if not env_pwd_str in os.environ:
        print(
            f"- ERROR in biosamples_relationships.py: no credentials file was given (option '-c'), but environmental variable '{env_pwd_str}' was not found either.",
            file=sys.stderr)
        sys.exit()

# Check that input file exists and is a .csv/.txt file:
if not os.path.isfile(args.spreadsheet):
    print(f"- ERROR in biosamples_relationships.py: given input file '{args.spreadsheet}' does not exist.",
          file=sys.stderr)
    sys.exit()

inp_file_ending = os.path.splitext(args.spreadsheet)[1]
if not inp_file_ending == ".txt" and not inp_file_ending == ".csv":
    print(
        f"- ERROR in biosamples_relationships.py: given input file '{args.spreadsheet}' is not a '.txt' or '.csv' file.",
        file=sys.stderr)
    sys.exit()

# If the chosen instance is production, check interactively that the user did intend to modify it in production.
if interactive_check_production and args.production:
    u_response = input(
        "\n- WARNING: Changes will be applied to the production instance of BioSamples ('-prod' was given as an argument). Do you wish to proceed? [y/n]")
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

def relationships(file_data, source_bs, target_list, r_type="derived from", node_name="relationships"):
    """ Fuction to add new relationships to a given node in a JSON file, based on a given source and target list.
        Returns a list of new relationships that can be used to update the file.

        Parameters:
            - file_data (JSON): JSON object to be updated.
            - source_bs (string): source of the relationship (e.g. SAMEA7616999)
            - target_list (list): list of targets of the relationship with the source (e.g. ['SAMEA6941288', 'SAMEA8698068'])
            - r_type (string): type of relationship (by default 'derived from')
            - node_name (string): node of the JSON file to add relationships to (by default 'relationships').
    """

 # We check if the node ("relationships") already exists in the JSON file.
    if node_name in file_data:
        rel_list = file_data[node_name]
    else:
        rel_list = []

    for target_bs in target_list:
        global new_relationship
        new_relationship = {"source": source_bs, "type": r_type, "target": target_bs}

    return rel_list

if not args.credentials_file == None:
    credentials_dict = load_json_file(file=args.credentials_file)
    credentials_user = credentials_dict[user_str]
    credentials_pwd = credentials_dict[pwd_str]
else:
    credentials_user = os.environ[env_user_str]
    credentials_pwd = os.environ[env_pwd_str]

# Check if output_dir where the output_xml will reside exists, create it if not.
#   Get the root directory of the tool.
root_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))  # prints root directory
output_dir = os.path.join(root_dir, output_dir_name)  # creates biosamples_output folder in root directory
dir_exists = os.path.isdir(output_dir)  # T or F
if not dir_exists and output_dir != '':
    try:
        os.makedirs(output_dir)
        print_v(f"Successfully created output directory '{output_dir}'.")
    except:
        print(
            f"\n- ERROR in biosamples_relationships.py: could not create output directory '{output_dir}'. Aborting script.",
            file=sys.stderr)
        exit()

# Obtaining Webin auth token
data = json.dumps(dict([("authRealms", ["ENA"]), ("password", credentials_pwd), ("username", credentials_user)]))
response = requests.post(auth_url, headers=header, data=data)
token = response.content.decode('utf-8')  # To decode from bytes (indicated by b') to string

headers = {"Content-Type": "application/json;charset=UTF-8", "Accept": "application/hal+json",
           "Authorization": "Bearer {0}".format(token)}
print_v(f"Obtained token is:\n{token}\n")

if response.status_code != 200:
    response_content = response.content.decode('utf-8')
    print(f"\n - ERROR when generating token. Aborting script. See response's content below:\n{response_content}",
          file=sys.stderr)
    sys.exit()

# ! TBD - Add XLSX as input
# input list of source + target accessions:
df = pd.read_csv(args.spreadsheet, sep='\t')
print_v(f"Input accession list (sources and targets that will be linked):\n{df}\n")
df_dict = {}
for source_bs, target in zip(df[source_col_name], df[target_col_name]):
    if source_bs not in df_dict:
        df_dict[source_bs] = [target] #list of target accessions

    else:
        df_dict[source_bs].append(target)

# Download SOURCE/ENA sample metadata:
if args.production:
    biosamples_start = 'https://www.ebi.ac.uk/biosamples/samples/'
else:
    biosamples_start = 'https://wwwdev.ebi.ac.uk/biosamples/samples/'

for source_bs in df_dict.keys():
    # Iterate over the dictionary keys (the source BSD IDs) and their lists of targets
    biosamples_url = "{0}{1}".format(biosamples_start, source_bs)
    r = requests.get(biosamples_url)  # No auth token needed
    source_json_file = os.path.join(output_dir, f"{source_bs}.json")
    with open(source_json_file, "w") as f:
        f.write(r.text)  # Saves sample json object to file

    file_data = json.loads(r.text)
    file_data.pop("_links")  # Removes links array (will be added automatically after updating the biosample)

    # We call the function that returns the list of relationships
    rel_list = relationships(file_data=file_data,
                             source_bs=source_bs,
                             target_list=df_dict[source_bs])

    target_bs = new_relationship['target'] #setting target_bs variable here

    # Editing SOURCE/ENA sample metadata to link samples:
    if not args.unlink:
        # We first append the new relationship to the list (only if it's not already there)
        if new_relationship in rel_list:
            print_v(f"- WARNING: Relationship of source sample '{source_bs}' derived from target sample '{target_bs}' already existed in the downloaded JSON file. Skipping.") #notdefined- why?
            continue
        rel_list.append(new_relationship)

        # We overwrite the relationships node in our array and file
        rel_array = {"relationships": rel_list}
        file_data.update(rel_array)

        # We update the JSON file with the new relationships
        edited_source_file = os.path.join(output_dir, f"linked_{source_bs}.json")
        with open(edited_source_file, 'w') as f:
            json.dump(file_data, f, indent=1)  # Converts python dictionary back into json string

    # Editing SOURCE/ENA sample metadata to unlink samples:
    elif args.unlink:
        #interactive check whether user would really like to remove unlink samples
        user_response = input(f" - \n WARNING: Relationship between source sample '{source_bs}' derived from target sample '{target_bs}' will be removed. Do you wish to proceed? [y/n]")
        if not user_response == "y":
            print("\t Aborting script.")
            sys.exit()
        # Remove relationships between source + target accs
        try:
            index = file_data["relationships"].index(new_relationship)
            file_data["relationships"].pop(index) # removing nested lists using integers not strings
        except: #except if ValueError thrown
            print(f"WARNING: Relationship of source sample '{source_bs}' derived from target sample '{target_bs}' does not exist in the downloaded JSON file. Skipping.")
            continue

        # We update the JSON file by removing the relationships
        edited_source_file = os.path.join(output_dir, f"unlinked_{source_bs}.json")
        with open(edited_source_file, 'w') as f:
            json.dump(file_data, f, indent=1)  # Converts python dictionary back into json string

    # Submit updated Biosample json file:
    update_url = "{0}{1}".format(biosamples_url, webin_auth)
    r = requests.put(update_url, headers = headers, data = json.dumps(file_data))

# error messages:
    if r.status_code == 200:
        if not args.unlink:
            print_v(f"-- Biosamples successfully linked. Source sample '{source_bs}' derived from target list:\n{df_dict[source_bs]}")
        elif args.unlink:
            print_v(f"-- Biosamples successfully unlinked. The following relationship has been removed: 'Source sample '{source_bs}' derived from target list:\n{df_dict[source_bs]}'")
    else:
        if not args.unlink:
            print(f"- ERROR: Biosamples linking failed (error code {r.status_code}). See error file. For Source sample '{source_bs}' derived from target list:\n{df_dict[source_bs]}", file=sys.stderr)
        elif args.unlink:
            print(f"- ERROR: Failed to remove Biosamples relationship/s (error code {r.status_code}). See error file. For Source sample '{source_bs}' derived from target list:\n{df_dict[source_bs]}", file=sys.stderr)
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%y_%H_%M_%S")
        with open(os.path.join(output_dir, f"error_{source_bs}_{dt_string}.log"), "wb") as e:
            error_file = e.write(r.content)
