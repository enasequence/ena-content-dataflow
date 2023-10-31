#!/usr/bin/env python3

# Copyright [2021-2023] EMBL-European Bioinformatics Institute
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
from datetime import datetime
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
# Using a boolean value for unlinking samples
user_response = False
# Datetime string for error file
now = datetime.now()
dt_string = now.strftime("%y-%m-%d_%H-%M-%S")
# Name of the default credentials file
credentials_json = "credentials.json"
# Number of columns expected in the input file (for now 2 but may add the type of Relationship)
number_input_columns = 2
# Name of the columns in the summary dataframe
exit_status_col = "exit_status"
feature_col = "link_or_unlink"
error_content_col = "extra_error_content"

# -------- #
# Parsing given arguments
# -------- #
description = """
This script links or unlinks source (e.g. viral ENA) biosamples with target (e.g. human EGA) biosamples, using the "derived from" relationships field (i.e. 'source sample derived from target sample').
"""

example = """
Example 1: link biosamples in development environment:
    python3 biosamples_relationships.py -s test_sample_accs.txt
Example 2: link biosamples in production:
    python3 biosamples_relationships.py -s test_sample_accs.txt -prod
Example 3: unlink biosamples in development environment: 
    python3 biosamples_relationships.py -u -s test_sample_accs.txt
"""

parser = argparse.ArgumentParser(prog="biosamples_relationships.py",
                                 description=description,
                                 formatter_class=RawTextHelpFormatter,
                                 epilog=example)

parser.add_argument('-s', '--spreadsheet-file',
                    dest='spreadsheet',
                    help=f"(required) filename for spreadsheet (.csv, .txt, .tsv or .xlsx) containing source and target biosample accessions, with 1 source and 1 corresponding target accession per line. The two expected column headers are '{source_col_name}' and '{target_col_name}'.",
                    type=str,
                    required=True)

parser.add_argument('-c', '--credentials-file',
                    dest='credentials_file',
                    nargs='?',  # 0 or 1 arguments
                    help=f"""(optional) JSON file containing the credentials (either root or original owner credentials - see data/test_credentials.json for its format) for the changes to be pushed (default: {credentials_json}). If not given, environment variables '{env_user_str}' and '{env_pwd_str}' will be used.""",
                    required=False)

parser.add_argument('-u', '--unlink-samples',
                    dest='unlink',
                    action='store_true',
                    help=f"""(optional) remove relationships specified within the spreadsheet file. These are deleted at the source samples.""",
                    required=False)

parser.add_argument('-prod', '--production',
                    help='(optional) Changes (either link or unlink biosamples) are made in production (if -prod not specified, biosamples will be linked in development by default).',
                    action='store_true')  # 'dev' env is default if prod not specified

parser.add_argument('--verbose',
                    action='store_true',
                    default=False,
                    help="""A boolean switch to add verbosity to the scripts (printing initial token, source and target lists...).""")

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

# If the chosen instance is production, check interactively that the user did intend to modify it in production.
if interactive_check_production and args.production:
    u_response = input(
        "\n- WARNING: Changes will be applied to the production instance of BioSamples ('-prod' was given as an argument). Do you wish to proceed? [y/n]")
    if not u_response == "y":
        print("\t Aborting script.")
        sys.exit()

# We check that the unlinking was specified on purpose
if args.unlink:
    while user_response not in ("n", "y"):
        user_response = input(" - \n WARNING: All relationships specified in the spreadsheet will be removed. Do you wish to proceed? [y/n]")
    if user_response == "n":
        print("\t Aborting script.")
        sys.exit()
    keyword_for_reporting = "un"
    exp_exit_status = 204
else:
    keyword_for_reporting = ""
    exp_exit_status = 201

# -------- #
# Code blocks
# -------- #
def print_v(text_to_print): #TODO: improve verbose printing
    """ Function to print a given text based on chosen verbosity.
    """
    if args.verbose:
        print(text_to_print)

# To read credentials
def load_json_file(file):
    """ Function to load (and return) a JSON file (given as input) as a dictionary.
    """
    with open(file, "r") as f:
        loaded_dict = json.load(f)

    return loaded_dict

# To create the relationship and curations
def relationships(source_bs, target_list, unlink_mode, r_type="derived from", node_name="relationships"): #removed file_data - do not need to check if curations exist before adding them
    """ Fuction to add new relationships to a given node in a JSON file, based on a given source and target list.
        Returns a list of new relationships that can be used to update the file.
        If 'unlink_mode' is True, then what the function does is remove the given targets from the list of
        relationships.

        Parameters:
            - source_bs (string): source of the relationship (e.g. SAMEA7616999)
            - target_list (list): list of targets of the relationship with the source (e.g. ['SAMEA6941288', 'SAMEA8698068'])
            - unlink_mode (bool): whether the function needs to add or remove relationships.
            - r_type (string): type of relationship (by default 'derived from')
            - node_name (string): node of the JSON file to add relationships to (by default 'relationships').
    """
    #regardless of linking or unlinking, the relationships array still needs to be created

    rel_list = []

    for target_bs in target_list:
        relationships.relationship = {"source": source_bs, "type": r_type, "target": target_bs} #specifying 'relationship' variable as attribute of relationships function, so that it can be called from hash function below
        rel_list.append(relationships.relationship) #TODO: do we need to check if relationship is in rel_list already?

    #if linking - add the relationship in new curation form, if unlinking, add it in 'removal' form:

    if not unlink_mode:
        new_curation_dict = {'sample': f"{source_bs}", 'curation': {'relationshipsPre': [], 'relationshipsPost': rel_list}}
        return new_curation_dict
    else:
        remove_curation_dict = {'sample': f"{source_bs}", 'curation': {'relationshipsPre': rel_list, 'relationshipsPost': []}}
        return remove_curation_dict

    #return does not print out a value once the function is run - it is 'implicitly returned', but if we print(relationships) func, the return variable will be explicitly printed

def obtain_cur_hash(response):
    """
        Obtains curation hash for a submitted curation, in order to remove it from the biosample

        Parameters:
            - response (dict): the https response containing info on all curations applied to the sample
    """
    sample_curs = response['_embedded']['curationLinks']  # lists all curations applied to sample
    #print("number of curations for source sample is:", len(sample_curs))
    counter = 0

    # Identify submitted curation index and obtain hash for it
    for idx, i in enumerate(sample_curs): #enumerate to print out counter for each curation
        #print(idx, i)
        if relationships.relationship in response['_embedded']['curationLinks'][idx]['curation']['relationshipsPost']: #calling 'relationship' variable as attribute of previous function
            obtain_cur_hash.curation_hash = response['_embedded']['curationLinks'][idx]['hash']
            print(f"curation hash for {relationships.relationship} \n = " + obtain_cur_hash.curation_hash)
            return obtain_cur_hash.curation_hash

        else:
            counter += 1 #count the number of times the loop has continued (i.e where relationship did not exist in curations)
            #print(counter)
            continue #continue the loop to the next iteration

    if counter == len(sample_curs): #TODO: iterate through to next curation in script, + output this to error file,
        print(f"ERROR in removing curation: {relationships.relationship} did not originally exist in source sample. \t Aborting script.")
        sys.exit()

    return


def read_input_file(input_file, number_input_columns = number_input_columns):
    """
        Transforms the input file into a dataframe, converging from different input file
            types (.csv, .tsv and .xlsx) into a unified format.

        Regarding datatypes (dtype), we choose to preserve data as stored in the input file (dtype = "object") instead
            of letting pandas interpret it or giving a dictionary with datatypes.

        Parameters:
            - input_file (str): filepath of the input file containing the spreadsheet. 
            - number_input_columns (int): number of columns expected in the input file.
    """
    # We check the given filepath exists and is a file
    if not os.path.isfile(input_file):
        print("- ERROR in read_input_file(): given input filepath '%s' does not exist" % input_file, file=sys.stderr)
        sys.exit()
    
    input_filetype = os.path.splitext(input_file)[1]
    input_file_basename = os.path.basename(input_file)

    if not input_filetype.lower() in [".txt", ".xlsx", ".tsv", ".csv"]:
        print(
            f"- ERROR in biosamples_relationships.py: given input file '{args.spreadsheet}' is not a '.txt', '.csv', '.tsv' or '.xlsx' file.",
            file=sys.stderr)
        sys.exit()    

    try:
        # Depending on what filetype it is, we read it with different pandas' methods
        if input_filetype == ".csv":
            input_dataframe = pd.read_csv(filepath_or_buffer = input_file, sep = ",", dtype = "object")

        elif input_filetype == ".tsv" or input_filetype == ".txt":
            input_dataframe = pd.read_csv(filepath_or_buffer = input_file, sep = "\t", dtype = "object")

        elif input_filetype == ".xlsx":
            input_dataframe = pd.read_excel(io = input_file, dtype = "object", engine='openpyxl')

        else:
            # If given a filetype that we didn't expect
            print("ERROR in read_input_file(): given input file (%s) has extension '%s', while the allowed file types are [.csv | .tsv | .txt | .xlsx]" \
                    % (input_file_basename, input_filetype), file=sys.stderr)
            sys.exit()

    except ValueError:
        print("ERROR in read_input_file() - generate_dataframe(): given input file (%s) did not have the expected tab" \
                    % (input_file_basename), file=sys.stderr)
        sys.exit()
        
    except Exception:
        print("ERROR in read_input_file(): given input filepath '%s' could not be read." % input_file, file=sys.stderr)
        raise
    
    # We assert that there are only 'number_input_columns' number of columns (above defined) in the dataframe.
    input_dataframe.dropna(axis=1, how="all", subset=None, inplace=True)
    column_number = len(input_dataframe.columns)
    if column_number != number_input_columns:
        print(f"ERROR in read_input_file(): input file {input_file_basename} had {column_number} columns, but {number_input_columns} are expected.", file=sys.stderr)
        sys.exit()

    return input_dataframe

# We load credentials
if not args.credentials_file == None:
    credentials_dict = load_json_file(file=args.credentials_file)
    credentials_user = credentials_dict[user_str]
    credentials_pwd = credentials_dict[pwd_str]
else:
    credentials_user = os.environ[env_user_str]
    credentials_pwd = os.environ[env_pwd_str]

# We create the dataframe to record exit statuses
summary_df = pd.DataFrame(columns = [feature_col, exit_status_col, source_col_name, target_col_name, error_content_col])

# Check if output_dir where the output_xml will reside exists, create it if not.
#   Get the root directory of the tool.
root_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))  # prints root directory
output_dir = os.path.join(root_dir, output_dir_name)  # creates biosamples_output folder in root directory
dir_exists = os.path.isdir(output_dir)  # T or F
if not dir_exists and output_dir != '':
    try:
        os.makedirs(output_dir)
        print_v(f"Successfully created output directory '{output_dir}'.")
    except Exception:
        print(f"\n- ERROR in biosamples_relationships.py: could not create output directory '{output_dir}'. Aborting script.",
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

##
#   Read input file
##
df = read_input_file(args.spreadsheet)
print_v(f"Input accession list (sources and targets that will be linked):\n{df}\n")

if not source_col_name in df.columns or not target_col_name in df.columns:
    print(f"\n- ERROR in biosamples_relationships.py: the dataframe created based on the input file '{args.spreadsheet}' did not contain both of the expected columns. Aborting script.\n\
            \t- Expected columns: {[source_col_name, target_col_name]}'\n\
            \t- Columns in the DF: {list(df.columns)}", file=sys.stderr)
    exit()
# We get rid of spaces (including unicode symbol for space, common in excel) in all rows
df[source_col_name] = df[source_col_name].str.replace(' ', '')
df[target_col_name] = df[target_col_name].str.replace(' ', '')

df[source_col_name] = df[source_col_name].str.replace(u'\xa0', '')
df[target_col_name] = df[target_col_name].str.replace(u'\xa0', '')

# We create the dictionary of sources and targets
df_dict = {}
for source_bs, target in zip(df[source_col_name], df[target_col_name]):
    if source_bs not in df_dict:
        df_dict[source_bs] = [target] #list of target accessions

    # If the source is repeated:
    else:
        df_dict[source_bs].append(target)

## main edits begin here ###-------------
# Create relationship json file and submit as curation

if args.production:
    biosamples_start = 'https://www.ebi.ac.uk/biosamples/samples/'
else:
    biosamples_start = 'https://wwwdev.ebi.ac.uk/biosamples/samples/'

curation_links = '/curationlinks'

for source_bs in df_dict.keys():

    # We call the function that returns the list of relationships to add or remove
    curation = relationships(source_bs=source_bs, target_list=df_dict[source_bs], unlink_mode=args.unlink)
    #print(curation)

    if not args.unlink: #TODO: the json file curation part may be better as a function

        # We create a json file containing the new curation to be added
        with open(os.path.join(output_dir, f"{source_bs}_new_rel_curation.json"), "w") as f: #virus sample json curation file
            json.dump(curation, f, indent=4) #number of spaces at the beginning of a line to indent by
            #json.dump() uses dictionary directly, while json.dumpS() requires covnersion to json string first

        # Submit curation file
        curation_url = "{0}{1}{2}".format(biosamples_start, source_bs, curation_links)
        r = requests.post(curation_url, headers = headers, data = open(os.path.join(output_dir, f"{source_bs}_new_rel_curation.json"), 'rb')) #have to open + read json file first

    else:

        # We create a json file containing the curation to be removed
        with open(os.path.join(output_dir, f"{source_bs}_rel_curation_removed.json"), "w") as f:
            json.dump(curation, f, indent=4)  # number of spaces at the beginning of a line to indent by
            # json.dump() uses dictionary directly, while json.dumpS() requires conversion to json string first

        # Then get all curation info for source sample
        get_cur_info = "{0}{1}{2}".format(biosamples_start, source_bs, curation_links)
        r = requests.get(get_cur_info, headers=header).json()  # no headers needed, nor webin token #obtaining result as json file
        # r_formatted = json.dumps(r, indent=4)  # to print curation info in human readable format
        cur_info = json.loads(json.dumps(r))  # loads -> json string to python dict

        # Obtain curation hash for each sample
        obtain_cur_hash(r) #prints out relationship + assoc. hash if relationship exists in source sample
        #print(obtain_cur_hash.curation_hash) #hash variable

        # Submit delete request to remove curation
        del_curation_url = "{0}{1}{2}{3}".format(biosamples_start,source_bs,curation_links,"/" + obtain_cur_hash.curation_hash) #curation_hash var not being recognised?
        r = requests.delete(del_curation_url, headers=header, data = open(os.path.join(output_dir, f"{source_bs}_rel_curation_removed.json"), 'rb'))
        #print(r.status_code) #204 for https delete request

    # We add the output of the pushed changes to the summary dataframe
    summary_df = summary_df.append({feature_col: "unlink" if args.unlink else "link",
                                    exit_status_col: r.status_code,
                                    source_col_name: source_bs,
                                    target_col_name: df_dict[source_bs],
                                    error_content_col: str(r.content)},
                                    ignore_index = True)

# creating output tsv files
summary_file_basename = f"{dt_string}_summary.tsv"
summary_filename = os.path.join(output_dir, summary_file_basename)
summary_df.to_csv(path_or_buf = summary_filename,
                sep = "\t",
                index = False)


# check to see if all samples have been linked/unlinked successfully
if (summary_df[exit_status_col] == exp_exit_status).all():
    print_v(f"- All {len(df)} relationships (from {len(summary_df)} sources) successfully {keyword_for_reporting}linked")
else:
    print(f"- ERROR: some relationships failed to be {keyword_for_reporting}linked. Please see error file {summary_file_basename} and check exit_status different from '{exp_exit_status}'.")
