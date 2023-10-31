#!/usr/bin/env python3.7

# Copyright [2020-2023] EMBL-European Bioinformatics Institute
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

import sys, argparse

"""
This script will generate SQL queries to add or remove lists of projects from a given
datahub. These queries can be copied/opened and run in SQLDeveloper.

Usage: generate_datahub_queries.py --add/--remove <options>

Options:
    --datahub : (required) name of datahub
    --file    : (required) path to file containing one project accession per line
    --private : should the added projects be private? (default: N)
    --add     : generate queries to add projects to the datahub
    --remove  : generate queries to remove projects from datahub

"""

parser = argparse.ArgumentParser()
parser.add_argument('--datahub', help="(required) name of datahub")
parser.add_argument('--file',    help="(required) path to file containing one project accession per line")
parser.add_argument('--private', help="should the added projects be private? (default: N)", action='store_true')
parser.add_argument('--add',     help="generate queries to add projects to the datahub", action='store_true')
parser.add_argument('--remove',  help="generate queries to remove projects from datahub", action='store_true')
opts = parser.parse_args(sys.argv[1:])

if ( opts.add and opts.remove ):
    sys.stderr.write("--add and --remove are mutually exclusive - please choose just one\n\n")
    sys.exit(1)

if ( not ( opts.file and opts.datahub ) ):
    sys.stderr.write("--datahub and --file are required\n\n")
    sys.exit(1)

if ( not (opts.add or opts.remove) ):
    sys.stderr.write("Either --add or --remove must be specified\n\n")
    sys.exit(1)

if ( opts.private ):
    is_private = 'Y'
else:
    is_private = 'N'

with open(opts.file) as file:
    if ( opts.add ):
        print("INSERT ALL")
        for line in file:
            project_acc = line.rstrip()
            print(f"\tINTO dcc_meta_key(project_id, meta_key, private) VALUES ('{project_acc}', '{opts.datahub}', '{is_private}')")
        # INSERT ALL requires a subquery - apparently this is the standard
        # subquery to use when you don't really want to use a subquery ¯\_(ツ)_/¯
        print("SELECT 1 FROM dual;")
    else:
        project_accs = ", ".join([f"'{x.rstrip()}'" for x in file.readlines()])
        print(f"DELETE FROM dcc_meta_key WHERE meta_key = '{opts.datahub}' AND project_id IN ({project_accs});")
