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

import os, sys
from datetime import datetime
import cx_Oracle, argparse
from getpass import getpass

description = """
Setup
-----
This script uses the cx_Oracle python module, which requires a little setup. For details, see:
https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html
The Oracle Instant Client is a requirement of this module. Please set the location of this library
using the $ORACLE_CLIENT_LIB environment variable before using this script.

Description
-----
This script will query ERAREAD for COVID-related projects and split the results into 4 logs:
    - log1 : sars-cov-2 sequences
    - log2 : other coronaviral sequences
    - log3 : human sequences
    - log4 : other host sequences

The script will create an output directory containing results for each log:
    - a TSV spreadsheet for import into Excel or similar
    - a list of all project accessions in the log (for input to the add_to_umbrella_project.py script)
    - a list of public project accessions in the log (to link to the datahub with generate_datahub_queries.py)

"""
usage = """
Usage: fetch_covid_projects_from_db.py <OPTIONS>

Options:
    --outdir : (optional) name of output directory (default: covid_logs_<timestamp>)
    --where  : (optional) additional filtering to add to the default SQL command (default: none)

"""
example = """
Example: save projects that are not in any umbrella in a directory called 'test_dir'
    fetch_and_filter_projects.py --outdir test_dir --where "umbrella_project_id IS NULL"

"""
parser = argparse.ArgumentParser(
    description=description+usage+example
)
parser.add_argument('--outdir', help="(optional) name of output directory (default: covid_logs_<timestamp>)");
parser.add_argument('--where',  help="(optional) additional filtering to add to the default SQL command (default: none)")
opts = parser.parse_args(sys.argv[1:])


# set up gigantic SQL query
where_clause = [ 
    "p.tax_id = 2697049 OR sm.tax_id = 2697049 OR " +
    "(lower(s.study_title) like '%sars%cov%2%' OR lower(s.study_title) like '%covid%' OR lower(s.study_title) like '%coronavirus%' OR lower(s.study_title) like '%severe acute respiratory%')",

    # this is a set of projects to use for testing - PRJEB37513 is part private, PRJNA294305 is private
    # "s.project_id IN ('PRJNA656810', 'PRJNA656534', 'PRJNA656060', 'PRJNA622652', 'PRJNA648425', 'PRJNA648677', 'PRJEB39632', 'PRJNA294305', 'PRJEB37513')",
]
if opts.where:
    where_clause.append(opts.where)
sql = """
SELECT d.meta_key as datahub, l.to_id as umbrella_project_id, p.project_id, p.first_created,
    s.study_id, s.study_title, COUNT(unique(sm.sample_id)) as sample_count, COUNT(unique(r.run_id)) 
    as run_count, p.center_name, p.tax_id as project_taxon_id, p.scientific_name as project_scientific_name,
    sm.tax_id as sample_taxon_id, sm.scientific_name as sample_scientific_name,
    p.status_id, avg(e.status_id), avg(sm.status_id), avg(r.status_id)
FROM study s 
    JOIN project p on s.project_id = p.project_id 
    LEFT JOIN dcc_meta_key d on d.project_id = p.project_id
    LEFT JOIN (select * from ena_link where to_id in ('PRJEB39908')) l on l.from_id = p.project_id
    JOIN experiment e on e.study_id = s.study_id
    JOIN experiment_sample es on es.experiment_id = e.experiment_id
    JOIN sample sm on sm.sample_id = es.sample_id
    JOIN run r on e.experiment_id = r.experiment_id
"""
sql += "WHERE " + " AND ".join(where_clause)
sql += """
GROUP BY d.meta_key, l.to_id, p.project_id, p.first_created, s.study_id, s.study_title, p.center_name,
    p.tax_id, p.scientific_name, sm.tax_id, sm.scientific_name, p.status_id
ORDER BY p.first_created desc
"""

# global variables for use throughout the script
log1, log2, log3, log4 = [], [], [], []
sars_tax_id, human_tax_id = '2697049', '9606'

def get_oracle_usr_pwd():
    usr = input("Username: ")
    pwd = getpass()
    return usr, pwd

def setup_connection():
    oracle_usr, oracle_pwd = get_oracle_usr_pwd()
    client_lib_dir = os.getenv('ORACLE_CLIENT_LIB')
    if not client_lib_dir or not os.path.isdir(client_lib_dir):
        sys.stderr.write("ERROR: Environment variable $ORACLE_CLIENT_LIB must point at a valid directory\n")
        exit(1)
    cx_Oracle.init_oracle_client(lib_dir=client_lib_dir)
    connection = None
    try:
        dsn = cx_Oracle.makedsn("ora-vm-069.ebi.ac.uk", 1541, service_name="ERAREAD")
        connection = cx_Oracle.connect(oracle_usr, oracle_pwd, dsn, encoding="UTF-8")
        return connection
    except cx_Oracle.Error as error:
        print(error)
    

"""
    fetch projects using the above SQL query and filter them into 1 of 4 logs:
        * log1 : sars-cov-2 sequences
        * log2 : other coronaviral sequences
        * log3 : human sequences (infected with covid-19)
        * log4 : other host sequences
"""
def fetch_and_filter_projects(connection):
    cursor = connection.cursor()
    for row in cursor.execute(sql):
        row = list(row) # is a tuple by default

        # record the status of the project
        this_status = ''
        project_status, exp_status, sample_status, run_status = row[13:]
        if ( project_status != 4 ):
            this_status = 'private'
        else:
            if ( exp_status == 4 and sample_status == 4 and run_status == 4 ):
                this_status = 'public'
            else:
                this_status = 'part private'
        row[13:] = [this_status]

        # filter into different logs on taxon id and scientific name
        project_taxon_id = row[9]  if row[9]  else ''
        sample_taxon_id  = row[11] if row[11] else ''
        if project_taxon_id == sars_tax_id or sample_taxon_id == sars_tax_id:
            log1.append(row)
        elif project_taxon_id == human_tax_id or sample_taxon_id == human_tax_id:
            log3.append(row)
        else:
            project_scientific_name = row[10] if row[10] else ''
            sample_scientific_name  = row[12] if row[12] else ''
            if 'virus' in project_scientific_name or 'virus' in sample_scientific_name:
                log2.append(row)
            else:
                log4.append(row)

"""
    a log can contain 'None' values and datetime objects - this method
    converts it to a comma-separated string, with dates in the 01-Jan-20 format
"""
def log_to_str(log, line_prefix=''):
    log_str = ''
    for i in log:
        i = list(i)
        i[3] = i[3].strftime("%d-%b-%y")
        log_str += line_prefix + "\t".join([str(j) if j else 'NULL' for j in i]) + "\n"
    return log_str

def print_log(log, title):
    print(f"-------- {title} --------")
    print(log_to_str(log, '- '))
    print()

"""
    extract project accessions from the log and return list string
"""
def project_list_str(log, status_filter=False):
    proj_list = []
    if status_filter:
        proj_list = [x[2] for x in log if x[13] == status_filter]
    else:
        proj_list = [x[2] for x in log]
    return "\n".join(proj_list)

""" 
    generate and create the output directory 
"""
def create_outdir():
    if opts.outdir:
        outdir = opts.outdir
    else:
        now = datetime.now()
        now_str = now.strftime("%d%m%y_%H%M%S")
        outdir = f"covid_logs_{now_str}"
    os.mkdir(outdir)
    return outdir

file_header =  ['datahub', 'umbrella_project_id', 'project_id', 'first_created', 'study_id',
    'study_title', 'sample_count', 'run_count', 'center_name', 'project_taxon_id', 'project_scientific_name',
    'sample_taxon_id', 'sample_scientific_name', 'project_status'
]
def write_log_files(log, file_prefix):
    with open(f"{file_prefix}.tsv", 'w') as log_tsv:
        log_tsv.write("\t".join(file_header) + "\n")
        log_tsv.write(log_to_str(log))
    with open(f"{file_prefix}.projects.all.list", 'w') as log_proj:
        log_proj.write(project_list_str(log))
    with open(f"{file_prefix}.projects.public_only.list", 'w') as log_proj:
        log_proj.write(project_list_str(log, 'public'))

#------------------------#
#          MAIN          #
#------------------------#
if __name__ == "__main__":
    # fetch and parse the data
    sys.stderr.write("Connecting to ERAREAD...\n")
    db_conn = setup_connection()
    sys.stderr.write("Querying ERAREAD for COVID-19 projects...\n")
    fetch_and_filter_projects(db_conn)

    # format output and write files
    sys.stderr.write("Writing files...\n")
    outdir = create_outdir()
    write_log_files(log1, f"{outdir}/log1.sars-cov-2")
    write_log_files(log2, f"{outdir}/log2.other_viruses")
    write_log_files(log3, f"{outdir}/log3.human")
    write_log_files(log4, f"{outdir}/log4.other_hosts")
    sys.stderr.write(f"Files written to '{outdir}'\n\n")
