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

import os, sys, shutil, re
from datetime import datetime
import cx_Oracle, argparse
from getpass import getpass
import pandas as pd

description = """
Setup
-----
This script uses the cx_Oracle python module, which requires a little setup. For details, see:
https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html
The Oracle Instant Client is a requirement of this module. Please set the location of this library
using the $ORACLE_CLIENT_LIB environment variable before using this script.

Description
-----
This script will query ERAREAD for COVID-related projects and split the results into 5 logs:
    - log1 : sars-cov-2 sequences
    - log2 : other coronaviral sequences
    - log3 : metagenomes
    - log4 : human sequences
    - log5 : other host sequences

The script will create an output directory containing:
    - an .xlsx spreadsheet for import into Excel or similar (one sheet per-log)
    - per-log list of project accessions that are not yet in an umbrella project (for input to the add_to_umbrella_project.py script)
    - per-log list of public project accessions that are not yet in a data hub (to link to the datahub with generate_datahub_queries.py)

"""
usage = """
Usage: fetch_covid_projects_from_db.py <OPTIONS>

Options:
    --outdir          : (optional) name of output directory (default: covid_logs_<timestamp>)
    --where           : (optional) additional filtering to add to the default SQL command (default: none)
    --ignore_projects : (optional) file containing list of project ids to ignore

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
parser.add_argument('--ignore_projects',  help="(optional) file containing list of project ids to ignore (default: none)")
parser.add_argument('--webin', help="(optional) username and password for your Webin account. Format 'User:Password'")
opts = parser.parse_args(sys.argv[1:])

# set up gigantic SQL query
umbrella_project_ids = ['PRJEB39908', 'PRJEB40349', 'PRJEB40770', 'PRJEB40771', 'PRJEB40772']
where_clause = [
    "p.tax_id = 2697049 OR sm.tax_id = 2697049 OR " +
    "(lower(s.study_title) like '%sars%cov%2%' OR lower(s.study_title) like '%covid%' OR lower(s.study_title) like '%coronavirus%' OR lower(s.study_title) like '%severe acute respiratory%')" +
    " AND p.status_id not in (3, 5)" +
    " AND (s.study_id not like 'EGA%' AND s.project_id not like 'EGA%') " +
    " AND (a.analysis_type IS NULL OR a.analysis_type = 'SEQUENCE_ASSEMBLY')"
    # this is a set of projects to use for testing - PRJEB37513 is part private, PRJNA294305 is private
    # "s.project_id IN ('PRJNA656810', 'PRJNA656534', 'PRJNA656060', 'PRJNA622652', 'PRJNA648425', 'PRJNA648677', 'PRJEB39632', 'PRJNA294305', 'PRJEB37513')",
]
if opts.where:
    where_clause.append(opts.where)
sql = """
SELECT l.to_id as umbrella_project_id, p.project_id, p.first_created,
    s.study_id, s.study_title, COUNT(unique(sm.sample_id)) as sample_count, COUNT(unique(r.run_id))
    as run_count, count(unique(a.analysis_id)) as sequence_count,
    p.center_name, p.tax_id as project_taxon_id, p.scientific_name as project_scientific_name,
    sm.tax_id as sample_taxon_id, sm.scientific_name as sample_scientific_name,
    p.status_id
FROM study s
    JOIN project p on s.project_id = p.project_id
    LEFT JOIN (select * from ena_link where to_id in ("""
sql += ",".join([f"'{u}'" for u in umbrella_project_ids]) # quote and join
sql += """)) l on l.from_id = p.project_id
    LEFT JOIN analysis a ON a.study_id = s.study_id
    LEFT JOIN analysis_sample ans ON a.analysis_id = ans.analysis_id
    LEFT JOIN experiment e ON e.study_id = s.study_id
    LEFT JOIN experiment_sample es ON es.experiment_id = e.experiment_id
    LEFT JOIN sample sm ON (ans.sample_id = sm.sample_id OR es.sample_id = sm.sample_id)
    LEFT JOIN run r ON e.experiment_id = r.experiment_id
"""
sql += "WHERE " + " AND ".join(where_clause)
sql += """
GROUP BY l.to_id, p.project_id, p.first_created, s.study_id, s.study_title, p.center_name,
    p.tax_id, p.scientific_name, sm.tax_id, sm.scientific_name, p.status_id
ORDER BY p.first_created desc
"""

# global variables for use throughout the script
log1, log2, log3, log4, log5 = [], [], [], [], []
sars_tax_id, human_tax_id = '2697049', '9606'

def get_oracle_usr_pwd():
    # usr = input("Username: ")
    # pwd = getpass()
    # return usr, pwd
    return ['era_reader', 'reader']

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
        sys.stderr.write("Could not connect to ERAREAD...\n{}\n".format(error))
        sys.exit(1)

"""
    fetch projects using the above SQL query and filter them into 1 of 4 logs:
        * log1 : sars-cov-2 sequences
        * log2 : other coronaviral sequences
        * log3 : metagenomes
        * log4 : human sequences (infected with covid-19)
        * log5 : other host sequences
"""
def fetch_and_filter_projects(connection):
    ignore_projects = []
    if opts.ignore_projects:
        ignore_projects = [line.strip() for line in open(opts.ignore_projects, 'r')]

    cursor = connection.cursor()
    for row in cursor.execute(sql):
        project_id = row[1]
        if project_id in ignore_projects:
            continue

        # convert tuple to list, and replace empty strings with NULL
        row = ['NULL' if (not x or x == '') else x for x in list(row)]

        # filter into different logs on taxon id, scientific name and study title
        study_title = row[4]
        project_taxon_id = row[9]
        sample_taxon_id  = row[11]
        if project_taxon_id == sars_tax_id or sample_taxon_id == sars_tax_id:
            log1.append(row)
        elif (project_taxon_id == human_tax_id or sample_taxon_id == human_tax_id) and 'metagenom' not in study_title:
            log4.append(row)
        else:
            project_scientific_name = row[10] if row[10] else ''
            sample_scientific_name  = row[12] if row[12] else ''
            if 'virus' in project_scientific_name or 'virus' in sample_scientific_name or 'viridae' in project_scientific_name or 'viridae' in sample_scientific_name:
                log2.append(row)
            elif 'metagenom' in project_scientific_name or 'metagenom' in sample_scientific_name or 'metagenom' in study_title:
                log3.append(row)
            else:
                log5.append(row)

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
    generate and create the output directory
"""
def create_outdir():
    if opts.outdir:
        outdir = opts.outdir
    else:
        now = datetime.now()
        now_str = now.strftime("%d%m%y_%H%M%S")
        outdir = f"covid_logs_{now_str}"

    if os.path.exists(outdir):
        shutil.rmtree(outdir)

    os.mkdir(outdir)
    return outdir

file_header =  ['umbrella_project_id', 'project_id', 'first_created', 'study_id',
    'study_title', 'sample_count', 'run_count', 'sequence_count', 'center_name', 'project_taxon_id',
    'project_scientific_name', 'sample_taxon_id', 'sample_scientific_name', 'project_status_id'
]
def write_logs(log, file_prefix, outdir, xls_writer):
    with open(f"{file_prefix}.tsv", 'w') as log_tsv:
        dataframe = pd.DataFrame(log, columns = file_header)
        dataframe.to_excel(xls_writer, sheet_name=file_prefix, index=False)

    project_accs_no_umbrella = {}
    for l in log:
        if l[0] == 'NULL':
            # use dictionary to get a uniq list
            project_accs_no_umbrella[l[1]] = 1

    with open(f"{outdir}/{file_prefix}.projects.no_umbrella.list", 'w') as log_proj:
        log_proj.write("\n".join(project_accs_no_umbrella))

    return project_accs_no_umbrella

def update_umbrella(accs, xml_template, outdir):
    # first, check that there are some accessions passed
    if len(accs) == 0:
        sys.stderr.write("No update required for {}. Skipping.\n".format(xml_template))
        return

    # construct the child projects XML
    child_project_xml = ['<RELATED_PROJECTS>']
    for project_acc in accs:
        child_project_xml.append(f"\t<RELATED_PROJECT><CHILD_PROJECT accession=\"{project_acc}\"/></RELATED_PROJECT>")
    child_project_xml.append('</RELATED_PROJECTS>')

    # parse the template XML and place the child projects in the correct position
    umbrella = False
    umbrella_w_children_xml = []
    with open(xml_template) as xml_file:
        for line in xml_file:
            line = line.rstrip()
            # print(line)
            umbrella_w_children_xml.append(line)
            regex = re.match("(\s+)<UMBRELLA_PROJECT/>", line)
            if regex:
                umbrella = True
                indent = regex.group(1)
                child_project_xml = [f"{indent}{x}" for x in child_project_xml]
                # print("\n".join(child_project_xml))
                umbrella_w_children_xml.append("\n".join(child_project_xml))

    if not umbrella:
        sys.stderr.write("\n\nError: <UMBRELLA_PROJECT/> tag not found in the --xml file : no <CHILD_PROJECT> tags added\n\n")

    # write the umbrella xml with child projects to file
    umbrella_xml_file = "{}/{}".format(outdir, os.path.basename(xml_template).replace('.xml', '.umbrella.xml'))
    with open(umbrella_xml_file, 'w') as ufile:
        ufile.write("\n".join(umbrella_w_children_xml))

    # write the submission xml to file
    submission_xml = """
<SUBMISSION>
     <ACTIONS>
         <ACTION>
             <MODIFY/>
         </ACTION>
    </ACTIONS>
</SUBMISSION>
    """
    submission_xml_file = "{}/{}".format(outdir, os.path.basename(xml_template).replace('.xml', '.submission.xml'))
    with open(submission_xml_file, 'w') as sfile:
        sfile.write(submission_xml)

    user_pass = 'User:Password'
    if opts.webin:
        user_pass = opts.webin

    # create and print the curl command required to submit the updated objects
    print(f"curl -u {user_pass} -F \"SUBMISSION=@{submission_xml_file}\" -F \"PROJECT=@{umbrella_xml_file}\" \"https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/\"")


#------------------------#
#          MAIN          #
#------------------------#
if __name__ == "__main__":
    outdir = create_outdir()

    # fetch and parse the data
    sys.stderr.write("Connecting to ERAREAD...\n")
    db_conn = setup_connection()
    sys.stderr.write("Querying ERAREAD for COVID-19 projects...\n")
    fetch_and_filter_projects(db_conn)

    # format output and write files
    file_prefixes = [
        'log1.sars-cov-2', 'log2.other_coronavirus', 'log3.metagenomes',
        'log4.human', 'log5.other_hosts'
    ]
    sys.stderr.write("Writing files...\n")
    xls_writer = pd.ExcelWriter(f"{outdir}/covid_logs.xlsx", engine='xlsxwriter')
    l1_no_umb = write_logs(log1, file_prefixes[0], outdir, xls_writer)
    l2_no_umb = write_logs(log2, file_prefixes[1], outdir, xls_writer)
    l3_no_umb = write_logs(log3, file_prefixes[2], outdir, xls_writer)
    l4_no_umb = write_logs(log4, file_prefixes[3], outdir, xls_writer)
    l5_no_umb = write_logs(log5, file_prefixes[4], outdir, xls_writer)
    xls_writer.save()
    sys.stderr.write(f"Files written to '{outdir}'\n\n")

    # update the umbrellas
    repo_root = os.path.realpath(__file__).replace('/scripts/fetch_covid_projects_from_db.py', '')
    xml_dir = "{}/xml/covid19_umbrellas".format(repo_root)
    update_umbrella(l1_no_umb, "{}/{}.umbrella.xml".format(xml_dir, file_prefixes[0]), outdir)
    update_umbrella(l2_no_umb, "{}/{}.umbrella.xml".format(xml_dir, file_prefixes[1]), outdir)
    update_umbrella(l3_no_umb, "{}/{}.umbrella.xml".format(xml_dir, file_prefixes[2]), outdir)
    update_umbrella(l4_no_umb, "{}/{}.umbrella.xml".format(xml_dir, file_prefixes[3]), outdir)
    update_umbrella(l5_no_umb, "{}/{}.umbrella.xml".format(xml_dir, file_prefixes[4]), outdir)
