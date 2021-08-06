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
import json

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
    --ignore_projects : (optional) file containing list of project ids to ignore
    --webin           : (optional) username and password for your Webin account. Format 'User:Password'
    --environment     : (optional) 'dev' or 'prod' (default: dev)
    --submit          : (optional) run umbrella update submission automatically (default: printed curl commands)

"""

parser = argparse.ArgumentParser(
    description=description+usage
)
parser.add_argument('--outdir', help="(optional) name of output directory (default: covid_logs_<timestamp>)");
parser.add_argument('--ignore_projects',  help="(optional) file containing list of project ids to ignore (default: none)")
parser.add_argument('--webin', help="(optional) username and password for your Webin account. Format 'User:Password'")
parser.add_argument('--environment', help="(optional) 'dev' or 'prod' (default: dev)", default='dev')
parser.add_argument('--submit', help="(optional) run umbrella update submission automatically (default: printed curl commands)", action='store_true', default=False)
parser.add_argument('--debug', help="(optional) print additional debugging statements", action='store_true', default=False)
opts = parser.parse_args(sys.argv[1:])


# global variables for use throughout the script
umbrella_project_ids = ['PRJEB39908', 'PRJEB40349', 'PRJEB40770', 'PRJEB40771', 'PRJEB40772']
log1, log2, log3, log4, log5 = [], [], [], [], []
sars_tax_id, human_tax_id = '2697049', '9606'
# some very large projects take a long time to join to the sample tables - predefine their taxonomy info
custom_project_tax_info = {
    'PRJEB37886' : [sars_tax_id, 'Severe acute respiratory syndrome coronavirus 2'], # COG-UK
    'PRJEB44987' : [sars_tax_id, 'Severe acute respiratory syndrome coronavirus 2'],  # RKI
    'PRJEB43828' : [sars_tax_id, 'Severe acute respiratory syndrome coronavirus 2'], # Swiss
    'PRJEB44803' : [sars_tax_id, 'Severe acute respiratory syndrome coronavirus 2'] # Iceland
}

# initialise libraries to query Oracle DBs
client_lib_dir = os.getenv('ORACLE_CLIENT_LIB')
if not client_lib_dir or not os.path.isdir(client_lib_dir):
    sys.stderr.write("ERROR: Environment variable $ORACLE_CLIENT_LIB must point at a valid directory\n")
    exit(1)
cx_Oracle.init_oracle_client(lib_dir=client_lib_dir)

"""
    Return connection object given db credentials
"""
def setup_connection(host,port,service_name,user,pwd):
    connection = None
    try:
        dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
        connection = cx_Oracle.connect(user, pwd, dsn, encoding="UTF-8")
        return connection
    except cx_Oracle.Error as error:
        sys.stderr.write("Could not connect to {}...\n{}\n".format(service_name, error))
        sys.exit(1)

    return connection

"""
    Return read-only connection to ERAREAD
"""
def setup_ERA_connection():
    return setup_connection("ora-vm-069.ebi.ac.uk", 1541, "ERAREAD", 'era_reader', 'reader')

"""
    Return read-only connection to ENAPRO
"""
def setup_ENA_connection():
    return setup_connection("ora-vm5-008.ebi.ac.uk", 1531, "ENAPRO", 'ena_reader', 'reader')


"""
    Query ERA to find projects (+ project metadata) that may contain COVID data
"""
def fetch_studies(connection):
    where_clause_studies = " AND ".join([
        " OR ".join([
            "p.tax_id = 2697049",
            "(lower(s.study_title) like '%sars%cov%2%'",
            "lower(s.study_title) like '%covid%'",
            "lower(s.study_title) like '%coronavirus%'",
            "lower(s.study_title) like '%severe acute respiratory%')"
        ]),
        "p.status_id not in (3, 5)",
        "(s.study_id not like 'EGA%' AND s.project_id not like 'EGA%') "
    ])
    # where_clause_studies = "s.project_id IN ('PRJNA656810', 'PRJEB43555', 'PRJNA683801')"

    sql = """
    SELECT p.project_id, s.study_id, p.tax_id, s.study_title, p.project_title, p.first_created,
           p.scientific_name, l.to_id as umbrella_project_id, p.status_id
    FROM project p LEFT JOIN study s ON p.project_id = s.project_id
    LEFT JOIN (select * from ena_link where to_id in ({0})) l on l.from_id = s.project_id
    WHERE {1} ORDER BY p.first_created, p.project_id DESC
    """.format(",".join([f"'{u}'" for u in umbrella_project_ids]), where_clause_studies)
    print(sql)

    ignore_projects = []
    if opts.ignore_projects:
        ignore_projects = [line.strip() for line in open(opts.ignore_projects, 'r')]

    # batch the ids into bits of 500 for use in SQL queries later
    # performance of the `WHERE ... IN ()` clause degrades after 500
    covid_study_list = [[]]
    cursor = connection.cursor()
    for row in cursor.execute(sql):
        [project_id, study_id, tax_id, s_title, p_title, first_created, sci_name, umbrella, status] = row
        if project_id in ignore_projects:
            continue
        try:
            if project_id == covid_study_list[-1][-1]['project']:
                covid_study_list[-1][-1]['umbrella'] += ",{}".format(umbrella)
                continue
        except IndexError:
            pass

        title = s_title if s_title else p_title.read() # .read to convert CLOB to string
        this_study = {
            'project': project_id, 'study': study_id, 'tax': tax_id,
            'title': title, 'first_created': first_created.strftime("%Y-%m-%d"),
            'sci_name': sci_name, 'umbrella': umbrella, 'status': status
        }
        covid_study_list[-1].append(this_study)
        if len(covid_study_list[-1]) >= 500:
            covid_study_list.append([])
    cursor.close()

    return covid_study_list

"""
    Query ENA for number of sequences in (nested) list of projects
"""
def sequence_count(connection, study_lists):
    cursor = connection.cursor()

    seq_counts = {}
    for batch in study_lists:
        string_list = ",".join(["'{}'".format(i['project']) for i in batch])
        for row in cursor.execute(f"SELECT study_id, count(*) FROM dbentry WHERE study_id IN ({string_list}) group by study_id"):
            seq_counts[row[0]] = row[1]
    cursor.close()

    return seq_counts

"""
    Query ERA for number of reads in (nested) list of projects
"""
def read_count(connection, study_lists):
    cursor = connection.cursor()

    read_counts = {}
    for batch in study_lists:
        string_list = ",".join(["'{0}'".format(i['study']) for i in batch])
        for row in cursor.execute(f"SELECT study_id, count(*) FROM experiment e JOIN run r ON e.experiment_id = r.experiment_id WHERE e.study_id IN ({string_list}) group by study_id"):
            read_counts[row[0]] = row[1]
    cursor.close()

    return read_counts

"""
    Join ENA to ERA to link sequences to their samples and fetch taxon info
"""
def fetch_tax_from_seqs(connection, project_id):
    cursor = connection.cursor()

    tax_info = []
    sql = """
    SELECT s.tax_id, s.scientific_name
    FROM dbentry d JOIN ena_xref x ON d.primaryacc# = x.acc
        JOIN sample@erapro.era_reader s ON s.biosample_id = x.xref_acc
    WHERE d.study_id = '{0}' group by s.tax_id, s.scientific_name
    """.format(project_id)
    for row in cursor.execute(sql):
        tax_info.append((row[0], row[1] if row[1] else ''))
    cursor.close()

    return tax_info

"""
    Link reads to their samples and fetch taxon info
"""
def fetch_tax_from_reads(connection, study_id):
    cursor = connection.cursor()

    tax_info = []
    sql = """
    SELECT s.tax_id, s.scientific_name FROM experiment@erapro.era_reader e
    LEFT JOIN experiment_sample@erapro.era_reader es ON es.experiment_id = e.experiment_id
    LEFT JOIN sample@erapro.era_reader s ON es.sample_id = s.sample_id
    WHERE e.study_id = '{0}' group by s.tax_id, s.scientific_name
    """.format(study_id)
    for row in cursor.execute(sql):
        tax_info.append((row[0], row[1] if row[1] else ''))
    cursor.close()

    return tax_info

"""
    Add a record to a log, with some formatting adjustments
"""
def add_to_log(log, entry):
    try:
        if type(entry['s_tax']) == list:
            entry['s_tax'] = ','.join(entry['s_tax'])
        if type(entry['s_sci_name']) == list:
            entry['s_sci_name'] = ','.join(entry['s_sci_name'])
    except KeyError:
        entry['s_tax'] = 'NULL'
        entry['s_sci_name'] = 'NULL'

    for k in entry.keys():
        if entry[k] == None:
            entry[k] = 'NULL'

    log.append(entry)

"""
    Check project metadata and filter into 5 logs:
        * log1 : sars-cov-2 sequences
        * log2 : other coronaviral sequences
        * log3 : metagenomes
        * log4 : human sequences (infected with covid-19)
        * log5 : other host sequences
"""
def filter_into_logs(entry_raw):
    entry = entry_raw.copy()

    # filter into different logs on taxon id, scientific name and study title
    study_title = entry['title'] if entry['title'] else 'None'
    [project_taxon_id, project_scientific_name] = [entry['tax'], entry['sci_name'] if entry['sci_name'] else '']
    [sample_taxon_ids, sample_scientific_names] = [[],[]]
    try:
        [sample_taxon_ids, sample_scientific_names] = [entry['s_tax'], entry['s_sci_name']]
    except KeyError: # no sample taxon info fetched - just use project info
        pass

    # first, find SARS-CoV-2
    if project_taxon_id == sars_tax_id:
        add_to_log(log1, entry)
        print("--> A. assigned to log 1 (sars-cov-2)") if opts.debug else ''
        project_scientific_name = ''
    elif sars_tax_id in sample_taxon_ids:
        add_to_log(log1, entry)
        print("--> B. assigned to log 1 (sars-cov-2)") if opts.debug else ''
        idx = sample_taxon_ids.index(sars_tax_id)
        del sample_taxon_ids[idx]
        del sample_scientific_names[idx]

    # next, find human
    if project_taxon_id == human_tax_id and 'metagenom' not in study_title:
        add_to_log(log4, entry)
        print("--> A. assigned to log 4 (human)") if opts.debug else ''
        project_scientific_name = ''
    elif human_tax_id in sample_taxon_ids and 'metagenom' not in study_title:
        add_to_log(log4, entry)
        print("--> B. assigned to log 4 (human)") if opts.debug else ''
        idx = sample_taxon_ids.index(human_tax_id)
        del sample_taxon_ids[idx]
        del sample_scientific_names[idx]

    if project_scientific_name == '' and len(sample_scientific_names) == 0:
        return

    # find other viruses
    if 'virus' in project_scientific_name or 'viridae' in project_scientific_name:
        add_to_log(log2, entry)
        print("--> A. assigned to log 2 (other viruses)") if opts.debug else ''
        project_scientific_name = ''
    else:
        added = False
        i, l = 0, len(sample_taxon_ids)
        while i < l:
            if 'virus' in sample_scientific_names[i] or 'viridae' in sample_scientific_names[i]:
                if not added:
                    add_to_log(log2, entry)
                    print("--> B. assigned to log 2 (other viruses)") if opts.debug else ''
                    added = True
                del sample_taxon_ids[i]
                del sample_scientific_names[i]
                l -= 1
                i -= 1
            i += 1

    if project_scientific_name == '' and len(sample_scientific_names) == 0:
        return

    # find metagenomes
    if 'metagenom' in project_scientific_name:
        add_to_log(log3, entry)
        print("--> A. assigned to log 3 (metagenomes)") if opts.debug else ''
        project_scientific_name = ''
    else:
        added = False
        i, l = 0, len(sample_taxon_ids)
        while i < l:
            if 'metagenom' in sample_scientific_names[i]:
                if not added:
                    add_to_log(log3, entry)
                    print("--> B. assigned to log 3 (metagenomes)") if opts.debug else ''
                    added = True
                del sample_taxon_ids[i]
                del sample_scientific_names[i]
                l -= 1
                i -= 1
            i += 1

    # is there anything left that hasn't been classified already?
    if project_scientific_name != '' or len(sample_scientific_names) > 0:
        add_to_log(log5, entry)
        print("--> assigned to log 5 (other hosts)") if opts.debug else ''

"""
    Fetch COVID-related projects from ERA, add read and sequence counts.
    Where a project doesn't have taxonomy information, fetch that from samples
    and filter project into 5 logs (mixed projects will be added to all applicable):
        * log1 : sars-cov-2 sequences
        * log2 : other coronaviral sequences
        * log3 : metagenomes
        * log4 : human sequences (infected with covid-19)
        * log5 : other host sequences
"""
def fetch_and_filter_projects():
    # start with ERA
    print("Connecting to ERAREAD...")
    db_conn_era = setup_ERA_connection()
    print("Querying ERAREAD for COVID-19 projects...")
    covid_study_lists = fetch_studies(db_conn_era)
    print("Querying ERAREAD for COVID-19 read counts...")
    read_counts = read_count(db_conn_era, covid_study_lists)
    db_conn_era.close()

    print("Connecting to ENAPRO...")
    db_conn_ena = setup_ENA_connection()
    print("Querying ERAPRO for COVID-19 sequence counts...")
    sequence_counts = sequence_count(db_conn_ena, covid_study_lists)

    # complete data and filter
    total_studies, x = sum([len(batch) for batch in covid_study_lists]), 0
    print("Processing {}/{} projects : {}%".format(x, total_studies, round((x/total_studies)*100)), end="\r")
    for batch in covid_study_lists:
        for entry in batch:
            x += 1
            if x%10 == 0:
                print("Processing {}/{} projects : {}%".format(x, total_studies, round((x/total_studies)*100)), end="\r")
            print("pre-completed project:  ") if opts.debug else ''
            print(entry) if opts.debug else ''

            # zip read/seq counts into data
            try:
                entry['read_count'] = read_counts[entry['study']]
            except KeyError:
                entry['read_count'] = 0

            try:
                entry['seq_count'] = sequence_counts[entry['project']]
            except KeyError:
                entry['seq_count'] = 0

            # fetch taxonomy info from samples if no info available from project
            try:
                entry['tax'], entry['sci_name'] = custom_project_tax_info[entry['project']]
            except KeyError:
                pass

            if not entry['tax']:
                sample_tax = []
                if entry['seq_count'] == 0:
                    sample_tax = fetch_tax_from_reads(db_conn_ena, entry['study'])
                else:
                    sample_tax = fetch_tax_from_seqs(db_conn_ena, entry['project'])
                entry['s_tax'] = [t[0] for t in sample_tax]
                entry['s_sci_name'] = [t[1] for t in sample_tax]
            print("post-completed project: ") if opts.debug else ''
            print(entry) if opts.debug else ''

            filter_into_logs(entry)
            print("\n-----------------\n\n") if opts.debug else ''
    print("Processed {}/{} projects : {}%   \n\n".format(x, total_studies, 100))

"""
    Generate and create the output directory
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


file_header = ['umbrella', 'project', 'study', 'first_created', 'title', 'read_count',
    'seq_count','tax', 's_tax', 'sci_name', 's_sci_name', 'status'
]
new_headers = ['Umbrella ID(s)', 'Project Acc', 'Study Acc', 'First Created', 'Description',
    'Read Count', 'Sequence Count', 'Project Taxon ID', 'Samples Taxon ID(s)', 'Project Scientific Name',
    'Samples Scientific Name(s)', 'Project Status ID'
]
header_map = {file_header[i]:new_headers[i] for i in range(len(file_header))}
"""
    Convert each log to a pandas dataframe and write to spreadsheet and generate
    list of projects that need to be added to a new umbrella
"""
def write_logs(log, file_prefix, outdir, xls_writer, umbrella_id):
    with open(f"{file_prefix}.tsv", 'w') as log_tsv:
        dataframe = pd.DataFrame(log, columns = file_header)
        dataframe = dataframe.rename(columns=header_map)
        dataframe.to_excel(xls_writer, sheet_name=file_prefix, index=False)

    project_accs_no_umbrella = {}
    for l in log:
        if not l['umbrella'] or umbrella_id not in l['umbrella']:
            # use dictionary to get a uniq list
            project_accs_no_umbrella[l['project']] = 1

    with open(f"{outdir}/{file_prefix}.projects.no_umbrella.list", 'w') as log_proj:
        log_proj.write("\n".join(project_accs_no_umbrella))

    return project_accs_no_umbrella

"""
    Generate project and submissions XMLs to update umbrella projects
"""
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
        sys.exit(1)

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
    submit_url = "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/"
    if opts.environment == 'prod':
        submit_url = "https://www.ebi.ac.uk/ena/submit/drop-box/submit/"

    curl_cmd = f"curl -u {user_pass} -F \"SUBMISSION=@{submission_xml_file}\" -F \"PROJECT=@{umbrella_xml_file}\" \"{submit_url}\" > {submission_xml_file}.receipt"
    print(f"Running: {curl_cmd}" if opts.submit else f"Would run: {curl_cmd}")
    if opts.submit:
        os.system(curl_cmd)


#------------------------#
#          MAIN          #
#------------------------#
if __name__ == "__main__":
    outdir = create_outdir()

    # fetch and parse the data
    fetch_and_filter_projects()

    # format output and write files
    file_prefixes = [
        'log1.sars-cov-2', 'log2.other_coronavirus', 'log3.metagenomes',
        'log4.human', 'log5.other_hosts'
    ]
    print("Writing files...\n")
    xls_writer = pd.ExcelWriter(f"{outdir}/covid_logs.xlsx", engine='xlsxwriter')
    l1_no_umb = write_logs(log1, file_prefixes[0], outdir, xls_writer, umbrella_project_ids[0])
    l2_no_umb = write_logs(log2, file_prefixes[1], outdir, xls_writer, umbrella_project_ids[1])
    l3_no_umb = write_logs(log3, file_prefixes[2], outdir, xls_writer, umbrella_project_ids[2])
    l4_no_umb = write_logs(log4, file_prefixes[3], outdir, xls_writer, umbrella_project_ids[3])
    l5_no_umb = write_logs(log5, file_prefixes[4], outdir, xls_writer, umbrella_project_ids[4])
    xls_writer.save()
    print(f"Files written to '{outdir}'\n\n")

    # update the umbrellas
    repo_root = os.path.realpath(__file__).replace('/scripts/fetch_covid_projects_from_db.py', '')
    xml_dir = "{}/xml/covid19_umbrellas".format(repo_root)
    update_umbrella(l1_no_umb, "{}/{}.umbrella.xml".format(xml_dir, file_prefixes[0]), outdir)
    update_umbrella(l2_no_umb, "{}/{}.umbrella.xml".format(xml_dir, file_prefixes[1]), outdir)
    update_umbrella(l3_no_umb, "{}/{}.umbrella.xml".format(xml_dir, file_prefixes[2]), outdir)
    update_umbrella(l4_no_umb, "{}/{}.umbrella.xml".format(xml_dir, file_prefixes[3]), outdir)
    update_umbrella(l5_no_umb, "{}/{}.umbrella.xml".format(xml_dir, file_prefixes[4]), outdir)
