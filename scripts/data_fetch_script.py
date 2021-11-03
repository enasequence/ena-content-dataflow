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


import argparse, hashlib, os, subprocess, sys, time
import sys
import shlex
import subprocess
import requests, sys
import re
import requests
import json
import datetime
from dateutil.relativedelta import relativedelta, MO
parser = argparse.ArgumentParser(prog='data_fetch_script.py', formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog="""
        + ============================================================ +
        |  European Nucleotide Archive (ENA) data flow monitoring Tool  |
        |                                                              |
        |             Tool to to fetch data from NCBI or ENA           |
        + =========================================================== +
This script is used only to fetch data from NCBI or ENA according to the options provided by the user. 
The options are provided by either  
        #### Argument format ( example: python3  data_fetch_script.py -r ENA -org 2697049 -db sequences )
        #### User input by only running the script as "python3  data_fetch_script.py" and following the instructions afterwards        
        """)
parser.add_argument('-db', '--database', help='Database type, sequences or reads', type=str, required=False)
parser.add_argument('-org', '--organism', help='Taxon id or scientific name', type=str, required=False)
parser.add_argument('-r', '--repository', help='Name of the repository, ENA, NCBI, covid19dataportal, ebisearch, all(ENA-advanced search, ebisearch, covid19dataportal and NCBIvirus)', type=str, required=False)
args = parser.parse_args()

# generate and create the output directory
def create_outdir():
    now = datetime.datetime.now()
    now_str = now.strftime("%d%m%y")
    outdir = f"databases_logs_{now_str}"
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    return outdir


#Using ensembl REST to access NCBItaxonomy to standerise the organsim input
def fetching_taxonomy():
    server = "https://rest.ensembl.org"
    s = 'id'
    ext = "/taxonomy/id/{}?simple=1".format(organism)
    r = requests.get(server + ext, headers={"Content-Type": "application/json"})
    result = re.search(s, r.text)
    if not r.ok:
        r.raise_for_status()
        sys.exit()
    data = json.loads(r.content)
    sciname = str(data['tags']['name']).strip('[').strip("'").strip(']').strip("'")
    taxid = data['id']
    return taxid, sciname


# Creating the script for fetching data from advance search in ENA
def advanced_search_data_fetching(database):
    acc = 'accession'
    date ='last_updated'
    if database == 'sequences':
        database = 'sequence'
    elif database == 'reads':
        database = 'read_experiment'
        acc = 'experiment_accession'
        date = 'first_public'

    print('PROCESSING DATA FROM ADVANCED SEARCH...................................................................')
    server = "https://www.ebi.ac.uk/ena/portal/api/search"
    ext = "?result={}&query=tax_eq({})&fields={}%2C{}&format=json&limit=0".format(database, tax_fetch[0], acc, date)
    command = requests.get(server + ext, headers={"Content-Type": "application/json"})
    status = command.status_code
    if status == 500:
        sys.stderr.write("Attention: Internal Server Error, the process has stopped and skipped ( Data might be incomplete )\n")
    data = json.loads(command.content)
    with open(f"{outdir}/{'ENA'}.{database}.log.txt", "w") as f:
        if acc == 'experiment_accession':
            for x in data:
                output = x["experiment_accession"], x['first_public']
                f.write("\t".join(output) + "\n")
        else:
            for x in data:
                output = x["accession"], x['last_updated']
                f.write("\t".join(output) + "\n")

    print('ENA-Advanced Search Data written to ' + f"{outdir}/{'ENA'}.{database}.log.txt")


# Creating the script for fetching data from COVID19dataPortal in ENA
def covid19portal_data_fetching(database):
    dateformat = 'creation_date'
    if database == 'sequence':
        database = 'sequences'
    elif database == 'reads':
        database = 'raw-reads'
        dateformat = 'first_public_date'

    print('PROCESSING DATA FROM COVID-19 DATA PORTAL...................................................................')

    # Using While loop to go through all the pages per each month of their release by the covid19dataportal API
    now = datetime.datetime.now()
    start = datetime.datetime(2020, 1, 1)
    delta = relativedelta(months=1)
    while start <= now:
        page = 0
        year=start.strftime("%Y")
        month =start.strftime("%m")
        start += delta
        while page >= 0:
            page = page + 1
            server = "https://www.covid19dataportal.org/api/backend/viral-sequences"
            ext = "/{}?query=TAXON:2697049 AND {}:{}-{}&page={}&size=1000&format=idlist".format(database,dateformat,year,month, page)
            command = requests.get(server + ext, headers={"Content-Type": "application/json"})
            status = command.status_code
            if status == 500:
                break
            else:
                data = json.loads(command.content)
                jsonData = data["entries"]
                if start == '2020-02-01 00:00:00':
                    f = open(f"{outdir}/{'Covid19DataPortal'}.{database}.log.txt", "w")
                else:
                    f = open(f"{outdir}/{'Covid19DataPortal'}.{database}.log.txt", "a")
                for x in jsonData:
                    output = x["id"]
                    f.write(output + "\n")
                f.close()
    print('Covid19dataportal Data written to ' + f"{outdir}/{'Covid19DataPortal'}.{database}.log.txt")



# Creating the script for fetching data from ebisearch in ENA
def ebisearch_data_fetching(database):
    date = 'creation_date'
    if database == 'sequences':
        database = 'embl-covid19'
    elif database == 'reads':
        database = 'sra-experiment-covid19'       
        date = 'first_public'
    print('PROCESSING DATA FROM EBI SEARCH...................................................................')

    # Using While loop to go through all the pages in the ebisearch API
    now = datetime.datetime.now()
    start = datetime.datetime(2020, 1, 1)
    delta = relativedelta(months=1)
    while start <= now:
        start_item = 0
        year=start.strftime("%Y")
        month =start.strftime("%m")
        start += delta
        while start_item >= 0:
            server = "http://www.ebi.ac.uk/ebisearch/ws/rest"
            ext = "/{}?query=TAXON:{} AND {}:{}-{}&fields=acc,{}&format=json&size=1000&start={}".format(database, tax_fetch[0],date,year,month, date, start_item)
            start_item = start_item + 1000
            command = requests.get(server + ext, headers={"Content-Type": "application/json"})
            status = command.status_code
            if status in [400, 500]:
                if status == 500:
                    sys.stderr.write(
                        "Attention: Internal Server Error, the process has stopped and skipped ( Data might be incomplete )\n")
                break
            else:
                data = json.loads(command.content)
                jsonData = data["entries"]
                if start == '2020-02-01 00:00:00':
                    f = open(f"{outdir}/{'EBIsearch'}.{database}.log.txt", "w")
                else:
                    f = open(f"{outdir}/{'EBIsearch'}.{database}.log.txt", "a")
                for x in jsonData:
                    output = [x['fields']['acc'][0], x['fields'][date][0]]
                    f.write("\t".join(output) + "\n")
                f.close()
    print('EBI-Search Data written to ' + f"{outdir}/{'ENA'}.{database}.log.txt")



#Creating script to fetch data from NCBIvirus
def NCBIvirus_data_fetching():
    print('PROCESSING  DATA IN NCBIVIRUS...................................................................')
    server = "https://api.ncbi.nlm.nih.gov/datasets/v1alpha/virus/taxon/{}/genome/table".format(tax_fetch[0])
    ext = "?refseq_only=false&annotated_only=false&table_fields=nucleotide_accession"
    command = requests.get(server + ext, headers={"Content-Type": "text/tab-separated-values"})
    status = command.status_code
    print(status)
    if status == 500:
        sys.stderr.write("Attention: Internal Server Error, the process has stopped and skipped ( Data might be incomplete )\n")

    with open(f"{outdir}/{'NCBI'}.{database}.log.txt", "w") as f:
        dec_split = command.content.decode('utf-8').strip('Nucleotide Accession')
        dec_split= dec_split.strip("\r\n")
        trimmed_accessions = [accession.split('.')[0] for accession in dec_split.split("\n")]
        f.write("\n".join(trimmed_accessions))
    print('NCBI Data written to ' + f"{outdir}/{'NCBI'}.{database}.log.txt")




# Creating the script for fetching data from other databases in NCBI (installation of edirect dependency required)
def NCBI_nucleotide_data_fetching():
        print('PROCESSING  DATA IN NUCLEOTIDE...................................................................')
        command = 'esearch -db {} -query "{}[ORGN]" | efetch -format acc '.format(
                    database, tax_fetch[1])
        sp = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        if "command not found" in err.decode():
            sys.stderr.write(err.decode()+ "\n This command uses 'esearch' and 'efetch' functions.\n "
                                           "You might need to download and install 'entrez-direct' to fetch any data from NCBI. "
                                           "\n Please follow the instruction in the link provided below. "
                                           "\n https://www.ncbi.nlm.nih.gov/books/NBK179288/ \n "
                                           "Please note that 'entrez-direct' only runs on Unix and Macintosh environments or under the Cygwin Unix-emulation environment on Windows \n ")
            exit(1)
        else:
            sys.stderr.write(err.decode())
        stdoutOrigin=sys.stdout
        with open(f"{outdir}/{'NCBI'}.{database}.log.txt", "w") as sys.stdout:
            dec_split = out.decode()
            trimmed_accessions = [accession.split('.')[0] for accession in dec_split.split("\n")]
            for x in trimmed_accessions:
                print(x)
            sys.stdout = stdoutOrigin
            print('NCBI Data written to ' + f"{outdir}/{'NCBI'}.{database}.log.txt")



#Creating Script to fetch data from SRA
def NCBI_SRA_data_fetching():
        print('PROCESSING  DATA IN SRA...................................................................')
        command = 'esearch -db {} -query "{}[ORGN]"   | esummary   | xtract -pattern DocumentSummary -element Run@acc, Experiment@acc, UpdateDate'.format(
            database, tax_fetch[1])
        sp = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        if "command not found" in err.decode():
            sys.stderr.write(err.decode()+ "\n This command uses 'esearch' and 'xtract' functions.\n "
                                           "You might need to download and install 'entrez-direct' to fetch any data from NCBI. "
                                           "\n Please follow the instruction in the link provided below. "
                                           "\n https://www.ncbi.nlm.nih.gov/books/NBK179288/ \n "
                                           "*****Please note that 'entrez-direct' only runs on Unix and Macintosh environments or under the Cygwin Unix-emulation environment on Windows***** \n ")
            exit(1)
        else:
            sys.stderr.write(err.decode())
        stdoutOrigin = sys.stdout
        with open(f"{outdir}/{'NCBI'}.{database}.log.txt", "w") as sys.stdout:
            reads_output_1 = re.split('[\n]',out.decode())
            reads_output = list(filter(None,reads_output_1))
            for row in reads_output:
                row_list = row.split("\t")
                exp_date = row_list[-2:]
                run_id = row_list[:-2]
                final_row = ",".join(run_id), exp_date[0], exp_date[1]
                print("\t".join(final_row))
            sys.stdout = stdoutOrigin
            print('NCBI written to ' + f"{outdir}/{'NCBI'}.{database}.log.txt")




# Creating the script for fetching data from NCBI
def NCBI_data_fetching():
    print('PROCESSING DATA FROM NCBI...................................................................')
    if database == 'ncbivirus':
        NCBIvirus_data_fetching()
    elif database == 'nucleotide':
        NCBI_nucleotide_data_fetching()
    elif database == 'sra':
        NCBI_SRA_data_fetching()
    else:
        sys.stderr.write(f'The NCBI database "{database}" does not exist or its not supported, please check your spelling or choose a different database and try again')



##############
#   MAIN     #
###############
if args.repository == None:
    repository = input("Name of the repository, ENA, NCBI, covid19dataportal,ebisearch,all: ").lower()
else:
    repository = args.repository.lower()
if repository in ['ena', 'ncbi','covid19dataportal', 'ebisearch', 'all']:

    if args.organism == None:
        organism = input("please indicate the taxon id or scientific name (ex: Severe acute respiratory syndrome coronavirus 2 or 2697049 ): ").lower()
    else:
        organism = args.organism.lower()


    #Calling the create_outdir module
    outdir = create_outdir()


    #Calling the ensembl REST to access NCBItaxonomy
    tax_fetch = fetching_taxonomy()


    # Running the script for fetching data from NCBI
    if repository == 'ncbi':
        if args.database == None:
            database = input("please indicate the dataset type, ex: ncbivirus, nucleotide, SRA: ").lower()
        else:
            database = args.database.lower()
        NCBI_data_fetching()
    else:
        if args.database == None:
            database = input("please indicate the dataset type, ex: sequences or reads: ").lower()
        else:
            database = args.database.lower()
        if database in ['sequences', 'reads']:

            # Running the script for fetching data from advance search in ENA
            if repository == 'ena':
                advanced_search_data_fetching(database)

            # Running the script for fetching data from COVID19dataPortal in ENA
            elif repository == 'covid19dataportal':
                covid19portal_data_fetching(database)

            # Running the script for fetching data from ebisearch in ENA
            elif repository == 'ebisearch':
                ebisearch_data_fetching(database)

        #Running the script for all the databases
            elif repository == 'all':
                advanced_search_data_fetching(database)
                covid19portal_data_fetching(database)
                ebisearch_data_fetching(database)
                if database == 'sequences':
                    database = 'ncbivirus'
                    NCBIvirus_data_fetching()
                else:
                    database = 'sra'
                    NCBI_SRA_data_fetching()
        else:
            sys.stderr.write(f'The dataset type "{database}" does not exist or its not supported yet, please check your spelling and try again')
else:
    sys.stderr.write(f'The repository type "{repository}" does not exist or its not supported yet, please check your spelling and try again')
    sys.exit(1)


print('DONE...........................................................................')
