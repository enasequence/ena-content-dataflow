import argparse, hashlib, os, subprocess, sys, time
import sys
import shlex
import subprocess
import requests, sys
from textops import *
import re
import requests
import json
from datetime import datetime
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
parser.add_argument('-org', '--organism', help='Organism id or scientific name', type=str, required=False)
parser.add_argument('-r', '--repository', help='Name of the repository, ENA, NCBI, covid19dataportal,ebisearch', type=str, required=False)
args = parser.parse_args()

# generate and create the output directory
def create_outdir():
    now = datetime.now()
    now_str = now.strftime("%d%m%y")
    outdir = f"databases_logs_{now_str}"
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    return outdir

#Calling the create_outdir module
outdir = create_outdir()

if args.repository == None:
    repository = input("Name of the repository, ENA, NCBI, covid19dataportal,ebisearch: ").lower()
else:
    repository = args.repository.lower()
if args.organism == None:
    organism = input("please indicated the organism id or scientific name (ex: Severe acute respiratory syndrome coronavirus 2 or 2697049 ): ").lower()
else:
    organism = args.organism.lower()

#Using ensembl rest to access NCBItaxonomy to standerise the organsim input
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
taxid= data['id']

# Creating the script for fetching data from advance search in ENA
if repository == 'ena':
    if args.database == None:
        database= input("please indicate the dataset type, ex: sequence or reads: ").lower()
        if database == 'sequences':
            database = 'sequence'
        elif database == 'reads':
            database = 'read_experiment'
    else:
        database = args.database.lower()
        if database == 'sequences':
            database = 'sequence'
        elif database == 'reads':
            database = 'read_experiment'
    print('PROCESSING...................................................................')
    command = 'curl -X POST -H "Content-Type: application/x-www-form-urlencoded" -d "'"result={}&query=tax_eq({})&fields=accession&format=tsv"'" "https://www.ebi.ac.uk/ena/portal/api/search"'.format(database, taxid)
    sp = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = sp.communicate()
    print(command)
    stdoutOrigin = sys.stdout
    sys.stdout = open(f"{outdir}/{'ENA'}.{database}.log.txt", "w")
    dec_split = out.decode().strip('accession\n')
    print(dec_split)
    sys.stdout.close()
    sys.stdout = stdoutOrigin
# Creating the script for fetching data from COVID19dataPortal in ENA
elif repository == 'covid19dataportal':
    if args.database == None:
        database= input("please indicate the dataset type, ex: sequences or reads: ").lower()
        if database == 'sequence':
            database = 'sequences'
        elif database == 'reads':
            database = 'raw-reads'
    else:
        database = args.database.lower()
        if database == 'sequence':
            database = 'sequences'
        elif database == 'reads':
            database = 'raw-reads'
    print('PROCESSING...................................................................')

   # Using While loop to go through all the pages in the covid19dataportal API
    page = 1
    while page >= 0:
        page = page + 1
        server = "https://www.covid19dataportal.org/api/backend/viral-sequences"
        ext = "/{}?query=TAXON:2697049&page={}&size=1000".format(database, page)
        command = requests.get(server+ext, headers={"Content-Type": "application/json"})
        status = command.status_code
        if status == 500:
            print(command)
            break
        else:
            data = json.loads(command.content)
            jsonData = data["entries"]
            if page==1:
            # script to select the keys in the jason output (not active)
               # jsonData = data["entries"]
                #for x in jsonData:
                    #keys = x.keys()
                    #print(keys)
                    #values = x.values()
                    #print(values)
                f = open(f"{outdir}/{'Covid19DataPortal'}.{database}.log.txt", "w")
                for x in jsonData:
                    output=x["id"]
                    f.write(output+"\n")
                f.close()
            else:
                f = open(f"{outdir}/{'Covid19DataPortal'}.{database}.log.txt", "a")
                for x in jsonData:
                    output = x["id"]
                    f.write(output + "\n")
                f.close()

# Creating the script for fetching data from ebisearch in ENA
elif repository == 'ebisearch':
    if args.database == None:
        database= input("please indicate the dataset type, ex: sequences or reads: ").lower()
        if database == 'sequences':
            database = 'embl-covid19'
        elif database == 'reads':
            database = 'sra-experiment-covid19'
    else:
        database = args.database.lower()
        if database == 'sequences':
            database = 'embl-covid19'
        elif database == 'reads':
            database = 'sra-experiment-covid19'
    print('PROCESSING...................................................................')

    # Using While loop to go through all the pages in the ebisearch API
    start = 0
    while start >= 0:
        server = "http://www.ebi.ac.uk/ebisearch/ws/rest"
        ext = "/{}?query=TAXON:{}&fields=acc&format=json&size=1000&start={}".format(database, taxid, start)
        start = start + 1000
        command = requests.get(server+ext, headers={"Content-Type": "application/json"})
        status = command.status_code
        if status == 400:
            print(command)
            break
        else:
            data = json.loads(command.content)
            jsonData = data["entries"]
            if start == 0:
                f = open(f"{outdir}/{'EBIsearch'}.{database}.log.txt", "w")
                for x in jsonData:
                    output = x["id"]
                    f.write(output + "\n")
                f.close()
            else:
                f = open(f"{outdir}/{'EBIsearch'}.{database}.log.txt", "a")
                for x in jsonData:
                    output = x["id"]
                    f.write(output + "\n")
                f.close()

# Creating the script for fetching data from NCBI
elif repository == 'ncbi':
    if args.database == None:
        database = input("please indicate the dataset type, ex: ncbivirus, nucleotide, SRA: ").lower()
    else:
        database = args.database.lower()
    print('PROCESSING...................................................................')

    # Creating the script for fetching data from NCBIvirus in NCBI
    if database == 'ncbivirus':
        command = 'curl -X GET "https://api.ncbi.nlm.nih.gov/datasets/v1alpha/virus/taxon/"{}"/genome/table?refseq_only=false&annotated_only=false&table_fields=nucleotide_accession" -H "Accept: application/json"'.format(taxid)
        sp = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        print(command)
        stdoutOrigin = sys.stdout
        sys.stdout = open(f"{outdir}/{'NCBI'}.{database}.log.txt", "w")
        dec_split = out.decode().strip('Nucleotide Accession')
        dec_split= dec_split.strip("\r\n")

        # trimming the output by removing the version number (numbers after ".")
        trimmed_accessions = []
        for accession in dec_split.split("\n"):
            accession = accession.split('.')[0]
            trimmed_accessions.append(accession)
        for x in trimmed_accessions:
            print(x)
        sys.stdout.close()
        sys.stdout = stdoutOrigin

    # Creating the script for fetching data from other databases in NCBI (installation of edirect dependency required)
    else:
        command = 'esearch -db {} -query {}"[ORGN]" | efetch -format acc '.format(
                    database, sciname)
        sp = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        print(command)
        stdoutOrigin=sys.stdout
        sys.stdout = open(f"{outdir}/{'NCBI'}.{database}.log.txt", "w")
        dec_split = out.decode()

        #trimming the output by removing the version number (numbers after ".")
        trimmed_accessions = []
        for accession in dec_split.split("\n"):
            accession = accession.split('.')[0]
            trimmed_accessions.append(accession)
        for x in trimmed_accessions:
            print(x)
        sys.stdout.close()
        sys.stdout = stdoutOrigin
print('DONE...........................................................................')
