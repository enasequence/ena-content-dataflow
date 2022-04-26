import argparse, hashlib, os, subprocess, sys, time
import pandas as pd
import requests
import json
import cx_Oracle
"""
Request username and password for databases
"""
def get_oracle_usr_pwd():
    return ['era_reader', 'reader']


"""
Setup the connection ERAPRO. 
"""
def setup_connection():
    oracle_usr, oracle_pwd = get_oracle_usr_pwd()
    client_lib_dir = os.getenv('ORACLE_CLIENT_LIB')
    if not client_lib_dir or not os.path.isdir(client_lib_dir):
        sys.stderr.write("ERROR: Environment variable $ORACLE_CLIENT_LIB must point at a valid directory\n")
        exit(1)
    cx_Oracle.init_oracle_client(lib_dir=client_lib_dir)
    connection = None
    try:
        dsn = cx_Oracle.makedsn("ora-vm-009.ebi.ac.uk", 1541, service_name="ERAPRO")
        connection = cx_Oracle.connect(oracle_usr, oracle_pwd, dsn, encoding="UTF-8")
        return connection
    except cx_Oracle.Error as error:
        print(error)



"""
Query ENAPRO dataset, process the data and fetching release date from NCBI nucleotide database. Print to a file.
"""
def SQLstat_fetch_dataframe(connection):
   # This Part is for querying ERAPRO
    c = connection.cursor()
    header = "\t".join(['Webin Account', 'Project ID', 'Project Status ID', 'Sample ID', 'Sample Status ID','RUN ID', 'RUN Status ID', 'Experiment ID', 'Experiment Status ID', 'Country'])
    sql_row = []
    c.execute("select sa.submission_account_id, d.project_id,d.status_id, d.biosample_id, d.status_sample, d.run_id, d.status_run, d.experiment_id, d.status_exp, sa.country from\
                (select sp.submission_account_id, c.project_id, c.status_id, c.status_run, sp.biosample_id, sp.status_id as status_sample, c.run_id, c.experiment_id, c.status_exp from\
                (select b.project_id, b.status_id, es.sample_id, b.run_id, b.status_run, b.experiment_id, b.status_exp from\
                (select a.project_id, a.status_id, r.run_id, r.status_id as status_run, r.experiment_id, a.status_exp from\
                (select st.project_id, st.status_id, ex.experiment_id, ex.status_id as status_exp from experiment ex left join study st on ex.study_id =st.study_id) a\
                join run r on a.experiment_id = r.experiment_id) b join experiment_sample es on es.experiment_id= b.experiment_id) c\
                join sample sp on c.sample_id = sp.sample_id where sp.tax_id in ('2697049')) d left join submission_account sa on sa.submission_account_id = d.submission_account_id ")
    for row in c:
        sql_row.append([row[0],row[1],row[2], row[3],row[4], row[5], row[6], row[7], row[8], row[9]])
    df = pd.DataFrame(sql_row, columns= ['Webin Account', 'Project ID', 'Project Status ID', 'Sample ID', 'Sample Status ID','RUN ID', 'RUN Status ID', 'experiment_accession', 'Experiment Status ID', 'Country'])
    return df


def fetching_seq_data():
    server = "https://www.ebi.ac.uk/ena/portal/api/search"
    ext = "?result=sequence&query=tax_tree(2697049)&fields=accession,first_public,country&format=json&limit=0"
    command = requests.get(server + ext, headers={"Content-Type": "application/json"})
    data = json.loads(command.content)
    database='sequence'
    return [data, database]

def fetching_reads_data():
    server = "https://www.ebi.ac.uk/ena/portal/api/search"
    ext = r"?result=read_experiment&query=tax_tree(2697049)&fields=accession,first_public,country&format=json&limit=0"
    command = requests.get(server + ext, headers={"Content-Type": "application/json"})
    read_data = json.loads(command.content)
    database = 'reads'
    return [read_data, database]

def dataframe (data, database):
    df = pd.DataFrame.from_dict(data, orient='columns')
    df['country'] = df['country'].str.split(':').str[0]
    df.sort_values('country')
    df1=df.rename(columns={'first_public': 'Submission Date'})
    filtered_df = df1.groupby(['Submission Date','country']).size().reset_index(name='Submissions')
    outdir = f"API_databases_files"
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    old_name = f'{outdir}/API.{database}.output.recent.csv'
    if os.path.exists(old_name):
        new_name = f'{outdir}/API.{database}.output.old.csv'
        os.rename(old_name, new_name)
    filtered_df.to_csv(old_name, index=False)
    return filtered_df

def stat_dataframe_reads (data, sql_output, database):
    df = pd.DataFrame.from_dict(data, orient='columns')
    df['country'] = df['country'].str.split(':').str[0]
    sql_api_join= pd.merge(sql_output, df[['experiment_accession', 'country']], on='experiment_accession', how='left')
    sql_api_join['Country'] = sql_api_join['Country'].fillna(sql_api_join['country'])
    sql_api_join.drop(['country'], inplace=True, axis=1)
    sql_api_join.to_csv(f"API_databases_files/SQL-API.{database}.log.csv", index=False)



###########################################
#                                         #
#                 MAIN                    #
#                                         #
###########################################
print('Fetching Reads Data  ........\n')
data_reads = fetching_reads_data()
dataframe_reads = dataframe(data_reads[0],data_reads[1])

print('Fetching Sequences Data  ........\n')
data_seq = fetching_seq_data()
dataframe_seq = dataframe(data_seq[0],data_seq[1])

sys.stderr.write("Connecting to ERAPRO...\n")
db_conn = setup_connection()
sys.stderr.write("Querying ERAPRO ..........\n")
sql_output= SQLstat_fetch_dataframe(db_conn)
stat_dataframe_reads(data_reads[0], sql_output,data_reads[1])
sys.stderr.write("*************END*************\n")