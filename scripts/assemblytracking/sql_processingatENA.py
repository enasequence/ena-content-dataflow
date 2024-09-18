#!/usr/bin/python3

# Copyright [2024] EMBL-European Bioinformatics Institute
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

import os, sys, argparse, configparser, logging, warnings
from datetime import datetime as dt
import pandas as pd
from sqlalchemy import create_engine, text

# Purpose of script - automate running of SQL queries and getting SQL results to remove copy and paste into SQL cmd step
# from the tracking process

def get_db_creds(database, config_file_path):
    # creates the 'engine' object which makes it possible to read an oracle database
    # database - ERA or ENA (include credentials for both in the config file)
    # inputs database info
    config = configparser.ConfigParser()
    config.read(config_file_path)
    if database == 'ERA':
        username = config['ERAREAD_DETAILS']['userName']
        password = config['ERAREAD_DETAILS']['password']
        host = config['ERAREAD_DETAILS']['host']
        port = config['ERAREAD_DETAILS']['port']
        serviceName = config['ERAREAD_DETAILS']['serviceName']
        engine_string = f"""oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={serviceName}"""
    elif database == 'ENA':
        username = config['ENAREAD_DETAILS']['userName']
        password = config['ENAREAD_DETAILS']['password']
        host = config['ENAREAD_DETAILS']['host']
        port = config['ENAREAD_DETAILS']['port']
        serviceName = config['ENAREAD_DETAILS']['serviceName']
        engine_string = f"""oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={serviceName}"""
    else:
        return 'no valid database selected, choose <ERA> or <ENA>'
    logging.basicConfig()
    logging.getLogger('sqlalchemy').setLevel(logging.ERROR)
    engine = create_engine(engine_string)
    return engine


def check_era_for_submission(tracking_file_path, tracking_files_path, xl_sheet0, engine):
    print(f'checking ERA database for new assembly submissions and status...')

    # Gets a list of names for searching in the ERA analysis/pipelite tables
    # get input names from 'In process to submit ' part of google sheet
    # get the term to search (after last .) and then add wildcard
    # NOTE: keep in touch with Sanger about naming conventions!!!
    names_to_track = xl_sheet0['name to track'].unique()  # read in unique names from excel input file


    pipelite_result_obj = []
    search_result_name_list = []
    with engine.connect() as conn: # opens the connection to database
        for name in names_to_track: # strips name into searchable format with wildcard (%)
            eraname = None
            split_name_list = name.split('.')
            tolid = split_name_list[0]
            if len(split_name_list) == 2:
                eraname = 'webin-genome-' + tolid + '%'
            elif len(split_name_list) == 3:
                eraname = 'webin-genome-' + tolid + '.' + split_name_list[1] + '%'
            else:
                row_index = xl_sheet0.loc[xl_sheet0["name to track"] == name].index[0]
                xl_sheet0.loc[row_index, "notes"] = 'unexpected name format, not tracked!'
            #print('name searched:', eraname)
            # uses sqlalchemy package to get sql result and read into a python dictionary
            temp_result = conn.execute(text("""select b.analysis_alias, b.first_created, b.bioproject_id, b.analysis_id,
                b.status_id, a.pipeline_name, a.state
                                        from pipelite2_process a
                                        join analysis b on a.process_id = b.analysis_id
                                        where analysis_alias like :eraname"""),{"eraname": eraname})
            result = []
            for row in temp_result:
                rowdict = {'analysis_alias': row.analysis_alias,
                           'first_created': row.first_created,
                           'bioproject_id': row.bioproject_id,
                           'analysis_id':row.analysis_id,
                           'status_id':row.status_id,
                           'pipeline_name':row.pipeline_name,
                           'state':row.state}
                result.append(rowdict)

            # save results by growing pipelite_result_obj (a list of dictionaries)
            namesfound = None
            mapping = {"analysis_alias": "name", "first_created": "submission date", "bioproject_id": "project",
                         "analysis_id": "analysis ID", "status_id": "status ID", "pipeline_name": "process",
                         "state": "status"}
            if not result:
                pass
            else:
                tempdf = pd.DataFrame(result)
                namesfound = tempdf['analysis_alias'].unique()
                namesfound = list(namesfound)
                result_renamed = [{mapping.get(k,k): v for k,v in row.items()} for row in result]
                pipelite_result_obj.extend(result_renamed)

            # add note to the original input sheet about results of searches, add name to search_result_names_list
            note = ''
            if namesfound is not None:
                note += 'found record(s) - '
                for name1 in namesfound:
                    name1 = name1.replace('webin-genome-', '')
                    name1 = name1.replace('_alternate_haplotype', ' alternate haplotype')
                    note += f'{name1}, '
                    search_result_name_list.append(name1)
            else:
                note += 'no submission found'
            row_index = xl_sheet0.loc[xl_sheet0["name to track"] == name].index[0]
            xl_sheet0.loc[row_index, "notes"] = note
            #print('name', name, 'info', note)

    conn.close()
    print('DONE')
    xl_sheet0.to_csv(f'{tracking_files_path}/In_process_to_submit.csv', index=False)
    print(f'''In Process to Submit notes added, check the file at: 
                {tracking_files_path}/In_process_to_submit.csv''')


    # filter 1 - get names from era result that are not complete (status != 'COMPLETED' in pipelite2 table)
    pipelite_result = pd.DataFrame(pipelite_result_obj)# convert obj to DataFrame
    incomplete_era = pipelite_result.loc[pipelite_result["status"] != 'COMPLETED']
    incomplete_era_name_list = list(incomplete_era['name'])
    incomplete_name_list = []
    # change eraname name to enapro version
    for item in incomplete_era_name_list:
        item = item.replace('webin-genome-', '')
        item = item.replace('_alternate_haplotype', ' alternate haplotype')
        incomplete_name_list.append(item)

    # filter 2 - remove assembly names that are already tracked
    tracked_names = pd.read_csv(tracking_file_path, sep='\t', usecols=['name']) # read names in tracking file
    tracked_names = tracked_names['name'].unique() # read names in tracking file
    tracked_names = list(tracked_names) # read names in tracking file
    # filter them against the names found to get only new names for searching in ENA db
    new_names_all = set(search_result_name_list) - set(tracked_names)
    new_names = set(new_names_all) - set(incomplete_name_list)
    new_names = list(new_names)

    print(len(new_names_all), 'submissions found.')
    print(len(new_names), 'new submissions found that have completed processing.')

    return new_names, incomplete_era


def get_ena_accessions(tracking_file_path, tracking_files_path, xl_sheet2, engine, new_names, incomplete_era):
    # searches for assemblies in ENA database and get accession numbers
    # have status 4 - move to next step
    # have status 2 - 'Still at ENA'

    # get names from processing at ENA sheet - also includes a check if added to tracking file
    print('Checking \'Processing at ENA\' excel tab for any untracked assemblies')
    tracked_names = pd.read_csv(tracking_file_path, sep='\t', usecols=['name'])
    tracked_names = tracked_names['name'].unique()
    tracked_names = list(tracked_names)
    processingat_ENA = xl_sheet2['name']
    processingat_ENA = processingat_ENA.to_list()
    processingat_ENA_new = set(processingat_ENA) - set(tracked_names)
    processingat_ENA_new = list(processingat_ENA_new)
    new_names = processingat_ENA_new + new_names
    new_names = set(new_names)
    print(len(new_names), 'total new names to search in ENA')
    gcs_assembly_result_obj = []

    with engine.connect() as conn:  # opens the connection to database
        print('searching for ENA assembly records in ENAREAD...')

        new_names = sorted(new_names)
        for name in new_names:
            enaname = name
            temp_result = conn.execute(text("""select name,created,accessioned,submitted,project_acc,assembly_id, 
                   biosample_id,gc_id,contig_acc_range,chromosome_acc_range,assembly_type,status_id 
                   from GCS_ASSEMBLY
                   where name = :enaname"""),{"enaname": enaname})
            result = []
            # save the result...
            for row in temp_result:
                rowdict = {"name": row.name , "submission date": row.created, "accessioned": row.accessioned,
                           "shared to NCBI": row.submitted, "project": row.project_acc, "analysis ID": row.assembly_id,
                           "sample ID": row.biosample_id, "GCA ID": row.gc_id, "Contig range": row.contig_acc_range,
                           "Chr range": row.chromosome_acc_range, "Assembly type": row.assembly_type,
                           "status ID": row.status_id}
                result.append(rowdict)
            gcs_assembly_result_obj.extend(result)

    conn.close()
    print('DONE')

    # Filter 3 - remove any assemblies that do not have status 4
    # strip #%H:%M:%S from result
    not_released = None
    gcsassembly_result = pd.DataFrame(gcs_assembly_result_obj)
    if gcsassembly_result.empty:
        pass
    else:
        # datetime conversion
        gcsassembly_result = pd.DataFrame(gcs_assembly_result_obj)
        gcsassembly_result['submission date'] = pd.to_datetime(gcsassembly_result['submission date'],
                                                               format='%Y-%m-%d %H:%M:%S', errors='ignore')
        gcsassembly_result['shared to NCBI'] = pd.to_datetime(gcsassembly_result['shared to NCBI'],
                                                               format='%Y-%m-%d %H:%M:%S', errors='igonre')

        gcsassembly_result['submission date'] = gcsassembly_result['submission date'].dt.strftime("%Y-%m-%d")
        gcsassembly_result['accessioned'] = gcsassembly_result["accessioned"].dt.strftime("%Y-%m-%d")
        gcsassembly_result['shared to NCBI'] = gcsassembly_result["shared to NCBI"].dt.strftime("%Y-%m-%d")
        not_released = gcsassembly_result.loc[gcsassembly_result["status ID"] != 4]
        # save results
        releasing_sequences = gcsassembly_result.loc[gcsassembly_result["status ID"] == 4]
        releasing_sequences = releasing_sequences.sort_values(by=["submission date", "name"])
        releasing_sequences.to_csv(f'{tracking_files_path}/Releasing_sequences.csv', index=False)
        print(len(releasing_sequences), f'''new assemblies with accessions and status 4 found ready to be added to the tracking file listed in: 
        {tracking_files_path}/Releasing_sequences.csv''')

    if (not_released is None) and incomplete_era.empty:
        pass
    elif (not_released is None):
        still_at_ena = incomplete_era
        still_at_ena.to_csv(f'{tracking_files_path}/Still_at_ENA.csv', index=False)
        print(len(still_at_ena), f'''submissions \'still at ENA\' listed in: 
        {tracking_files_path}/Still_at_ENA.csv''')
    elif incomplete_era.empty:
        not_released_append = not_released.iloc[:, [0, 1, 4, 5, 11, 10, 7]]
        still_at_ena = not_released_append
        still_at_ena.to_csv(f'{tracking_files_path}/Still_at_ENA.csv', index=False)
        print(len(still_at_ena), f'''submissions \'still at ENA\' listed in: 
        {tracking_files_path}/Still_at_ENA.csv''')
    else:
        not_released_append = not_released.iloc[:, [0, 1, 4, 5, 11, 10, 7]]
        still_at_ena = pd.concat([incomplete_era, not_released_append])
        still_at_ena.to_csv(f'{tracking_files_path}/Still_at_ENA.csv', index=False)
        print(len(still_at_ena), f'''submissions \'still at ENA\' listed in: 
        {tracking_files_path}/Still_at_ENA.csv''')



def main(opts, tracking_files_path, tracking_file_path, exceldl_path):
    warnings.simplefilter(action='ignore', category=FutureWarning)
    os.chdir(opts.workingdir)
    xl_sheet0 = pd.read_excel(exceldl_path, sheet_name=0, header=1)
    xl_sheet0 = xl_sheet0.drop(['Unnamed: 3'], axis=1)
    # run functions
    engine = get_db_creds('ERA', opts.config)
    new_names, incomplete_era = check_era_for_submission(tracking_file_path, tracking_files_path, xl_sheet0, engine)
    engine = get_db_creds('ENA', opts.config)
    xl_sheet2 = pd.read_excel(exceldl_path, sheet_name=2, header=1)
    if (new_names is None):
        pass
    else:
        get_ena_accessions(tracking_file_path, tracking_files_path, xl_sheet2, engine, new_names, incomplete_era)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="sql processing script argparser")

    parser.add_argument('-p', '--project', help="Project to track DToL, ASG or ERGA", default="none")
    parser.add_argument('-w', '--workingdir', help="location of tracking file folders",
                        default="scripts/assemblytracking/")
    parser.add_argument('-c', '--config', help="config file path", default="config_private.yaml")

    opts = parser.parse_args()

    # set file path strings (needs to be outside of main function)
    tracking_files_path = f'{opts.project}-tracking-files'
    # set the location of the downloaded excel file for reading
    exceldl_path = f'{tracking_files_path}/{opts.project} assembly tracking.xlsx'
    # FILE OUTPUTS SAVE LOCATIONS
    tracking_file_path = f'{tracking_files_path}/tracking_file.txt'

    print('''
    --------------------------------------
      running sql processing ena script
    --------------------------------------
        ''')

    main(opts, tracking_files_path, tracking_file_path, exceldl_path)

