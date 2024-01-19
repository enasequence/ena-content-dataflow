#!/usr/bin/python3

# Copyright [2023] EMBL-European Bioinformatics Institute
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

import smtplib
import os,sys
import requests
import os.path
import pandas as pd
from datetime import datetime
import dateutil.parser
# google libraries
import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# BEFORE RUNNING SCRIPT
# 1) Install the packages using conda or pip
# 2) set the project ID to the latest VEO project ID
project = 'TODO:add project ID'
# 3) choose a path which will be the working directory for saving the sent_record and saving/reading API results
path='/path/to/working_dir/'
# 4) Use local bash variables for sending emails using EBI smtp server (add to .bash_profile)
username = os.environ.get('MYEMAIL')
password = os.environ.get('MYPW')
# 5) add list of email addresses to recieve the updates
recipients = ['TODO:add recipients email list']
# 6) also set up local google credentials (credentials.josn and token.json) for uploading to a google drive folder using OAuth 
# https://developers.google.com/workspace/guides/create-credentials#oauth-client-id

# START OF SCRIPT
def get_analyses():
    # uses the ENA advanced search to get analyses from the specified project with the specified data fields
    url = "https://www.ebi.ac.uk/ena/portal/api/search"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    payload = {'result': 'analysis', 'query': f'study_accession%3D%22{project}%22', 'fields': [
        'analysis_accession%2Canalysis_alias%2Crun_accession%2Ccollection_date%2Cfirst_created%2Cfirst_public%2Csubmitted_ftp%2Csubmitted_md5'],
               'limit': '0', 'format': 'tsv'}

    r = requests.post(url, data=payload, headers=headers)
    results = r.text
    # save tsv and then read output as pandas dataframe
    with open('analysis_input.tsv', 'w') as output:
        output.write(results)
    global analysis
    analysis = pd.read_csv('analysis_input.tsv', sep='\t')
    
    # include monitoring for project (projects have an object limit of 500k)
    how_many_total = len(analysis)
    body = f"""There are {how_many_total} analyses found in the project {project}.

    If its more than 500k - you need to make new project ids. 
    https://docs.google.com/document/d/1txSZdGYOHDFb--IbSE1W9cIPSieXgM3Ajg5spuylUk4/edit
        """
    send_email(username, password, 'CV19 - Project total check', body, recipients ='jasmine@ebi.ac.uk')

def check_new_analyses_email():
    # check analyses against sent data and decide whether to proceed - sent_record.tsv saved on codon cluster at path
    sent = pd.read_csv('sent_record.tsv', sep='\t')
    # uses the set object to make a camparison
    new_analyses_list = set(analysis['analysis_accession']) - set(sent['analysis_accession'])

    global how_many_new #how_many_new is global variable and if this is >0 then a report is generated to share new data.
    how_many_new = len(new_analyses_list)

    body = f"""Dear all,
**this is an auto-generated email**

There are {how_many_new} new analyses found in the project {project} using the ENA advanced search API.

If there is new analysis data, an update with the FTP links will follow shortly.

The script used to check and send the latest VEO archived data can be found at 
https://github.com/enasequence/ena-content-dataflow/blob/master/scripts/veo_cv19analyses_share.py

The VEO projects are available to view at: https://www.ebi.ac.uk/ena/browser/view/PRJEB45555

Best wishes,
Jasmine

        """

    send_email(username, password, 'SARS-CoV-2 data update check', body, recipients)


def add_runs():
    # query will get all runs with cv19 tax id.
    url = "https://www.ebi.ac.uk/ena/portal/api/search"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {'result': 'read_run', 'query': 'tax_eq(2697049)', 'fields': [
        'run_accession%2Cinstrument_platform%2Cinstrument_model%2Ccountry'],
               'limit': '0', 'format': 'tsv'}

    r = requests.post(url, data=payload, headers=headers)
    results = r.text
    with open('all_rundata.tsv', 'w') as output:
        output.write(results)

    # read api output as pandas dataframe
    run = pd.read_csv('all_rundata.tsv', sep='\t')

    # filter runs - filter the run output so it is only included for runs cited from current project analyses by using inner join
    df_all = run.merge(analysis, left_on=['run_accession'], right_on=['run_accession'],
                       how='inner', indicator=False)
    global merged
    merged = df_all[
        ['run_accession', 'analysis_accession', 'instrument_platform', 'instrument_model', 'country', 'analysis_alias',
         'first_created', 'first_public','collection_date', 'submitted_ftp', 'submitted_md5']]

def get_new():
    # compares latest analysis pull with runs/analyses in sent_record
    sent = pd.read_csv('sent_record.tsv', sep='\t')

    global raw_data

    #compares latest data (merged) with sent data by run accession to get new runs list using set python object
    new_runs_list = set(merged['run_accession']) - set(sent['run_accession'])
    #subsets the merged dataframe to only include new data
    mask = merged['run_accession'].isin(new_runs_list)
    raw_data = merged[mask]

    raw_data = raw_data[
        ['run_accession', 'analysis_accession', 'instrument_platform', 'instrument_model', 'country', 'analysis_alias',
         'first_created', 'first_public', 'collection_date','submitted_ftp', 'submitted_md5']]


def get_stats_write_email():
    global run_count
    run_count = str(len(raw_data))

    global latest_analysis
    #get the date of analysis from the alias instead of from first created (sense checking)
    #raw_data['analysis_alias'] = raw_data['analysis_alias'].replace(regex=r'.*._.RR.*_', value='')
    #raw_data['analysis_alias'] = raw_data['analysis_alias'].replace(regex=r'T', value=':')
    #raw_data['analysis_alias'] = raw_data['analysis_alias'].replace(regex=r'-', value=':')
    #raw_data['analysis_alias'] = raw_data['analysis_alias'].apply(lambda x: datetime.strptime(x, '%Y:%m:%d:%H:%M:%S'))

    # use first created to check date
    raw_data['first_created_date'] = raw_data['first_created'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    latest_analysis = raw_data['first_created_date'].max()
    latest_analysis = latest_analysis.strftime('%Y-%m-%d')

    global latest_collection
    # do the same for collection dates (different method bc collection_date field is not consistent)
    dates = raw_data.collection_date.values.tolist()
    realdates = []
    for date in range(0, len(dates)):
        k = str(dates[date])
        if k != 'nan':
            try:
                timeobject = datetime.strptime(k,'%Y-%m-%d')
                realdates.append(timeobject)
            except:
                pass
            try:
                timeobject = datetime.strptime(k, '%Y')
                realdates.append(timeobject)
            except:
                pass
            try:
                timeobject = dateutil.parser.parse(k)
                realdates.append(timeobject)
            except:
                pass
    latest_collection = max(realdates).strftime('%Y-%m-%d')
    global contents
    contents = f"""Dear Krisztian and Jozsi,
**this is an auto-generated email**

Here is a google drive link <https://drive.google.com/drive/folders/1zoowqmi1wuBOib6iE4tKBylJnwo6Q2PJ?usp=sharing> for the latest update of archived covid analysis data for you to download.

The latest collection date in this dataset is {latest_collection}, and it includes analysis of older data collected in 2021. The latest date an analysis was created was on {latest_analysis}.

There is a total of {run_count} runs in this dataset.

Best wishes,
Jasmine

-- 
Jasmine McKinnon
User-Support Bioinformatician
ENA - European Nucleotide Archive
European Bioinformatics Institute (EMBL-EBI) 

    """

def make_spreadsheet(send_file):
    
    elte_out = raw_data[
        ['run_accession', 'analysis_accession', 'instrument_platform', 'instrument_model', 'country', 'collection_date',
         'submitted_ftp', 'submitted_md5']]
    elte_out = elte_out.rename(columns={'instrument_platform': 'platform','instrument_model': 'MODEL' })

    elte_out.to_csv(send_file, sep='\t',index=False)


def save_sent_accs():
    sent = pd.read_csv('sent_record.tsv', sep='\t')
    new = pd.read_csv(f'latest_acc_ELTE_{day}{mon}.tsv', sep='\t')
    new_accs = new[['analysis_accession', 'run_accession']]
    sent_updated = pd.concat([sent, new_accs], axis=0)
    sent_updated.to_csv('sent_record.tsv', sep='\t', index=False)


# upload finished data table to google drive
# needs: token and credentials json, dat, month
def upload_to_folder(folder_id):
    SCOPES = ['https://www.googleapis.com/auth/drive']
    """Upload a file to the specified folder and prints file ID, folder ID
    Args: Id of the folder
    Returns: ID of the file uploaded

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'google_credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)

        file_metadata = {'name': send_file, 'parents': [folder_id]}
        media = MediaFileUpload(send_file,
                                mimetype='text/tab-separated-values')
        # pylint: disable=maybe-no-member
        file = service.files().create(body=file_metadata, media_body=media,
                                      fields='id').execute()
        print(F'File ID: "{file.get("id")}".')
        return file.get('id')

    except HttpError as error:
        print(F'An error occurred: {error}')
        return None


def send_email(username, password, subject, body, recipients):
    # Set up the SMTP server for EBI
    server = smtplib.SMTP('outgoing.ebi.ac.uk', 587)

    server.ehlo()
    server.starttls()

    server.login(username, password)

    # Create the email
    msg = f'From: {username}\nSubject: {subject}\n\n{body}'

    # Send the email to multiple recipients
    server.sendmail(username, recipients, msg)

    # Disconnect from the server
    server.quit()


if __name__ == '__main__':
    os.chdir(path)
    get_analyses()
    check_new_analyses_email()
    if how_many_new > 0:
        add_runs()
        get_new()
        del analysis
        del merged
        get_stats_write_email()
        day = datetime.now().strftime('%d')
        mon = datetime.now().strftime('%b')
        send_file = f'latest_acc_ELTE_{day}{mon}.tsv'
        make_spreadsheet(send_file) #save the spreadsheet
        # upload spreadsheet update to google drive using google credentials
        upload_to_folder(folder_id = '1zoowqmi1wuBOib6iE4tKBylJnwo6Q2PJ')
        # send email
        send_email(username, password, 'latest data update for SARS-CoV-2', contents, recipients)
        save_sent_accs()
        os.remove('all_rundata.tsv')
        os.remove('analysis_input.tsv')
    else:
        sys.exit(0)
