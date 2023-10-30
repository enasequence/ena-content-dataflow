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

import os, smtplib
from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import pickle

import logging

logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)
from datetime import datetime
from datetime import timedelta

# get username and password from config file (for use if running locally using a config.py file):
#import config
#username = config.username
#password = config.password
#passworde = config.passworde



# get username and password, plus email password (passworde) from .bash_profile (export MYUNAME="")
username = os.environ['MYUNAME']
password = os.environ['MYORPW']
passworde = os.environ['MYPW']

# create the sqlalchemy engine using database login credentials
engine = create_engine(f"oracle+oracledb://ops${username}:{password}@ora-era-read-hl.ebi.ac.uk:1531/?service_name=ERAPRO", echo=True)
# add codon path address for saving outputs
save_path = "/hps/nobackup/cochrane/ena/users/jasmine/"

class Datahub:
    def __init__(self, dhub_name, desc, pub_desc, status, parent_webin = [], projects = [], submission_contact = [], latest_hold_date = '', run_count = 0 , latest_run = '', analysis_count = 0, latest_analysis = '', recc_status = '', notes = ''):
        self.dhub_name = dhub_name
        self.desc = desc
        self.pub_desc = pub_desc
        self.status = status
        self.parent_webin = parent_webin
        self.projects = projects
        self.submission_contact = submission_contact
        self.latest_hold_date = latest_hold_date
        self.run_count = run_count
        self.latest_run = latest_run
        self.analysis_count = analysis_count
        self.latest_analysis = latest_analysis
        self.recc_status = recc_status
        self.notes = notes
    def __repr__(self):
        description = f"""Object of class Datahub. Details...
        meta_key:{self.dhub_name}, desc:{self.desc}, pub_desc:{self.pub_desc}, status:{self.status}, projects:{self.projects}
        submission_contact:{self.submission_contact}, latest_hold_date:{self.latest_hold_date}, run_count:{self.run_count},
        latest_run:{self.latest_run}, analysis_count:{self.analysis_count}, latest_analysis:{self.latest_analysis}, recc_status:{self.recc_status}"""
        return description
      
class Project:
    def __init__(self, prj_id, stu_id, prj_title, hold_date, status, datahub, webin_id, run_count = 0 , latest_run = '', analysis_count = 0, latest_analysis = '', notes = ''):
        self.prj_id = prj_id
        self.stu_id = stu_id
        self.prj_title = prj_title
        self.hold_date = hold_date
        self.status = status
        self.datahub = datahub
        self.webin_id = webin_id
        self.run_count = run_count
        self.latest_run = latest_run
        self.analysis_count = analysis_count
        self.latest_analysis = latest_analysis
        self.notes = notes
    def __repr__(self):
        description = f"""Project object. Details...
        prj_id:{self.prj_id}, prj_title:{self.prj_title}, hold_date:{self.hold_date}, status:{self.status}, datahub:{self.datahub}
        run_count:{self.run_count}, latest_run:{self.latest_run}, analysis_count:{self.analysis_count}, latest_analysis:{self.latest_analysis}"""
        return description
    def check_runs(self):
        with engine.connect() as conn:
            with conn.begin():
                count_result = conn.execute(text("""select first_created from run where experiment_id in
                                    (select experiment_id from experiment where study_id in
                                    (select study_id from study where PROJECT_ID = :prj_id))"""), {"prj_id": self.prj_id})
                date_list = []
                for row in count_result:
                    self.run_count += 1
                    date_list.append(row[0])
                if self.run_count > 0:
                    self.latest_run = max(date_list) #.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    self.latest_run = None
    def check_analyses(self):
        with engine.connect() as conn:
            with conn.begin():
                count_result = conn.execute(text("""select analysis_id, first_created from analysis where STUDY_ID = :stu_id"""), {"stu_id": self.stu_id})
                date_list = []
                for row in count_result:
                    self.analysis_count += 1
                    date_list.append(row[1])
                if self.analysis_count > 0:
                    self.latest_analysis = max(date_list) #.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    self.latest_analysis = None
def fetch_data_proj():
    # get project metadata
    with engine.connect() as conn:
        with conn.begin():
            result = conn.execute(text("""select dcc.project_id, stu.study_id, prj.PROJECT_TITLE, prj.HOLD_DATE, prj.status_id, dcc.META_KEY, prj.submission_account_id from
project prj join dcc_meta_key dcc on dcc.project_id = prj.project_id join study stu on stu.project_id = prj.project_id """))
        # conn.commit()
        global proj_list
        proj_list = list()
        for row in result:
            proj_list.append(Project(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
def fetch_data_dhub():
    # get datahub metadata
    with engine.connect() as conn:
        with conn.begin():
            global dhub_list
            dhub_list = list()
            result1 = conn.execute(
                text("select META_KEY, DESCRIPTION, public_description, ACCOUNT_STATUS, submission_account_id from dcc_account"))
            for row in result1:
                dhub_list.append(Datahub(row[0], row[1], row[2], row[3], row[4]))

    # get datahub associated projects
    for dhub in dhub_list:
        dhub_to_check = dhub.dhub_name
        with engine.connect() as conn:
            with conn.begin():
                result2 = conn.execute(
                    text("select project_id, meta_key from dcc_meta_key where meta_key = :dhub_name"),
                    {"dhub_name": dhub_to_check})
                project_list = list()
                for row in result2:
                    project_list.append(row[0])
                dhub.projects = project_list

    # get datahub associated webin accounts
    for dhub in dhub_list:
        dhub_to_check = dhub.dhub_name
        with engine.connect() as conn:
            with conn.begin():
                result2 = conn.execute(text(
                    "select submission_account_id, meta_key from submission_account_dcc where meta_key = :dhub_name"),
                                       {"dhub_name": dhub_to_check})
                webin_list = list()
                for row in result2:
                    webin_list.append(row[0])
                dhub.submission_contact = webin_list
def pickle_outputs(object, filename='', save_path=''):
    # saves object outputs as pickle object for backing up later
    file = save_path + filename
    with open(file, 'wb') as f:
        pickle.dump(object, f)
def save_project_csv(save_path):
    projects = [{"datahub": proj.datahub,
                 "prj_id": proj.prj_id,
                 "prj_webin": proj.webin_id,
                 "hold_date": proj.hold_date,
                 "status_id": proj.status,
                 "run_count": proj.run_count,
                 "latest_run": proj.latest_run,
                 "analysis_count": proj.analysis_count,
                 "latest_analysis": proj.latest_analysis,
                 "prj_title": proj.prj_title,
                 }
                for proj in proj_list]
    df = pd.DataFrame(projects)
    filename = save_path + 'projects.csv'
    df.to_csv(filename, index=False)
  
def send_email(username, password, subject, body, recipients):
    # Set up the SMTP server for EBI
    server = smtplib.SMTP('outgoing.ebi.ac.uk', 587)

    server.ehlo()
    server.starttls()

    server.login(username, password)

    # Create the email
    msg = f'Subject: {subject}\n\n{body}'

    # Send the email to multiple recipients
    server.sendmail(username, recipients, msg)

    # Disconnect from the server
    server.quit()
  
def evaluate_and_report():
    # PROJECT
    for proj in proj_list:
        if (proj.run_count == 0 and proj.analysis_count == 0): # if no data, mark as empty
            proj.notes += 'Empty'

        if proj.status == 4 and (proj.hold_date is None): # if project is public
            proj.notes += 'Ded-1'
        elif (proj.status == 2 or proj.status == 7) and (proj.hold_date is not None): # if project is due public in future
            # CHECK IF PROJECT WILL BE PUBLIC SOON
            futuresix = datetime.today() + timedelta(days=183)
            k = proj.hold_date
            l = k - futuresix
            if l.days < 0: # less than 6 months until public
                proj.notes += 'Ded-2'
            elif l.days > 0 or l.days == 0:
                pass
            # CHECK FOR RECENT/any DATA
            if proj.run_count > 0:
                m = proj.latest_run
                n = datetime.today() - m
                if n.days > 183: # has OLD run data
                    proj.notes += 'Ded-3r'
                elif n.days < 183:
                    pass
            if proj.analysis_count > 0:
                o = proj.latest_analysis
                p = datetime.today() - o
                if p.days > 183: # has OLD analysis data
                    proj.notes += 'Ded-3a'
                elif p.days < 183:
                    pass
        # check for additional project statuses / information
        elif proj.status == 3:
            proj.notes += 'c'
        elif proj.status == 5:
                proj.notes += 's'

    # DATAHUB
    empty_dhubs = []
    for dhub in dhub_list:

        proj_count = 0
        ded_proj_count = 0
        # collect reasons for dormant datahub
        public_proj_count = 0
        duepublic_proj_count = 0
        old_analysis_projs = 0
        old_run_projs = 0
        empty_projs = 0
        can_proj = 0
        sup_proj = 0

        for project in dhub.projects:
            for proj in proj_list:
                if (proj.prj_id == project): # for each project within the datahub, check for features
                    proj_count += 1
                    dhub.run_count += proj.run_count
                    dhub.analysis_count += proj.analysis_count
                    if 'Ded' in proj.notes:
                        ded_proj_count += 1
                    if '1' in proj.notes:
                        public_proj_count += 1
                    if '2' in proj.notes:
                        duepublic_proj_count += 1
                    if '3a' in proj.notes:
                        old_analysis_projs += 1
                    if '3r' in proj.notes:
                        old_run_projs += 1
                    if 'Empty' in proj.notes:
                        empty_projs += 1
                    if ('c' in proj.notes):
                        can_proj += 1
                    if ('s' in proj.notes):
                        sup_proj += 1
                    else:
                        pass

        if len(dhub.projects) == 0:
            empty_dhubs.append(dhub.dhub_name)
            dhub.notes += 'Empty'
            dhub.recc_status = 'DORMANT'
        elif len(dhub.projects) > 0:
            dhub.notes += f'{dhub.run_count} Runs, {dhub.analysis_count} Analyses   |   '
            if ded_proj_count == public_proj_count: # if all project are dormant, mark datahub as dormant
                dhub.recc_status = 'DORMANT'
            # now add informative notes about the datahub
            if proj_count == public_proj_count:
                dhub.notes += f'All {proj_count} projects are public.'
            else:
                dhub.notes += f'Of {proj_count} total projects:'
                if (public_proj_count > 0) and (public_proj_count < proj_count):
                    dhub.notes += f' {public_proj_count} public projects,'
                if duepublic_proj_count > 0:
                    dhub.notes += f' {duepublic_proj_count} projects due to be public in 6mo,'

            if empty_projs > 0:
                dhub.notes += f' {empty_projs} empty projects,'
            if can_proj > 0:
                dhub.notes += f' {can_proj} cancelled projects,'
            if sup_proj > 0:
                dhub.notes += f' {sup_proj} suppressed projects,'
            if old_analysis_projs > 0:
                dhub.notes += f' {old_analysis_projs} projects with analysis data older than 6mo,'
            if old_run_projs > 0:
                dhub.notes += f' {old_run_projs} projects wth run data older than 6mo,'
            else:
                pass



    # SEND EMAIL REPORT
    recipients ='jasmine@ebi.ac.uk'

    total_recc = 0
    for dhub in dhub_list:
        if (dhub.recc_status == 'DORMANT'):
            total_recc += 1
    body = f"""             ***this is an automated email***
    
------------------- DATAHUBS USAGE REPORT ----------------------

Out of {len(dhub_list)} total datahubs, {total_recc} are reccommended to be changed to DORMANT.

Outputs for this script have been saved at /hps/nobackup/cochrane/ena/users/jasmine/ 
dhub_list.pkl
projects.csv
These can be uploaded to google colab to see the Datahubs Usage Report:
https://colab.research.google.com/github/jas-mckin/datahubs-dormancy/blob/main/datahubs_dormancy_report.ipynb

A document with Dormancy notification email templates is here:
https://docs.google.com/document/d/1RbLBsrXJh7FPshEt9gkRLIsW6rB0kcxXUcPwC0lS3XE/edit#heading=h.afx446nhk2x4

To mark a datahub as dormant, use the portal_dcc_package, documented here: 
https://www.ebi.ac.uk/seqdb/confluence/display/DCP/Datahubs

------------------------------------------------------------------
{len(empty_dhubs)} empty datahubs (no projects associated):
"""
    for item in empty_dhubs:
        body += f'{item},'

    body += """\n

Remaining datahubs:
DATAHUB NAME    |  DESCRIPTION    |  DATA  |   DORMANCY REASON
"""
    for dhub in dhub_list:
        if (len(dhub.projects) > 0) and (dhub.recc_status =='DORMANT'):
            body += f'{dhub.dhub_name}  | {dhub.desc}  | {dhub.notes}\n'

    body += '\n\nThe scripts for this report can be found at: https://github.com/jas-mckin/datahubs-dormancy\n\nKind Regards,\nJasmine McKinnon\njasmine@ebi.ac.uk'

    send_email(username, passworde, 'Datahubs Usage Report', body, recipients)



###### RUN FUNCTIONS ########
fetch_data_proj()
fetch_data_dhub()
# pickle - save outputs as class objects
pickle_outputs(dhub_list, 'dhub_list.pkl', save_path) 
pickle_outputs(proj_list, 'proj_list.pkl', save_path)
send_email(username, passworde, 'dhub_dhpr_notif', 'dhub and proj data gathered, saved', recipients='jasmine@ebi.ac.uk')

# get run information for project - WARNING - takes a long time
for proj in proj_list:
    try:
        Project.check_runs(proj)
    except:
        print('exception: failure')

# get analysis information for project - WARNING - takes a long time
for proj in proj_list:
    try:
        Project.check_analyses(proj)
    except:
        print('exception: failure')

# save project output info
save_project_csv(save_path)
send_email(username, passworde, 'dhub_ra_notif', 'datahubs linked data check completed, saved', recipients='jasmine@ebi.ac.uk')

with open(save_path +'proj_list.pkl', 'rb') as f:
    proj_list = pickle.load(f)


evaluate_and_report()

# save evaluation result
pickle_outputs(dhub_list, 'dhub_list.pkl', save_path)  
pickle_outputs(proj_list, 'proj_list.pkl', save_path)


