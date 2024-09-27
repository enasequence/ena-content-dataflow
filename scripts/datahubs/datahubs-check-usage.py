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

import os, sys, smtplib, argparse, configparser, pickle, logging
from datetime import datetime
from datetime import timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.message import EmailMessage
import pandas as pd
import numpy as np
import oracledb
from sqlalchemy import create_engine
from sqlalchemy import text
import plotly.express as px
from jinja2 import Template
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)


class Datahub:
    def __init__(self, dhub_name, title, pub_desc, status, coord_contact=[], projects=[], provider_webin=[],
                 latest_data='', run_count=0, latest_run='', analysis_count=0, latest_analysis='',
                 recc_status='', notes=''):
        self.dhub_name = dhub_name
        self.title = title
        self.pub_desc = pub_desc
        self.status = status
        self.coord_contact = coord_contact
        self.projects = projects
        self.provider_webin = provider_webin
        self.latest_data = latest_data
        self.run_count = run_count
        self.latest_run = latest_run
        self.analysis_count = analysis_count
        self.latest_analysis = latest_analysis
        self.recc_status = recc_status
        self.notes = notes

    def __repr__(self):
        description = f"""Object of class Datahub.{self.dhub_name}, {self.notes}"""
        return description


class Project:
    def __init__(self, prj_id, prj_title, hold_date, last_updated, status, datahub, webin_id, project_type, stu_id='',
                 run_count=0, latest_run=None, analysis_count=0, latest_analysis=None, notes=''):
        self.prj_id = prj_id
        self.prj_title = prj_title
        self.hold_date = hold_date
        self.last_updated = last_updated
        self.status = status
        self.datahub = datahub
        self.webin_id = webin_id
        self.project_type = project_type
        self.stu_id = stu_id
        self.run_count = run_count
        self.latest_run = latest_run
        self.analysis_count = analysis_count
        self.latest_analysis = latest_analysis
        self.notes = notes

    def __repr__(self):
        description = f"""Project object. Details...
        prj_id:{self.prj_id}, prj_title:{self.prj_title}, hold_date:{self.hold_date}, status:{self.status}, 
        datahub:{self.datahub}, run_count:{self.run_count}, latest_run:{self.latest_run}, 
        analysis_count:{self.analysis_count}, latest_analysis:{self.latest_analysis}"""
        return description


def fetch_proj_metadata(engine):
    # get project metadata
    proj_list = list()
    with engine.connect() as conn:
        with conn.begin():
            result = conn.execute(text("""select dmk.project_id, prj.PROJECT_TITLE, prj.HOLD_DATE, prj.last_updated, 
            prj.status_id, dmk.META_KEY, prj.submission_account_id, prj.project_type 
            from project prj join dcc_meta_key dmk on dmk.project_id = prj.project_id"""))
            for row in result:
                proj_list.append(Project(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
    for p in proj_list:
            if p.project_type == 'UMBRELLA':
                pass
            else:
                p_to_check = p.prj_id
                with engine.connect() as conn:
                    with conn.begin():
                        result2 = conn.execute(text("""select study_id, project_id from study where project_id = :prj_id"""),
                                       {"prj_id": p_to_check})
                    for row in result2:
                        p.stu_id = row[0]
    return proj_list


def fetch_dhub_metadata(engine, exemption_list):
    # gets basic datahub metadata, and then gatehrs linked Webin IDs and projects for non-exempt datahubs
    dhub_list = list()
    # get basic data hub metadata
    with engine.connect() as conn:
        with conn.begin():
            result1 = conn.execute(
                text("""select cvf.META_KEY, dca.TITLE, dca.DESCRIPTION, dca.ACCOUNT_STATUS
                from cv_fire_meta_key cvf
                join dcc_account dca on cvf.meta_key = dca.meta_key"""))
            for row in result1:
                dhub_list.append(Datahub(row[0], row[1], row[2], row[3]))

    # get additional metadata
    for dhub in dhub_list:
        dhub_to_check = dhub.dhub_name
        with engine.connect() as conn:
            with conn.begin():
                # get data providers webin accounts
                result3 = conn.execute(text("""select submission_account_id, meta_key from dcc_user 
                                               where role = 'PROVIDER' and meta_key = :dhub_name"""),
                                                                            {"dhub_name": dhub_to_check})
                provider_webin_list = list()
                for row in result3:
                    provider_webin_list.append(row[0])
                dhub.provider_webin = provider_webin_list
                 # get coordinator contact
                result2 = conn.execute(text("""select submission_account_id, meta_key, email_address 
                                               from dcc_user 
                                               where role = 'COORDINATOR' and meta_key = :dhub_name"""),
                                                                            {"dhub_name": dhub_to_check})
                for row in result2:
                    dhub.coord_contact = row.email_address
                # get current status from dcc_account
                result2 = conn.execute(text("""select account_status 
                                               from dcc_account where meta_key = :dhub_name"""),
                                                                    {"dhub_name": dhub_to_check})
                for row in result2:
                     dhub.status = row.account_status

    # add linked project info for annotation functions
    # (not checking exempted datahubs due to large data count skewing plot)
    for dhub in dhub_list:
        if dhub.dhub_name not in exemption_list:
            dhub_to_check = dhub.dhub_name
            with engine.connect() as conn:
                with conn.begin():
                    # get linked projects
                    result4 = conn.execute(
                        text("select project_id from dcc_meta_key where meta_key = :dhub_name"),
                        {"dhub_name": dhub_to_check})
                    project_list = list()
                    for row in result4:
                        project_list.append(row.project_id)
                        dhub.projects = project_list
                conn.close()
        else:
            pass
    return dhub_list


def check_runs(proj_list, engine):
    # counts runs and gets latest run for each project in proj_list
    for proj in proj_list:
        s = proj.stu_id
        if not s:
            proj.latest_run = None
            proj.run_count = 0
        else:
            with engine.connect() as conn:
                with conn.begin():
                    count_result = conn.execute(text("""select first_created from run where experiment_id in
                                    (select experiment_id from experiment where study_id in
                                    (select study_id from study where PROJECT_ID = :prj_id))"""),
                                            {"prj_id": proj.prj_id})
                    date_list = []
                    for row in count_result:
                        proj.run_count += 1
                        date_list.append(row[0])
                    if proj.run_count > 0:
                        proj.latest_run = max(date_list)  # .strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        proj.latest_run = None
    return proj_list


def check_analyses(proj_list, engine):
    # counts analyses and gets latest analysis for each project in proj_list
    for proj in proj_list:
        s = proj.stu_id
        if not s:
            proj.latest_analysis = None
            proj.analysis_count = 0
        else:
            with engine.connect() as conn:
                with conn.begin():
                    count_result = conn.execute(text("""select analysis_id, first_created 
                    from analysis where STUDY_ID = :stu_id"""), {"stu_id": proj.stu_id})

                    date_list = []
                    for row in count_result:
                        proj.analysis_count += 1
                        date_list.append(row.first_created)
                    if proj.analysis_count > 0:
                        proj.latest_analysis = max(date_list)  # .strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        proj.latest_analysis = None
    return proj_list


def pickle_outputs(object, filename='.pkl', save_path=''):
    # saves object outputs as pickle object for backing up
    file = save_path + filename
    with open(file, 'wb') as f:
        pickle.dump(object, f)
    print('file output saved at', file)


def save_project_csv(proj_list, save_path):
    # save project object data in csv format
    projects = [{"datahub": proj.datahub,
                 "prj_id": proj.prj_id,
                 "stu_id": proj.stu_id,
                 "prj_type": proj.project_type,
                 "prj_webin": proj.webin_id,
                 "latest_data": proj.latest_data,
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
    projectscsvpath = save_path + 'projects.csv'
    print('project csv saved at', projectscsvpath)
    df.to_csv(projectscsvpath, index=False)
    return projectscsvpath


def save_datahub_csv(dhub_list, save_path):
    # saves datahub object data in csv format
    datahubs = [{"dhub_name": dhub.dhub_name,
                 "title": dhub.title,
                 "description": dhub.pub_desc,
                 "project_count": len(dhub.projects),
                 "latest_data": dhub.latest_data,
                 "run_count": dhub.run_count,
                 "latest_run": dhub.latest_run,
                 "analysis_count": dhub.analysis_count,
                 "latest_analysis": dhub.latest_analysis,
                 "coord_contact": dhub.coord_contact,
                 "recc_state": dhub.recc_status,
                 "reason": dhub.notes
                 }
                for dhub in dhub_list]
    df = pd.DataFrame(datahubs)
    dhubcsvpath = save_path + 'datahubs.csv'
    print('datahub csv saved at', dhubcsvpath)
    df.to_csv(dhubcsvpath, index=False)
    return dhubcsvpath


def make_html_plot(dhubcsvpath, save_path, selected_datatype):
    # fix hover labels using the below stack overflow advice.
    # https://stackoverflow.com/questions/59057881/how-to-customize-hover-template-on-with-what-information-to-show
    df = pd.read_csv(dhubcsvpath)

    df['count scalar'] = np.log(df[selected_datatype] + 1)
    # df['run_count_transform'] = np.log(df[selected_datatype] + 1)

    cust = None
    hover_temp = None
    title = None
    selected_latest = None
    if selected_datatype == 'run_count':
        cust = ['dhub_name', 'run_count'] #, 'latest_run'
        hover_temp = ["Data Hub: %{customdata[0]}", "latest run: %{x}", "Projects: %{y}",
                                                 "Runs: %{customdata[1]}"] #, "Latest run: %{customdata[2]}"
        title = "Data Hubs by Projects, Latest run and Run count"
        selected_latest = 'latest_run'
    elif selected_datatype == 'analysis_count':
        cust = ['dhub_name', 'analysis_count'] #, 'latest_analysis'
        hover_temp = ["Data Hub: %{customdata[0]}", "latest analysis: %{x}", "Projects: %{y}",
                                                 "Analyses: %{customdata[1]}"] #, "Latest analysis: %{customdata[2]}"
        title = "Data Hubs by Projects, Latest analysis and Analysis count"
        selected_latest = 'latest_analysis'

    fig = px.scatter(df, x=df[selected_latest], y=df['project_count'],
                     size=df[selected_datatype] + 1, color=df['count scalar'],
                     hover_name=df["dhub_name"], log_y=True,
                     # size_max=60,
                     title=title,
                     custom_data=cust
                     )
    fig.update_traces(marker=dict(sizemode='area',  # line=dict(width=2, color='DarkSlateGrey'),
                                  sizeref=2 * max(df[selected_datatype]) / (110 ** 2), sizemin=3),
                      hovertemplate="<br>".join(hover_temp)
                      )

    plotpath = save_path + f'datahubs_plot_{selected_datatype}.html'
    print('plot path:', plotpath)
    # fig.write_html(plotpath)
    # following code uses jinja to save full html (reduces html file size for emailing)
    input_template_path = r"template.html"
    plotly_jinja_data = {"fig": fig.to_html(full_html=False)}
    with open(plotpath, "w", encoding="utf-8") as output_file:
        with open(input_template_path) as template_file:
            j2_template = Template(template_file.read())
            output_file.write(j2_template.render(plotly_jinja_data))

    return plotpath


def send_email(email_user, email_password, subject, body, recipients, htmlfile=None):
    # Set up the SMTP server for EBI
    session = smtplib.SMTP('outgoing.ebi.ac.uk', 587)
    session.starttls()
    session.ehlo()

    session.login(email_user, email_password)

    # uses EmailMessage from email library to attach html file
    if htmlfile is not None:
        # Create email message
        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = email_user
        msg['To'] = recipients
        msg.set_content(body)
        with open(htmlfile, 'rb') as f:
            file_data = f.read()
        filepath = htmlfile.split('/')
        msg.add_attachment(file_data, maintype='html', subtype='html', filename=filepath[-1])

        session.send_message(msg)

        session.quit()
    else:  # or just send a plain text message
        # Create the email
        msg = f'Subject: {subject}\n\n{body}'

        # Send the email to multiple recipients
        session.sendmail(email_user, recipients, msg)

        # Disconnect from the server
        session.quit()


def annotate_projects(proj_list):
    # PROJECT
    for proj in proj_list:
        if (proj.run_count == 0 and proj.analysis_count == 0):  # if no data, mark as empty
            proj.notes += 'Empty'

        if proj.status == 4:  # if project is public
            proj.notes += 'Ded-1'
        elif (proj.status == 2 or proj.status == 7):  # if project is due public in future
            # CHECK IF PROJECT WILL BE PUBLIC SOON
            six_m_in_future = datetime.today() + timedelta(days=183)
            hold_date = proj.hold_date
            diff = hold_date - six_m_in_future
            if diff.days < 0:  # if less than 6 months until public
                proj.notes += 'Ded-2'
            elif diff.days > 0 or diff.days == 0:
                pass
            # CHECK FOR RECENT/any DATA
            if proj.run_count > 0:
                m = proj.latest_run
                n = datetime.today() - m
                if n.days > 183:  # has OLD run data
                    proj.notes += 'Ded-3r'
                elif n.days < 183:
                    pass
            if proj.analysis_count > 0:
                o = proj.latest_analysis
                p = datetime.today() - o
                if p.days > 183:  # has OLD analysis data
                    proj.notes += 'Ded-3a'
                elif p.days < 183:
                    pass
        # check for additional project statuses / information
        elif proj.status == 3:
            proj.notes += 'c'
        elif proj.status == 5:
            proj.notes += 's'

        # get latest date of any data for a project!
        if proj.latest_run or proj.latest_analysis is not None:
            proj.latest_data = max([i for i in [proj.latest_run, proj.latest_analysis] if i is not None])
        elif proj.latest_run and proj.latest_analysis is None:
            pass
    return proj_list


def annotate_dhubs_and_report(proj_list, dhub_list, save_path, exemption_list):
    # checks through all datahubs with attached data, assesses based on lifecycle criteria
    # and returns up to date dhub_list and text body report in string format

    # DATAHUB
    empty_dhubs = []
    for dhub in dhub_list:

        proj_count = len(dhub.projects) # count total projects in dhub
        ded_proj_count = 0 # count how many projects are inactive according to criteria
        # collect reasons for dormant datahub
        public_proj_count = 0
        duepublic_proj_count = 0
        old_analysis_projs = 0
        old_run_projs = 0
        empty_projs = 0
        can_proj = 0
        sup_proj = 0
        # get dates of latest data
        run_dates = [datetime(2014, 1, 1)]
        analysis_dates = [datetime(2014, 1, 1)]

        if proj_count == 0:
            dhub.notes += f'no projects | Empty'
        elif proj_count > 0:
            for proj in proj_list:
                if (proj.prj_id in dhub.projects):  # for each project within the datahub, check for features:
                    dhub.run_count += proj.run_count
                    dhub.analysis_count += proj.analysis_count
                    if proj.latest_run is not None:
                            run_dates.append(proj.latest_run)
                    if proj.latest_analysis is not None:
                            analysis_dates.append(proj.latest_analysis)
                    else:
                        pass

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

        # logic to get latest data for datahub
        m = None
        if len(run_dates) > 0:
            m = max(run_dates)
            dhub.latest_run = m
        n = None
        if len(analysis_dates) > 0:
            n = max(analysis_dates)
            dhub.latest_analysis = n
        o = [m, n]
        if len(o) == 2:
            dhub.latest_data = max(o)
        elif len(o) == 1:
            dhub.latest_data = o
        else:
            pass

        if (dhub.dhub_name in exemption_list):
            dhub.recc_status = 'EXEMPT'
        elif (dhub.dhub_name not in exemption_list) and (proj_count == 0) and dhub.run_count == 0 \
                and dhub.analysis_count == 0:
            empty_dhubs.append(dhub)
            dhub.recc_status = 'INACTIVE'
        elif (dhub.dhub_name not in exemption_list) and (proj_count > 0) and dhub.run_count == 0 \
                and dhub.analysis_count == 0:
            if ded_proj_count == proj_count:  # if all project are dormant, mark datahub as dormant
                dhub.recc_status = 'INACTIVE'
            #empty_dhubs.append(dhub)
            dhub.notes += f'no data   |  '
        elif (dhub.dhub_name not in exemption_list) and (proj_count > 0) and (dhub.run_count > 0 \
                or dhub.analysis_count > 0):
            dhub.notes += f'{dhub.run_count} Runs, {dhub.analysis_count} Analyses   |   '
            if ded_proj_count == proj_count:  # if all project are dormant, mark datahub as dormant
                dhub.recc_status = 'INACTIVE'
            # now add informative notes about the datahub
            if proj_count == public_proj_count:
                dhub.notes += f'All {proj_count} projects are public. '
            elif (public_proj_count > 0) and (public_proj_count < proj_count):
                dhub.notes += f'Of {proj_count} total projects: {public_proj_count} public projects,'
            else:
                pass

            if duepublic_proj_count > 0:
                    dhub.notes += f' {duepublic_proj_count} projects due to be public in 6mo,'
            if empty_projs > 0:
                dhub.notes += f' {empty_projs} empty projects,'
            if can_proj > 0:
                dhub.notes += f' {can_proj} cancelled projects,'
            if sup_proj > 0:
                dhub.notes += f' {sup_proj} suppressed projects,'
            #if old_analysis_projs > 0:
            #    dhub.notes += f' {old_analysis_projs} projects with no new analysis data for 6m,'
            #if old_run_projs > 0:
            #    dhub.notes += f' {old_run_projs} projects with no new run data for 6m,'
            # CHECK if data hub has recent data...
            six_m_ago = datetime.today() - timedelta(days=183)
            latest_data = dhub.latest_data
            diff = latest_data - six_m_ago
            if diff.days < 0:  # if latest data more than 6m ago
                dhub.notes += 'has no new data for 6m'
            elif diff.days >= 0:
                dhub.notes += 'has recent data'




    # WRITE EMAIL REPORT BODY TEXT

    total_recc = 0
    for dhub in dhub_list:
        if (dhub.recc_status == 'INACTIVE'):
            total_recc += 1

    body = f"""             ***this is an automated email***

------------------- DATA HUBS USAGE REPORT ----------------------

Out of {len(dhub_list)} total data hubs, {total_recc} are reccommended to be changed to INACTIVE(DORMANT).

Outputs for this script have been saved at:
{save_path}

The SOP for Recycling Data Hubs is here:
https://docs.google.com/document/d/1oTT1el-DMLb778OhStiGDuhp9gQemBXO5kMyrvcrf-4/edit#heading=h.21ijythyvafj

To mark a datahub as dormant, use the portal_dcc_package, documented here: 
https://www.ebi.ac.uk/seqdb/confluence/display/EMBL/Data+Hubs+Documentation

------------------------------------------------------------------
{len(exemption_list)} exempt Data Hubs (not checked):
"""
    for item in exemption_list:
        body += f'{item}, '

    body += f"""\n

{len(empty_dhubs)} empty datahubs:
DATAHUB NAME |  DATA  |   INACTIVE REASON    |  TITLE   
"""
    empty_dhubs = sorted(empty_dhubs, key=lambda dhub: dhub.dhub_name)
    for dhub in empty_dhubs:
        body += f'{dhub.dhub_name} | {dhub.notes}  | {dhub.title}  \n'

    other = total_recc - len(empty_dhubs)
    body += f"""\n


{other} additional Data Hubs:
DATAHUB NAME |  DATA  |   INACTIVE REASON    |  TITLE   
"""
    dhub_list = sorted(dhub_list, key=lambda dhub: dhub.dhub_name)
    for dhub in dhub_list:
        if (len(dhub.projects) > 0) and (dhub.recc_status == 'INACTIVE'):
            body += f'{dhub.dhub_name} | {dhub.notes}  | {dhub.title}  \n'

    body += """

    The script used to generate this report can be found at: 
    https://github.com/enasequence/ena-content-dataflow/tree/master/scripts/datahubs

    Kind Regards,
    Jasmine
    jasmine@ebi.ac.uk"""

    return body, dhub_list, proj_list


def check_for_existing_files(save_path):
    proj_path = save_path + 'proj_list.pkl'
    dhub_path = save_path + 'dhub_list.pkl'
    if os.path.exists(proj_path) and os.path.exists(dhub_path):
        with open(proj_path, 'rb') as f:
            proj_list = pickle.load(f)
        with open(dhub_path, 'rb') as f:
            dhub_list = pickle.load(f)
    else:
        print(f"File '{file_path}' does not exist. Running alternate function...")
    if os.path.exists(dhub_path):
        with open(dhub_path, 'rb') as f:
            dhub_list = pickle.load(f)
    else:
        print(f"File '{file_path}' does not exist. Running alternate function...")

    return proj_list, dhub_list, save_path


def main(opts):
    ## GET ARGUMENTS/INPUT
    config = configparser.ConfigParser()
    config.read(opts.config)
    # get db details
    username = config['ERAREAD_DETAILS']['username']
    password = config['ERAREAD_DETAILS']['password']
    host = config['ERAREAD_DETAILS']['host']
    port = config['ERAREAD_DETAILS']['port']
    servicename = config['ERAREAD_DETAILS']['servicename']
    # get email sender details
    # email_user = os.environ['MYUNAME']
    # email_password = os.environ['MYPW']
    email_user = config['EMAILSENDER_DETAILS']['email_user']
    email_password = config['EMAILSENDER_DETAILS']['email_password']
    recipients = config['RECIPIENTS_DETAILS']['recipients']

    # Provide exemption list of datahubs which will not be retired
    exemption_list = config['DataHubs_exemption']['exemption']
    exemption_list = exemption_list.split(',')
    # create the sqlalchemy engine using database login credentials, and use logging package to prevent
    # too many printed outputs from sqlalchemy package


    engine = create_engine(f"oracle://{username}:{password}@{host}:{port}/?service_name={servicename}",
                           echo=True)

    ###### RUN FUNCTIONS ########
    # create an output path for all the script outputs to be saved
    now = datetime.now()
    save_path = opts.outputdir + "dhub_usage_report-" + now.strftime('%Y%m%d-%H%M') + '/'
    os.mkdir(save_path)

    # get basic metadata about datahubs and their projects
    proj_list = fetch_proj_metadata(engine)
    dhub_list = fetch_dhub_metadata(engine, exemption_list)
    print('metadata check completed')

    # get run and analysis information for project - WARNING - takes a long time
    proj_list = check_runs(proj_list, engine)
    proj_list = check_analyses(proj_list, engine)

    print('run and analysis information gathered')

    # evaluate datahubs
    proj_list = annotate_projects(proj_list)
    body, dhub_list, proj_list = annotate_dhubs_and_report(proj_list, dhub_list, save_path, exemption_list)

    # save evaluation result # pickle - save outputs as class objects
    pickle_outputs(dhub_list, 'dhub_list.pkl', save_path)
    pickle_outputs(proj_list, 'proj_list.pkl', save_path)
    dhubcsvpath = save_datahub_csv(dhub_list, save_path)
    save_project_csv(proj_list, save_path)

    # send email containing the datahubs usage report (including plot attachment)
    plotpath = make_html_plot(dhubcsvpath, save_path, 'run_count')
    send_email(email_user, email_password, 'Datahubs Usage Report', body, recipients, plotpath)
    plotpath = make_html_plot(dhubcsvpath, save_path, 'analysis_count')
    body = f"""             ***this is an automated email***

    -------- DATAHUBS USAGE REPORT - analysis data attached -----------

    """
    send_email(email_user, email_password, 'Datahubs Usage Report - analysis data plot', body, recipients, plotpath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataHubs check usage script")
    parser.add_argument('-c', '--config', help="config file path", default="config.yaml")
    parser.add_argument('-o', '--outputdir', help="location of file outputs", default="")
    opts = parser.parse_args()
    main(opts)