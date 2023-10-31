#!/usr/bin/env python3.7

# Copyright [2020-2023] EMBL-European Bioinformatics Institute
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

import pandas as pd
import numpy as np
import fnmatch #module for unix style pattern matching
import glob #module is used to retrieve files/pathnames matching a specified pattern
from yattag import Doc, indent
import argparse, hashlib, os, subprocess, sys, time


parser = argparse.ArgumentParser(prog='ena-metadata-xml-generator.py', formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog="""
        + =================================================================================================================================== +
        |  European Nucleotide Archive (ENA) Analysis Submission Tool                                                                         |
        |                                                                                                                                     |
        |  Tool to register study and sample metadata to an ENA project, mainly in the drag and drop tool context.                            |
        |example: python3 metadata_xml_generator.py -u Webin-### -p 'password' -f <dir to the spreadsheet> -a <add/modify> -t <for test server|
        + =================================================================================================================================== +
        """)
parser.add_argument('-u', '--username', help='Webin submission account username (e.g. Webin-XXXXX)', type=str, required=True)
parser.add_argument('-p', '--password', help='password for Webin submission account', type=str, required=True)
parser.add_argument('-t', '--test', help='Specify whether to use ENA test server for submission', action='store_true')
parser.add_argument('-f', '--file', help='path for the metadata spreadsheet', type=str, required=True)
parser.add_argument('-a', '--action', help='Specify the type of action needed ( ADD or MODIFY)', type=str, required=True)
args = parser.parse_args()


os.listdir(".") #list files and dirs in wd - make sure you are in the one where the user metadata spreadsheet will be found
files_xlsx = glob.glob(args.file) #should we accept other spreadsheet extensions?


"""
General trimming to the metadata in the spreadsheet and save it in a panda dataframe object
"""
def trimming_the_spreadsheet(df):
    trimmed_df = df.iloc[3: ,].copy()
    trimmed_df.insert(6,"submission_tool",'drag and drop uploader tool',allow_duplicates=True) #study #to inject constant into trimmed df
    trimmed_df.insert(24,"submission_tool",'drag and drop uploader tool',allow_duplicates=True) #sample
    trimmed_df.insert(26,"sample capture status",'active surveillance in response to outbreak',allow_duplicates=False)
    trimmed_df.rename(columns = {'collecting institute':'collecting institution'}, inplace = True) #####temp fix for collecting institute error
    trimmed_df.rename(columns={'collecting institute': 'collecting institution'}, inplace=True)
    trimmed_df["release_date"] = pd.to_datetime(trimmed_df["release_date"], errors='coerce').dt.strftime("%Y-%m-%d")
    trimmed_df["collection date"] = pd.to_datetime(trimmed_df["collection date"], errors='coerce').dt.strftime("%Y-%m-%d")
    trimmed_df["receipt date"] = pd.to_datetime(trimmed_df["receipt date"], errors='coerce').dt.strftime("%Y-%m-%d")
    trimmed_df['collection date'] = trimmed_df['collection date'].fillna('not provided')
    return trimmed_df

"""
Write pandas dataframe object to study xml file
"""
def study_xml_generator(df):
    doc, tag, text = Doc().tagtext()
    xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
    df = df.loc[3: ,'study_alias':'release_date'] # trim the dataframe to the study section only
    df = df.iloc[:, :-1]
    modified_df = df.where(pd.notnull(df), None) # replace the nan with none values
    doc.asis(xml_header)
    with tag('STUDY_SET'):
        for item in modified_df.to_dict('records'):
            if item['study_alias'] != None:
                cleaned_item_dict = {k: v for k, v in item.items() if v not in [None, ' ']} # remove all the none and " " values
                with tag('STUDY', alias=cleaned_item_dict['study_alias']):
                    with tag('DESCRIPTOR'):
                        with tag("STUDY_TITLE"):
                            text(cleaned_item_dict['study_name'])
                        doc.stag('STUDY_TYPE', existing_study_type="Other")
                        with tag('STUDY_ABSTRACT'):
                            text(cleaned_item_dict['abstract'])
                        with tag('CENTER_PROJECT_NAME'):
                            text(cleaned_item_dict['short_description'])
                    with tag('STUDY_ATTRIBUTES'):
                        for header, object in cleaned_item_dict.items():
                            if header not in ['study_alias', 'email_address', 'center_name', 'study_name',
                                              'short_description', 'abstract']:
                                with tag("STUDY_ATTRIBUTE"):
                                    with tag("TAG"):
                                        text(header)
                                    with tag("VALUE"):
                                        text(object)

    result_study = indent(
        doc.getvalue(),
        indent_text=False
    )

    with open("study.xml", "w") as f:
        f.write(result_study)




"""
Write pandas dataframe object to sample xml file
"""
def sample_xml_generator(df):
    doc, tag, text = Doc().tagtext()
    xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
    df = df.loc[3:, 'sample_alias':'experiment_name'] # trim the dataframe to the sample section including the "experiment name" to include any user defined fields
    df = df.iloc[:, :-1] # remove the last column in the trimmed dataframe ( the "experiment name" column)
    modified_df = df.where(pd.notnull(df), None) # replace the nan with none values
    doc.asis(xml_header)
    with tag('SAMPLE_SET'):
        for item in modified_df.to_dict('records'):
            if item['sample_alias'] != None:
                cleaned_item_dict = {k: v for k, v in item.items() if v not in [None, ' ']} # remove all the none and " " values
                if cleaned_item_dict:
                    with tag('SAMPLE', alias=cleaned_item_dict['sample_alias']):
                        with tag('TITLE'):
                            text(cleaned_item_dict['sample_title'])
                        with tag('SAMPLE_NAME'):
                            with tag("TAXON_ID"):
                                text(cleaned_item_dict['tax_id'])
                            with tag("SCIENTIFIC_NAME"):
                                text(cleaned_item_dict['scientific_name'])
                        with tag("DESCRIPTION"):
                            text(cleaned_item_dict['sample_description'])

                        with tag('SAMPLE_ATTRIBUTES'):
                            for header, object in cleaned_item_dict.items():
                                if header not in ['sample_alias', 'sample_title', 'tax_id', 'scientific_name',
                                                  'sample_description']:
                                    with tag("SAMPLE_ATTRIBUTE"):
                                        with tag("TAG"):
                                            text(header)
                                        with tag("VALUE"):
                                            text(object)
                                        if header in ['geographic location (latitude)', 'geographic location (longitude)']:
                                            with tag("UNITS"):
                                                text('DD')
                                        elif header in ['host age']:
                                            with tag("UNITS"):
                                                text('years')



                            with tag("SAMPLE_ATTRIBUTE"):
                                with tag("TAG"):
                                    text("ENA-CHECKLIST")
                                with tag("VALUE"):
                                    text("ERC000033")

    result = indent(
        doc.getvalue(),
        indent_text=False
    )

    with open("sample.xml", "w") as f:
        f.write(result)



"""
Write pandas dataframe object to submission xml file
"""
def submission_xml_generator(df):
    doc, tag, text = Doc().tagtext()
    xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
    doc.asis(xml_header)
    with tag('SUBMISSION_SET'):
        with tag('SUBMISSION'):
            with tag("ACTIONS"):
                with tag('ACTION'):
                    doc.stag(args.action.upper())
                if not df['release_date'].dropna().empty: # in case of multiple studies, it will take the release date of the first study only - make sure all the study release dates are the same
                    with tag('ACTION'):
                        doc.stag('HOLD', HoldUntilDate=str(df.iloc[0]['release_date']))

    result_s = indent(
        doc.getvalue(),
        indentation='    ',
        indent_text=False
    )

    with open("submission.xml", "w") as f:
        f.write(result_s)


"""
the submission command of the output xmls from the spreadsheet
"""
def submission_command(df, args):
    if not df["sample_alias"].dropna().empty and not df["study_accession"].dropna().empty or df["study_alias"].dropna().empty:
        if args.test is True:
            command = 'curl -u {}:{} -F "SUBMISSION=@submission.xml" -F "SAMPLE=@sample.xml"  "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/"'.format(
                args.username, args.password)

        if args.test is False:
            command = 'curl -u {}:{} -F "SUBMISSION=@submission.xml" -F "SAMPLE=@sample.xml"  "https://www.ebi.ac.uk/ena/submit/drop-box/submit/"'.format(
                args.username, args.password)
    elif not df["study_alias"].dropna().empty and df["study_accession"].dropna().empty and df["sample_alias"].dropna().empty:
        if args.test is True:
            command = 'curl -u {}:{} -F "SUBMISSION=@submission.xml" -F "STUDY=@study.xml" "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/"'.format(
                args.username, args.password)

        if args.test is False:
            command = 'curl -u {}:{} -F "SUBMISSION=@submission.xml" -F "STUDY=@study.xml" "https://www.ebi.ac.uk/ena/submit/drop-box/submit/"'.format(
                args.username, args.password)
    else:
        if args.test is True:
            command = 'curl -u {}:{} -F "SUBMISSION=@submission.xml" -F "SAMPLE=@sample.xml" -F "STUDY=@study.xml" "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/"'.format(
                args.username, args.password)

        if args.test is False:
            command = 'curl -u {}:{} -F "SUBMISSION=@submission.xml" -F "SAMPLE=@sample.xml" -F "STUDY=@study.xml" "https://www.ebi.ac.uk/ena/submit/drop-box/submit/"'.format(
                args.username, args.password)

    sp = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = sp.communicate()

    print("-" * 100)
    print("CURL submission command: \n")
    print(command)
    print("Returned output: \n")
    print(out.decode())
    print("-" * 100)


# scanning the metadata spreadsheet
for f in files_xlsx:
    # if the spreadsheet is an assembly spreadsheet
    if fnmatch.fnmatch(f, '*genome*'):
        print('you are using an assembly spreadsheet')
        metadata_df = pd.read_excel(f, usecols="L:AW", header=1, sheet_name='Sheet1') #col range suits v4
        output_df= trimming_the_spreadsheet(metadata_df)
    # if the spreadsheet is a raw read spreadsheet
    elif fnmatch.fnmatch(f, '*raw_reads*'):
        print('you are using a raw reads spreadsheet')
        metadata_df = pd.read_excel(f, usecols="B:AM", header=1, sheet_name='Sheet1') #col range suits v4
        output_df= trimming_the_spreadsheet(metadata_df)
    else:
        print('you have used an unsupported spreadsheet, please try again')


# General trimming of the metadata and sort them in a panda object

    output_trimmed = output_df.drop_duplicates(subset="study_alias")

# Generating the sample and study xml according to the metadata
    if not output_trimmed["study_alias"].dropna().empty and output_trimmed["study_accession"].dropna().empty: #study alias field of trimmed spreadsheet first row
        study_xml_generator(output_trimmed)
        if not output_df['sample_alias'].dropna().empty:
            sample_xml_generator(output_df)
    else:
        if not output_df['sample_alias'].dropna().empty:
            sample_xml_generator(output_df)
        else:
            sys.stderr.write(
                "Attention: The Spreadsheet is empty \n")
            exit(1)


# Generating the submission xml
    submission_xml_generator(output_df)

# running the submission command according to the metadata provided
    submission_command(output_df,args)
