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

import sys, re, argparse
import pandas as pd

gisaid_to_ena = {
    # sample metadata
    'covv_collection_date': 'collection date',
    'covv_location': 'geographic location (country and/or sea)',
    'covv_add_location': 'geographic location (region and locality)',
    'covv_host': 'host common name',
    'covv_gender': 'host sex',
    'covv_authors': 'collector name',
    'covv_orig_lab': 'collecting institute',
    'covv_outbreak': 'sample capture status',
    'covv_patient_status': 'host disease outcome',
    'covv_patient_age': 'host age',
    'covv_subm_sample_id': 'virus identifier',
    'covv_specimen': 'isolation source host-associated',
    # experiment metadata
    'covv_seq_technology': 'sequencing_platform',
    'covv_assembly_method': 'library_construction protocol'
}

mandatory_ena_fields = [
    'taxon_id',
    'geographic location (country and/or sea)',
    'host common name',
    'host subject id',
    'host health state',
    'host sex',
    'host scientific name',
    'collector name',
    'collecting institution',
    'isolate'
]
default_sheet = 'ENA Submission'

def parse_args(args):
    parser = argparse.ArgumentParser(
        description="""
        Script to convert GISAID metadata spreadsheets into ENA formatted ones.
        Can handle CSV or any format handled by the pandas.read_excel method (https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html)
        Output format will be decided based on the file extension provided by the user (.csv/.xls)

        Examples:
        # convert GISAID spreadsheet in CSV format to ENA in excel format
        gisaid_to_ena.py --csv gisaid.csv --out ena.xls
        # convert GISAID metadata from sheet called 'Samples' to ENA CSV
        gisaid_to_ena.py --xls gisaid.xlsx --sheet Samples --out ena.csv
        """
    )
    parser.add_argument('--csv', help="path to CSV file")
    parser.add_argument('--xls', help="path to excel file")
    parser.add_argument('--sheet', help="(optional) name of excel sheet (default: first sheet)")
    parser.add_argument('--out', help="output file name (suffix determines format)")
    opts = parser.parse_args(sys.argv[1:])
    return opts

"""
Read GISAID spreadsheet into pandas dataframe
"""
def parse_gisaid_metadata(opts):
    gisaid_df = ''
    if opts.csv:
        gisaid_df = pd.read_csv(opts.csv)
    elif opts.xls:
        # 'Submissions' is the name of the sheet in the GISAID template
        sheet = opts.sheet if opts.sheet else 'Submissions'
        gisaid_df = pd.read_excel(opts.xls, sheet_name=sheet)
    else:
        sys.stderr.write("Must provide either --csv or --xls")
        sys.exit(1)

    return gisaid_df

"""
Convert the metadata fields using the GISAID->ENA mapping
"""
def convert_gisaid_to_ena(gisaid_df):
    ena_data = {}
    for gisaid_field in gisaid_df:
        try:
            ena_field = gisaid_to_ena[gisaid_field]
            ena_data[ena_field] = list(gisaid_df[gisaid_field])
        except KeyError:
            continue

    # make sure all mandatory fields are in the spreadsheet
    num_rows = len(ena_data['collection date'])
    for m_field in mandatory_ena_fields:
        if m_field not in ena_data:
            ena_data[m_field] = list(' ' * num_rows)

    return pd.DataFrame(ena_data)

def highlight_mandatory_fields(writer, headers):
    workbook  = writer.book
    worksheet = writer.sheets[default_sheet]
    fmt_orange = workbook.add_format({'bold':True, 'font_color': 'orange'})
    fmt_black = workbook.add_format({'bold':True, 'font_color': 'black'})
    for i in range(len(headers)):
        if i < len(mandatory_ena_fields):
            worksheet.write(0, i, headers[i], fmt_orange)
        else:
            worksheet.write(0, i, headers[i], fmt_black)

    return writer

def write_dataframe(df, outfile):
    # order headers - mandatory ones first
    headers = mandatory_ena_fields.copy()
    for h in df:
        if h not in headers:
            headers.append(h)

    out_suffix = outfile.split('.')[-1]
    if out_suffix == 'xls' or out_suffix == 'xlsx':
        writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
        df.to_excel(writer, index=False, columns=headers, sheet_name=default_sheet)
        writer = highlight_mandatory_fields(writer, headers)
        writer.save()
    elif out_suffix == 'csv':
        df.to_csv(outfile, index=False, columns=headers)
    else:
        sys.stderr.write(f"Unable to determine format {out_suffix}. Please use .xls, .xlsx or .csv")

#------------------------#
#          MAIN          #
#------------------------#
if __name__ == "__main__":
    opts = parse_args(sys.argv[1:])
    gisaid_dataframe = parse_gisaid_metadata(opts)
    ena_dataframe = convert_gisaid_to_ena(gisaid_dataframe)
    write_dataframe(ena_dataframe, opts.out)
