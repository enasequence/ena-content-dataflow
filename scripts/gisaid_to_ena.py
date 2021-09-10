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

import sys, re, argparse, requests
import pandas as pd
import pycountry

from yattag import Doc, indent
doc, tag, text = Doc().tagtext()
xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
doc.asis(xml_header)
ena_fields = [
    'sample_alias*', 'tax_id*', 'scientific_name*', 'common_name',
    'sample_title*', 'sample_description*', 'collection date*',
    'geographic location (country and/or sea)*', 'geographic location (region and locality)',
    'sample capture status*', 'host common name*', 'host subject id*', 'host age',
    'host health state*', 'host sex*', 'host scientific name*', 'virus identifier',
    'collector name*', 'collecting institution*', 'isolate*', 'isolation source host-associated',
    'gisaid_accession_id'
]

gisaid_to_ena = {
    # sample metadata
    'covv_collection_date': 'collection date*',
    'covv_location': 'geographic location (country and/or sea)*',
    'covv_add_location': 'geographic location (region and locality)',
    'covv_virus_name': 'sample_alias*',
    'covv_host': 'host common name*',
    'covv_gender': 'host sex*',
    'covv_authors': 'collector name*',
    'covv_orig_lab': 'collecting institution*',
    # 'covv_outbreak': 'sample capture status*',
    'covv_patient_status': 'host health state*',
    'covv_patient_age': 'host age',
    'covv_subm_sample_id': 'sample_title*',
    'covv_specimen': 'isolation source host-associated',
    # experiment metadata
    # 'covv_seq_technology': 'sequencing_platform',
    # 'covv_assembly_method': 'library_construction protocol'
}
default_sheet = 'ENA Submission'


"""
Handle command line arguments & helptext
"""
def parse_args(args):
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(
        description="""

description:
  Script to convert GISAID metadata spreadsheets into ENA formatted ones.
  Can handle CSV or any format handled by the pandas.read_excel method
  (https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html)

  Output format will be in Microsoft Excel format (.xlsx)

examples:
  # convert GISAID spreadsheet in CSV format to ENA in excel format
  gisaid_to_ena.py --csv gisaid.csv --outfile ena.xlsx --outformat excel
  # convert GISAID metadata from sheet called 'Samples' to ENA spreadsheet
  gisaid_to_ena.py --xls gisaid.xlsx --sheet Samples --outfile ena.xml --outformat xml
        """,
        formatter_class=RawTextHelpFormatter
    )
    parser.add_argument('--csv', help="path to CSV file")
    parser.add_argument('--xls', help="path to excel file")
    parser.add_argument('--sheet', help=f"(optional) name of excel sheet (default: 'Submissions')")
    parser.add_argument('--outfile', help="output file name")
    parser.add_argument('--taxon', help="taxon name or id of samples")
    parser.add_argument('--outformat', help='Specify between xml or excel', type=str, required=True)
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
        sys.stderr.write("Must provide either --csv or --xls\n\n")
        sys.exit(1)

    # gisaid sheets have 2 headers - drop second one
    gisaid_df = gisaid_df.drop([0])

    return gisaid_df

"""
Convert the metadata fields using the GISAID->ENA mapping
"""
def convert_gisaid_to_ena(gisaid_df):
    ena_data = {}

    # is additional location info provided or should we infer some from `covv_location`?
    infer_add_loc = True if pd.isnull(gisaid_df.at[1, 'covv_add_location']) else False

    for gisaid_field in gisaid_df:
        if gisaid_field == 'covv_add_location' and infer_add_loc:
            continue

        try:
            ena_field = gisaid_to_ena[gisaid_field]
            if gisaid_field == 'covv_location':
                geo_info = [ extract_geographic_info(gl) for gl in list(gisaid_df[gisaid_field]) ]
                ena_data['geographic location (country and/or sea)*']  = [g[0] for g in geo_info]
                if infer_add_loc:
                    ena_data['geographic location (region and locality)'] = [g[1] for g in geo_info]
            else:
                ena_data[ena_field] = list(gisaid_df[gisaid_field])
        except KeyError:
            continue

    ena_data = smart_fill(ena_data, gisaid_df)
    return pd.DataFrame(ena_data)


"""
Autofill as much stuff as possible, replace bad
values, general tidy up of data.
"""
def smart_fill(ena_data,gisaid_df):
    # need num of rows to autofill missing data
    num_rows = len(ena_data['collection date*'])

    # add taxon info if given

    ena_data = add_taxonomic_information(ena_data,gisaid_df)

    # add standard capture status
    ena_data['sample capture status*'] = ['active surveillance in response to outbreak' for i in range(num_rows)]

    # make sure all other fields are in the spreadsheet
    for field in ena_fields:
        if field not in ena_data:
            ena_data[field] = [' ' for i in range(num_rows)]

    ena_data = fix_missing_values(ena_data, num_rows)

    return ena_data

"""
Fill 'not provided' in place of 'unknown', except for
'host age' ('not provided' is not accepted)
"""
def fix_missing_values(dataframe, num_rows):
    # if host age is empty, remove it
    # it only causes issues with webin interactive later
    if _check_empty_list(dataframe['host age']):
        del dataframe['host age']
        ena_fields.remove('host age')

    # otherwise, autofill empty/unknowns
    for field in dataframe:
        if 'unknown' in dataframe[field] or ' ' in dataframe[field]:
            # don't autofill sample_title
            if field != 'sample_title*':
                if field[-1] == '*':
                    dataframe[field] = ['not provided' if v in ['unknown', ' '] else v for v in dataframe[field]]
                else:
                    dataframe[field] = ['not provided' if v == 'unknown' else v for v in dataframe[field]]

    return dataframe


def _check_empty_list(list):
    list_empty = True
    list = [' ' if l == 'unknown' else l for l in list]
    for elem in list:
        if re.search("^\S+$", elem):
            list_empty = False
            break
    return list_empty

"""
Split out geographic info from GISAID location string
"""
def extract_geographic_info(location_str):
    loc_parts = [l.strip() for l in location_str.split('/')]
    for i in range(len(loc_parts)):
        loc = loc_parts[i]
        if pycountry.countries.get(name=loc):
            return loc, ", ".join(loc_parts[i+1:])

    # return input string if country can't be found
    return location_str, None

#---------------------------------------#
#           taxonomy methods            #
#---------------------------------------#
"""
Add extra taxonomic information to a given dataframe
"""
def add_taxonomic_information(dataframe,gisaid_df):
    num_rows = len(dataframe['collection date*'])
    if opts.taxon:
        this_taxon_id = taxon_id(opts.taxon)
        dataframe['tax_id*'] = [this_taxon_id for i in range(num_rows)]

        this_sci_name = scientific_name(opts.taxon)
        dataframe['scientific_name*'] = [this_sci_name for i in range(num_rows)]
    # in case betacoronavirus is indicated in the spreadsheet and the --taxon is not specified
    elif opts.taxon == None:
        dataframe['tax_id*'] =[]
        dataframe['scientific_name*']=[]
        for tax in gisaid_df['covv_type']:
            if tax == 'betacoronavirus':
                this_taxon_id= taxon_id('2697049')
                this_sci_name = scientific_name('2697049')

                dataframe['tax_id*'].append (this_taxon_id)
                dataframe['scientific_name*'].append(this_sci_name)
            else:
                dataframe['tax_id*'].append(' ')
                dataframe['scientific_name*'].append(' ')


    if dataframe['host common name*']:
        dataframe['host scientific name*'] = [scientific_name(x) for x in dataframe['host common name*']]








    return dataframe


"""
Return id from taxonomy
"""
def taxon_id(taxon_name_or_id):
    return taxonomy(taxon_name_or_id)['id']


"""
Return scientific name from taxonomy
"""
def scientific_name(taxon_name_or_id):
    return taxonomy(taxon_name_or_id)['scientific_name']


"""
Query EnsEMBL taxonomy REST endpoint
"""
def taxonomy(id_or_name):
    endpoint = f"http://rest.ensembl.org/taxonomy/id/{id_or_name}?"
    r = requests.get(endpoint, headers={ "Content-Type" : "application/json"})

    if not r.ok:
        r.raise_for_status()
        sys.exit()

    decoded = r.json()
    return decoded


#---------------------------------------#
#         spreadsheet methods           #
#---------------------------------------#
"""
Misc formatting of the spreadsheet
"""
def format_sheet(writer, headers):
    workbook  = writer.book
    worksheet = writer.sheets[default_sheet]
    fmt_orange = workbook.add_format({'bold':True, 'font_color': 'orange'})
    fmt_black = workbook.add_format({'bold':True, 'font_color': 'black'})

    # first, add and format the essential header rows
    worksheet.write(0, 0, '#Checklist', fmt_orange)
    #worksheet.write(1, 0, '#unique_name_prefix',  fmt_orange)
    worksheet.write(0, 1, 'ERC000033', fmt_black)
    worksheet.write(0, 2, 'ENA virus pathogen reporting standard checklist', fmt_black)
    worksheet.write(2, 0, '#units', fmt_orange)

    # second, add headers and highlight mandatory ones
    for i in range(len(headers)):
        if headers[i][-1] == '*':
            worksheet.write(1, i, headers[i][:-1], fmt_orange)
        else:
            worksheet.write(1, i, headers[i], fmt_black)

    return writer


"""
Write pandas dataframe object to excel spreadsheet
"""
def write_dataframe(df, outfile):
    out_suffix = outfile.split('.')[-1]
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    print(writer)
    df.to_excel(writer, index=False, columns=ena_fields, header=False, sheet_name=default_sheet, startrow=3, startcol=0)
    writer = format_sheet(writer, ena_fields)
    writer.save()



"""
Write pandas dataframe object to xml file
"""
def xml_generator (dataframe):
    modified_ena_df = dataframe.where(pd.notnull(dataframe), None)
    with tag('SAMPLE_SET'):
        for item in modified_ena_df.to_dict('records'):
            cleaned_item_dict = {k: v for k, v in item.items() if v not in [None, ' ']}
            with tag('SAMPLE', alias=cleaned_item_dict['sample_alias*']):
                with tag('TITLE'):
                    text(cleaned_item_dict['sample_title*'])
                with tag('SAMPLE_NAME'):
                    with tag("TAXON_ID"):
                        text(cleaned_item_dict['tax_id*'])
                    with tag("SCIENTIFIC_NAME"):
                        text(cleaned_item_dict['scientific_name*'])
                with tag("DESCRIPTION"):
                    text(cleaned_item_dict['sample_description*'])

                for header, object in cleaned_item_dict.items():
                    if header not in ['sample_alias*', 'sample_title*', 'tax_id*', 'scientific_name*',
                                      'sample_description*']:
                        with tag("SAMPLE_ATTRIBUTE"):
                            with tag("TAG"):
                                text(header.strip("*"))
                            with tag("VALUE"):
                                text(object)

                with tag("SAMPLE_ATTRIBUTE"):
                    with tag("TAG"):
                        text("ENA-CHECKLIST")
                    with tag("VALUE"):
                        text("ERC000033")

    result = indent(
        doc.getvalue(),
        indent_text=False
    )

    with open(opts.outfile, "w") as f:
        f.write(result)

#------------------------#
#          MAIN          #
#------------------------#
if __name__ == "__main__":
    opts = parse_args(sys.argv[1:])
    gisaid_dataframe = parse_gisaid_metadata(opts)
    ena_dataframe = convert_gisaid_to_ena(gisaid_dataframe)

    if opts.outformat.lower() == 'xml':
        if opts.outfile == None:
            opts.outfile = 'ENA_output.xml'
        ena_dataframe_rearranged = ena_dataframe[ena_fields]

        xml_generator(ena_dataframe_rearranged)


    elif opts.outformat.lower() in ['excel','xls','xlsx']:
        if opts.outfile == None:
            opts.outfile = 'ENA_output.xlsx'
        write_dataframe(ena_dataframe, opts.outfile)

    else:
        sys.stderr.write(f'The file format "{opts.outformat}" is not supported, accepted values : [xml, xls, xlsx, excel]')

