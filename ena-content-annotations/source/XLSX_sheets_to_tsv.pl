#!/usr/bin/env python3
"""Script of XLSX_sheets_to_tsv.pl is to convert  sheets of XLSX to tsv

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2024-10-31
__docformat___ = 'reStructuredText'

"""


import logging
logger = logging.getLogger(__name__)
import argparse
import pandas as pd
import os

def read_excel(infile):
    """"""
    logger.debug(f"infile={infile}")
    try:
        #df = pd.read_excel(infile)
        sheets_dict = pd.read_excel(infile, sheet_name=None)
        #sheet_keys = df.keys()
        #print(sheet_keys)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    # save each DataFrame to a variable dynamically if needed
    sheet_dataframes = {sheet_name: df for sheet_name, df in sheets_dict.items()}
    logger.debug(sheet_dataframes.keys())

    directories = os.path.dirname(infile)

    for sheet in sheet_dataframes:
        logger.info(sheet)
        df = sheet_dataframes[sheet]
        print(df.head(5).to_markdown(index=False))
        outfile_name = directories + "/" + sheet.replace(" ", "_") + ".tsv"
        logger.debug(f"{outfile_name}")
        df.to_csv(outfile_name, sep="\t", index=False)
        logger.info(f"created: {outfile_name}")






def main():
    read_excel(args.file)

if __name__ == '__main__':
    logging.basicConfig(level = logging.DEBUG, format = '%(levelname)s - %(message)s')
    parser = argparse.ArgumentParser(
                    prog='XLSX_sheets_to_tsv.pl',
                    description='convert  sheets of XLSX to tsv',
                    epilog='')
    parser.add_argument("-f", "--file", help="XLSX file to convert")
    args = parser.parse_args()
    if args.file:
        logger.debug(f"args.file selected value={args.file}")
        main()
    else:
        parser.print_help()


