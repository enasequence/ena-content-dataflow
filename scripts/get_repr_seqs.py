#!/usr/bin/python

import requests
import argparse
import pandas as pd

parser = argparse.ArgumentParser(description='Tool to select oldest sequence of a lineage with strong scorpio support. The script takes two arguments; an input file that is a complete pangoling output csv file and a date argument. Script automatically writes output to file representative_lineages_out.txt')

parser.add_argument('-i',
                    '--input_file',
                    help="Complete pangolin output csv.",
                    type=str,
                    required=True)

parser.add_argument('-d',
                    '--date',
                    help="Earliest date to start selecting from in Y-M-D format. Default argument is 2020-01-01.",
                    type=str,
                    default="2020-01-01",
                    required=True)

args = parser.parse_args()


# Function truncates pangolin output, combines with query output to generate final output
def read_join_filter(infile, query_output):
    df1 = pd.read_csv(infile)
    df2 = pd.read_csv(query_output, sep='\t')
    df2 = df2.rename(columns={'accession': 'taxon', 'collection_date': 'date'})
    df3 = df1.set_index('taxon').join(df2.set_index('taxon'))
    df3 = df3.dropna(subset=['scorpio_support'])
    df3 = df3[df3['scorpio_support'] > 0.95 ]
    df3 =df3[df3['date'] > args.date ]
    df3 = df3.sort_values('lineage')
    df3 = df3[['lineage','scorpio_call','scorpio_support','date', 'note']]
    df3.reset_index(level=0, inplace=True)
    df3 = (df3.sort_values('date').groupby('lineage', as_index=False).first().reset_index(drop=True).assign(a=lambda x: x.index + 1))
    df3.to_csv('representative_lineages_out.txt', sep='\t')

# Function queries ENA advanced search API for collection date fields information

def query_api():
    url = "https://www.ebi.ac.uk/ena/portal/api/search"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {'result': 'sequence', 'query':'tax_tree(2697049)', 'fields': 'collection_date', 'limit': '0', 'format': 'tsv'}
    r = requests.post(url, data=payload, headers=headers)
    results = r.text
    return results


if __name__ == '__main__':
    query_out = query_api()
    with open('output.txt', 'w', encoding="utf-8") as out_file:
        out_file.write(query_out)
    read_join_filter(args.input_file, 'output.txt')


