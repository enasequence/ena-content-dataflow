#!/usr/bin/python3

__author__ = 'Nadim Rahman'


# EXAMPLE: python define_proteomes.py --input ~/Downloads/priority.csv --taxID_header 'Taxonomy ID'
# Input to include at minimum, a column of NCBI Taxonomic IDs to compare against.


import argparse, datetime, io, json, os, requests, sys
import pandas as pd

UNIPROT_PROTEOME_URL= 'https://rest.uniprot.org/proteomes/stream'
headers = {
    'accept': 'application/json',
}       # Define headers for the requests

# Dictionary containing all the search criteria for data types
proteome_search = {
    'proteomes': {'search_fields': ['upid', 'organism', 'organism_id'], 'query': 'proteome_type:1'}
}

# Date stamp
today = datetime.date.today()
date = today.strftime('%d%m%Y')

def get_args():
    '''
    Define and obtain script arguments
    :return: Arguments object
    '''
    parser = argparse.ArgumentParser(prog='define_proteomes.py', formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog="""
        + ========================================= +
        |  EMBL-EBI Proteomes for Pathogens Portal  |
        |                                           |
        |  Tool to obtain proteomes from UniProt    |
        |  for the Pathogens Portal.                |
        + ========================================= +
        """)
    parser.add_argument('-i', '--input', help='Input CSV file that needs to be annotated with proteome information', type=str, required=True)
    parser.add_argument('-t', '--taxID_header', help='Column header name for the input file that corresponds to NCBI Taxonomic ID', type=str, required=True)
    args = parser.parse_args()
    return args


class retrieve_data:
    def __init__(self, search):
        self.search = search        # A dictionary of that includes: query and search fields to return

    def req(URL, headers, params):
        """
        Run a request and retrieve the output
        :param URL: URL used for the search
        :param headers: Headers to be used in the search
        :param params: Parameters for the request
        :return: A response object with the results of the search
        """
        response = requests.get(URL, headers=headers, params=params)  # No authentication required in the query
        return response

    def build_request_params(**kwargs):
        """
        Build parameters for the request search
        :param query_url: query URL to use in the search
        :param start: starting point for results to be retrieved
        :return: A tuple of tuples of the parameters to be used in the request search
        """
        search_params = []
        for key, value in kwargs.items():
            parameter = (key, value)
            search_params.append(parameter)
        return tuple(search_params)

    def coordinate_retrieval(self):
        """
        Run the retrieval of ENA data
        :return: Data frame
        """
        self.search_params = retrieve_data.build_request_params(query=self.search['query'],
                                                        fields=self.search['search_fields'])
        self.search_result = retrieve_data.req(UNIPROT_PROTEOME_URL, headers, self.search_params)      # Search the query
        data = json.loads(self.search_result.text)      # Load in the resulting text from the request
        self.results = pd.json_normalize(data['results'])       # Convert JSON object to pandas dataframe
        output_file = os.path.join('data', 'Search_Results_{}.txt'.format(date))
        self.results.to_csv(output_file, sep="\t", index=False)      # Save search results to a dataframe
        return self.results


if __name__ == '__main__':
    args = get_args()

    # Get data
    data_retrieval = retrieve_data(proteome_search['proteomes'])  # Instantiate class with information
    proteome_results = data_retrieval.coordinate_retrieval()

    # Compare with a list of taxa to annotate
    if args.input is not None:
        input_taxa = pd.read_csv(args.input)

        # Create dataframe of reference proteomes that are present in the input file
        merged = pd.merge(proteome_results, input_taxa, left_on='taxonomy.taxonId', right_on=args.taxID_header, how='inner')
        merged.drop_duplicates(subset=['taxonomy.taxonId'])

        # Merge and tidy the dataframe
        final = merged[['id', 'proteomeType', 'taxonomy.scientificName', 'taxonomy.taxonId']].drop_duplicates(ignore_index=True).sort_values('taxonomy.taxonId', ascending=True)
        proteome_file = os.path.join('data', 'Proteomes_{}.txt'.format(date))
        final.to_csv(proteome_file, sep="\t", index=False)
