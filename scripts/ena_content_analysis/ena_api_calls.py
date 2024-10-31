#!/usr/bin/env python3
"""Script of ena_api_calls.py is to setup ena_api_calls

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2024-09-23
__docformat___ = 'reStructuredText'
chmod a+x ena_api_calls.py
"""

import json
import sys
from eDNA_utilities import logging, run_webservice_with_params
import coloredlogs
logger = logging.getLogger(name = 'mylogger')

def get_environment_ena_checklist_query():
    """
    Construct the checklist query part for the environmental checklists
    :return: string
    """
    checklists = ["ERC000012", "ERC000013", "ERC000020", "ERC000021", "ERC000022", "ERC000023", "ERC000024",
                  "ERC000025", "ERC000027", "ERC000055", "ERC000030", "ERC000031", "ERC000036", "ERC000040"]
    checklist_query = " OR ".join([f'CHECKLIST="{checklist}"' for checklist in checklists])
    return checklist_query

def get_environment_ncbi_reporting_standards_query():
    """
    Construct the reporting standards query part
    :return: string
    """
    reporting_standards = ["*ENV*", "*WATER*", "*SOIL*", "*AIR*", "*SEDIMENT*", "*BUILT*", "*SURVEY*HOST-ASSOCIATED*"]
    reporting_query = " OR ".join([f'ncbi_reporting_standard="{standard}"' for standard in reporting_standards])
    return reporting_query

def get_default_ena_checklist_query():
    """
    :return:  string
    """
    return 'CHECKLIST=ERC000011'

def get_default_ncbi_reporting_standards_query():
    """
    :return:  string
    """
    return 'ncbi_reporting_standard=generic'

def environment_fields_to_retrieve():
    """
    Define the fields to retrieve
    :return: fields array
    """

    fields = [
        "sample_accession", "run_accession", "library_strategy", "library_source",
        "instrument_platform", "lat", "lon", "country", "broad_scale_environmental_context", "environmental_medium",
        "tax_id", "checklist", "collection_date", "ncbi_reporting_standard",
        "target_gene", "tag", "study_accession", "study_title", "sample_collection"
    ]
    # other fields that Elianne wanted, but not in 'https://www.ebi.ac.uk/ena/portal/api/returnFields?result=read_run'
    # samp_mat_process
    # size_frac    - size fraction selected
    # samp_size  - amount or size of sample collected
    # samp_vol_we_dna_ext - sample volume or weight for DNA extraction
    # nucl_acid_ext - nucleic acid extraction kit

    return fields

def get_all_environment_params(checklist_type, limit):
    """
    getting all the environment params
    :param checklist_type:
    :param limit:
    :return: params
    """

    if checklist_type == 'environmental_checklists':
        checklist_query = get_environment_ena_checklist_query()
        reporting_query = get_environment_ncbi_reporting_standards_query()
    elif checklist_type == 'default_checklists':
        checklist_query = get_default_ena_checklist_query()
        reporting_query = get_default_ncbi_reporting_standards_query()
    else:
        sys.exit(f"checklist_type={checklist_type} is not supported")

    # Combine the full query
    query = f"(environmental_sample=true OR ({checklist_query}) OR ({reporting_query})) AND not_tax_tree(9606)"
    fields = environment_fields_to_retrieve()

    # Encode query parameters
    params = {
        "result": "read_run",
        "query": query,
        "fields": ",".join(fields),
        "format": "json",
        "limit": limit
    }
    return params

def get_base_ena_search_url():
    return "https://www.ebi.ac.uk/ena/portal/api/search"

def setup_run_api_call(checklist_type, limit):
    """
    setup and run ena api
    :param checklist_type:
    :param limit:
    :return: API call output
    """

    base_url = get_base_ena_search_url()

    params = get_all_environment_params(checklist_type, limit)
    limit = params["limit"]
    logger.info(f"base_url={base_url}")
    logger.info(f"params={params}")

    out = run_webservice_with_params(base_url, params)
    if 0 < limit < 1000:
        logger.info(f"out={out}")
    output = json.loads(out)

    if 0 < limit < 1000:
        logger.info(output)
        # record_list = extract_record_ids_from_json('run_accession', output)
        # logger.info(len(record_list))
    # sys.exit("Exiting from get_env_readrun_detail in setup_run_api_call")

    logger.info(len(output))

    return output


def main():
   print("")   

if __name__ == '__main__':
    main()
