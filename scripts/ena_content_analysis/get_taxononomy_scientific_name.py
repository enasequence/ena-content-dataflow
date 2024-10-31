#!/usr/bin/env python3
"""Script of get_taxononomy_scientific_name.py is to get_taxononomy_scientific_name.py

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2024-05-09
__docformat___ = 'reStructuredText'
chmod a+x get_taxononomy_scientific_name.py
"""

import requests
import xml.etree.ElementTree as ET

from eDNA_utilities import logger
import argparse
import sys


def get_taxonomy_root(taxid):
    """

    :return:
    """
    # curl -s https://www.ebi.ac.uk/ena/browser/api/xml/797283 | xq | sed 's/@//g' | jq '.TAXON_SET.taxon | .scientificName'
    url = r'https://www.ebi.ac.uk/ena/browser/api/xml/' + str(taxid)
    r = requests.get(url)

    if r.status_code == 200:
        # print(r.text)
        root = ET.fromstring(r.text)
    else:
        logger.info(r.status_code)
        logger.info(f"for url={r.url}")
        sys.exit(1)
    return root

def get_taxonomy_scientific_name(taxid):
    """"""
    root = get_taxonomy_root(taxid)
    taxon = root.find('.//taxon')
    # logger.info(taxon.attrib['scientificName'])
    return taxon.attrib['scientificName']

def get_pretty_taxonomy_scientific_name(taxid):
    scientific_name = get_taxonomy_scientific_name(taxid)
    return f"{scientific_name}({taxid})"

def get_pretty_taxonomy_rankings(taxid):
    """
    :param tax
    # example root(1);unclassified entries(2787823);unclassified sequences(12908);metagenomes(408169);ecological metagenomes(410657);ant fungus garden metagenome(797283)
    :return: get_pretty_taxonomy_rankings string
    """
    root = get_taxonomy_root(taxid)
    logger.info(root)
    taxon = root.find('.//taxon')
    logger.info(taxon.attrib)
    logger.info(taxon.attrib['scientificName'])

    pretty_taxonomy_rankings = [f"{taxon.attrib['scientificName']}({taxid})"]
    logger.info(pretty_taxonomy_rankings)

    for lineage in taxon.findall('.//lineage'):

        logger.info(lineage.tag)
        logger.info(lineage.attrib)
        for child in lineage:
            logger.info(child.attrib['scientificName'])
            logger.info(child.attrib['taxId'])
            pretty_taxonomy_rankings.append(f"{child.attrib['scientificName']}({child.attrib['taxId']})")

    logger.info(pretty_taxonomy_rankings)
    logger.info(list(reversed(pretty_taxonomy_rankings)))
    return ";".join(reversed(pretty_taxonomy_rankings))


def main(args):
    tax_ranking_string = args.tax_rank_string
    # tax_ranking_string = "1;10239;12333"
    #taxid = 797283
    finest_taxid = tax_ranking_string.split(";")[-1]  # want the latest

    logger.info(finest_taxid)
    logger.info(get_pretty_taxonomy_scientific_name(finest_taxid))
    pretty_taxonomy_rankings = get_pretty_taxonomy_rankings(finest_taxid)
    if (len(pretty_taxonomy_rankings) < 5):
        pretty_taxonomy_rankings = "not_found"
    print(f"RTN={pretty_taxonomy_rankings}")



if __name__ == '__main__':
    # Read arguments from command line
    prog_des = "Script to create a pretty taxonomy scientific name based on taxonomic rankings"

    parser = argparse.ArgumentParser(description = prog_des)

    # Adding optional argument, n.b. in debugging mode in IDE, had to turn required to false.
    parser.add_argument("-d", "--debug_status",
                        help = "Debug status i.e.True if selected, is verbose",
                        required = False, action = "store_true")
    parser.add_argument("-t", "--tax_rank_string",
                        help = "tax_rank_string of form [0-9*];[0-9*];[0-9*];[0-9*] e.g. = 1;10239;12333 ",
                        required = True
                        )
    parser.parse_args()
    args = parser.parse_args()

    logger.info(prog_des)

    main(args)
