#!/usr/bin/env python3
"""Script of get_environmental_info.py

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2024-05-09
__docformat___ = 'reStructuredText'

"""

import json
import re
import sys
import pandas as pd
from eDNA_utilities import logging
import coloredlogs
import argparse

from collections import Counter

from geography import Geography
from taxonomy import *
from eDNA_utilities import pickle_data_structure, unpickle_data_structure, my_coloredFormatter, run_webservice, \
    run_webservice_with_params, get_shorter_list, print_value_count_table, capitalise
from ena_api_calls import setup_run_api_call

logger = logging.getLogger(name = 'mylogger')
pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', None)


# pd.set_option("display.max_rows", None)

def get_query_params(checklist_type):
    my_params = {
        "srv": "https://www.ebi.ac.uk/ena/portal/api/search",
        "query": ""
    }

    # see  https://www.ebi.ac.uk/ena/browser/checklists

    if checklist_type == "environmental_checklists":
        my_params['query'] = """(environmental_sample%3Dtrue%20OR%20(\
        CHECKLIST%3D%22ERC000012%22%20OR%20CHECKLIST%3D%22ERC000020%22\
        %20OR%20CHECKLIST%3D%22ERC000021%22%20OR%20CHECKLIST%3D%22ERC000022%22%20OR%20CHECKLIST%3D\
        %22ERC000023%22%20OR%20CHECKLIST%3D%22ERC000024%22%20OR%20CHECKLIST%3D%22ERC000025%22%20OR\
        %20CHECKLIST%3D%22ERC000027%22%20OR%20CHECKLIST%3D%22ERC000055%22%20OR%20CHECKLIST%3D%22ERC000030\
        %22%20OR%20CHECKLIST%3D%22ERC000031%22%20OR%20CHECKLIST%3D%22ERC000036%22)%20OR (
        ncbi_reporting_standard%3D%22*ENV*%22%20ORncbi_reporting_standard%3D%22*WATER*%22\
        %20ORncbi_reporting_standard%3D%22*SOIL*%22%20ORncbi_reporting_standard%3D%22*AIR*%22\
        %20ORncbi_reporting_standard%3D%22*SEDIMENT*%22%20ORncbi_reporting_standard%3D%22*BUILT%22%20))\
        AND%20not_tax_tree(9606)"""
    elif checklist_type == "default_checklists":

        # 'result=read_experiment&query=not_tax_eq(9606)%20AND%20(ncbi_reporting_standard%3D%22generic%22%20OR%20(%20checklist%3D%22erc000011%22%20OR%20ncbi_reporting_standard%3D%22generic%22%20))&fields=experiment_accession%2Cexperiment_title%2Ctax_id%2Cncbi_reporting_standard&format=tsv' "https://www.ebi.ac.uk/ena/portal/api/search"
        my_params['query'] = """not_tax_eq(9606)%20AND%20(ncbi_reporting_standard%3D%22generic%22%20OR%20(%20checklist%3D%22\
                 erc000011%22%20OR%20ncbi_reporting_standard%3D%22generic%22%20))"""
    else:
        logger.error(f"unknown {checklist_type}")
        sys.exit(-1)
    return my_params


def extract_record_ids_from_json(id_field_name, json_blob):
    # print(json.dumps(json_blob, indent=4))
    record_list = []
    for record in json_blob:
        # logger.info(record)
        record_list.append(record[id_field_name])
    return record_list


def get_env_readrun_ids():
    env_read_run_id_file = "read_run.pickle"
    if os.path.exists(env_read_run_id_file):
        logger.info("env_read_run_id_file exists, so can unpickle it")
        return unpickle_data_structure(env_read_run_id_file)

    query_params_json = get_query_params("environmental_checklists")
    srv = query_params_json['srv']
    params = "result=read_run&query=" + query_params_json['query'] + "&format=json"
    limit = '&limit=5'
    url = srv + '?' + params + limit
    logger.info(url)
    output = json.loads(run_webservice(url))
    logger.info(output)
    record_list = extract_record_ids_from_json('run_accession', output)
    logger.info(len(record_list))
    pickle_data_structure(record_list, env_read_run_id_file)
    return record_list


def get_all_checklist_types():
    """
    :return: an array of all available checklist_types
    """
    checklist_types = ["environmental_checklists", "default_checklists"]
    return checklist_types


def get_env_readrun_detail(total_records_to_return):
    """

    :param total_records_to_return: if 0, this means return all.
    :return: records as a list
    """

    """
    curl -X 'POST' \
    'https://www.ebi.ac.uk/ena/portal/api/search' \
    -H 'accept: */*' \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -d 'excludeAccessionType=&download=false&query=(environmental_sample%253Dtrue%2520OR%2520(CHECKLIST%253D%2522ERC000012%2522%2520OR%2520CHECKLIST%253D%2522ERC000020%2522%2520OR%2520CHECKLIST%253D%2522ERC000021%2522%2520OR%2520CHECKLIST%253D%2522ERC000022%2522%2520OR%2520CHECKLIST%253D%2522ERC000023%2522%2520OR%2520CHECKLIST%253D%2522ERC000024%2522%2520OR%2520CHECKLIST%253D%2522ERC000025%2522%2520OR%2520CHECKLIST%253D%2522ERC000027%2522%2520OR%2520CHECKLIST%253D%2522ERC000055%2522%2520OR%2520CHECKLIST%253D%2522ERC000030%2522%2520OR%2520CHECKLIST%253D%2522ERC000031%2522%2520OR%2520CHECKLIST%253D%2522ERC000036%2522)%2520OR(ncbi_reporting_standard%253D%2522*ENV*%2522%2520ORncbi_reporting_standard%253D%2522*WATER*%2522%2520ORncbi_reporting_standard%253D%2522*SOIL*%2522%2520ORncbi_reporting_standard%253D%2522*AIR*%2522%2520ORncbi_reporting_standard%253D%2522*SEDIMENT*%2522%2520ORncbi_reporting_standard%253D%2522*BUILT%2522%2520))AND%2520not_tax_tree(9606)&excludeAccessions=&includeMetagenomes=false&dataPortal=&includeAccessionType=&includeAccessions=&format=json&fields=sample_accession%252Crun_accession%252Clibrary_strategy%252Clibrary_source%252Cinstrument_platform%252Clat%252Clon%252Ccountry%252Cbroad_scale_environmental_context%252Ctax_id%252Cchecklist%252Ccollection_date%252Cncbi_reporting_standard%252Ctarget_gene%252Ctag%252Cstudy_accession%252Cstudy_title&dccDataOnly=false&&rule=&result=read_run&limit=0' > all.json
    :return: records list

    # This is for the default_checklist
    ## curl 'https://www.ebi.ac.uk/ena/portal/api/search?result=read_run&query=not_tax_eq(9606)%20AND%20(ncbi_reporting_standard%3D%22generic%22%20OR%20(%20checklist%3D%22erc000011%22%20OR%20ncbi_reporting_standard%3D%22generic%22%20))&fields=sample_accession%2Crun_accession%2Clibrary_strategy%2Clibrary_source%2Cinstrument_platform%2Clat%2Clon%2Ccountry%2Cbroad_scale_environmental_context%2Ctax_id%2Cchecklist%2Ccollection_date%2Cncbi_reporting_standard%2Ctarget_gene%2Ctag%2Cstudy_accession%2Cstudy_title&format=json&limit=0' > default.json
    """
    logger.info("get_env_readrun_detail")

    limit = 0


    checklist_types = get_all_checklist_types()
    combined_record_list = []
    for checklist_type in checklist_types: #currently does a search of the default templates and then a search of the environmental ones
        logger.info(f"++++Doing the main search for environmental data via -->{checklist_type}<-- +++")
        if checklist_type == "default_checklists":
            env_read_run_detail_file = "read_run_allinsdc_defaultgeneric.json.pickle"
            env_read_run_detail_jsonfile = "read_run_allinsdc_defaultgeneric.json"
        else:
            env_read_run_detail_file = "read_run_allinsdc_detail.pickle"
            env_read_run_detail_jsonfile = "read_run_allinsdc_detail.json"

        if os.path.exists(env_read_run_detail_file):
            logger.info(f"{env_read_run_detail_file} exists, so can unpickle it")
            record_list = unpickle_data_structure(env_read_run_detail_file)
        # elif os.path.exists(env_read_run_detail_jsonfile):
        #     logger.info(f"{env_read_run_detail_jsonfile} exists, so using that")
        #     with open(env_read_run_detail_jsonfile, "r") as f:
        #         record_list = json.load(f)
        #         length = len(record_list)
        #         for i in range(length):
        #             # record = record_list[i]
        #             record_list[i]["query_type"] = checklist_type
        #         pickle_data_structure(record_list, env_read_run_detail_file)
        else:
            #query_params_json = get_query_params(checklist_type)
            record_list = setup_run_api_call(checklist_type, limit)
            logger.info(f"Finished running {checklist_type}")

            logger.info(f"got {len(record_list)} records")
            # record_list = record_list[0:5]
            length = len(record_list)
            for i in range(length):
                # record = record_list[i]
                record_list[i]["query_type"] = checklist_type
            with open(env_read_run_detail_jsonfile, 'w') as f:
                json.dump(record_list, f)
            logger.info(f"Writing records to pickle: {env_read_run_detail_file}")
            pickle_data_structure(record_list, env_read_run_detail_file)

        logger.info(f"record_list length: {len(record_list)}")

        if total_records_to_return > 0:  # this is only really used in debugging
            record_list = get_shorter_list(record_list, int(total_records_to_return / 2))
        combined_record_list.extend(record_list)

        logger.info(f"current combined_record len= {len(combined_record_list)}")
        # logger.info(f"current combined_record=\n{combined_record_list}")
        print("---------------------------------------------------------------------------------------")

    # pickle_out = "env_readrun_detail_all.pickle"
    # logger.info(f"writing to = {pickle_out}")
    # pickle_data_structure(df_env_readrun_detail, pickle_out)

    logger.info(f"Finished getting all RELEVANT ENA raw data combined len= {len(combined_record_list)}")
    return combined_record_list


def get_all_study_details():
    """

    :return: dataframe with all study details (study_accession study_title study_description)
    """
    study_details_file = "study_details.pickle"
    study_details_json = "study_details.json"
    if os.path.exists(study_details_file):
        logger.info("env_sample_id_file exists, so can unpickle it")
        record_list = unpickle_data_structure(study_details_file)
        logger.info(f"Number of study_details records= {len(record_list)}")
        return pd.DataFrame.from_records(record_list)
    elif os.path.exists(study_details_json):
        logger.info(f"{study_details_json} exists,")
        with open(study_details_json, "r") as f:
            record_list = json.load(f)
        pickle_data_structure(record_list, study_details_file)
        return pd.DataFrame.from_records(record_list)

    record_list = []

    # curl 'https://www.ebi.ac.uk/ena/portal/api/search?result=study&fields=study_accession%2Cstudy_title%2Cstudy_description&format=json&limit=0' >stud
    # curl 'https://www.ebi.ac.uk/ena/portal/api/search?result=study&fields=study_accession%2Cstudy_title%2Cstudy_description&format=json&limit=5'

    limit_size = "0"
    logger.info(f"Reading study details for this many {limit_size}")
    # 'result=study&fields=study_accession%2Cstudy_title%2Cstudy_description&format=tsv'
    # gets all study data, so only need to do once,
    checklist_type = "default_checklists"
    logger.info(f"In get_all_study_deails for {checklist_type}")
    query_params_json = get_query_params(checklist_type)
    srv = query_params_json['srv']
    fields = "study_accession%2Cstudy_title%2Cstudy_description"
    params = "result=study" + "&fields=" + fields + "&format=json"
    limit = '&limit=' + limit_size
    url = srv + '?' + params + limit
    logger.info(url)
    print("curl '" + url + "'")
    record_list = json.loads(run_webservice(url))
    logger.info(len(record_list))

    pickle_data_structure(record_list, study_details_file)
    logger.info(f"Number of records= {len(record_list)} records")

    return pd.DataFrame.from_records(record_list)


def get_env_sample_ids():
    env_sample_id_file = "env_sample_id_file.pickle9"

    if os.path.exists(env_sample_id_file):
        logger.info("env_sample_id_file exists, so can unpickle it")
        return unpickle_data_structure(env_sample_id_file)

    query_params_json = get_query_params("environmental_checklists")
    srv = query_params_json['srv']
    params = "result=sample&query=" + query_params_json['query'] + "&format=json"
    limit = '&limit=5'
    url = srv + '?' + params + limit
    logger.info(url)
    output = json.loads(run_webservice(url))
    logger.info(output)
    record_list = extract_record_ids_from_json('sample_accession', output)
    logger.info(record_list)
    pickle_data_structure(record_list, env_sample_id_file)
    return record_list


def select_first_part(value):
    """
    select just the first part of value before the :
    :param value:
    :return:
    """

    my_list = value.split(":")
    if my_list[0] == "GAZ":
        target_term = my_list[1]
    else:
        target_term = my_list[0]
    val1_list = target_term.split(";")
    val1 = val1_list[0]

    if len(val1) > 0:
        return val1
    else:
        return "missing"

    # #logger.info(value[:value.find(":")])
    # if value.find(":") >= 0:
    #     return value[:value.find(":")+1]
    # else:
    #     return value


def get_presence_or_absence_col(df, col_name):
    # col with and without values
    # FFS isnull etc. did not work
    col_list = df[col_name].to_list()
    absent_count = 0
    present_count = 0
    for val in col_list:
        if val is None:
            absent_count += 1
        else:
            present_count += 1
    return present_count, absent_count



def delist_col(my_list):
    """
    deconvolute a list of list
    :param my_list:
    :return: list
    """
    gene_list = []
    for gene_row_list in my_list:
        for gene in gene_row_list:
            gene_list.append(gene)
    return gene_list


def get_percentage_list(gene_list):
    """
    Takes
    :param gene_list:
    :return:
    """
    # logger.info(Counter(gene_list))
    c = Counter(gene_list)
    out = sorted([(i, c[i], str(round(c[i] / len(gene_list) * 100.0, 2)) + "%") for i in c])
    logger.info(out)
    return out


def analyse_all_study_details(df):
    """
    Generates a subset of the df, indexed from sample_accession
    Plus annotates two columns using
    barcoding_df['barcoding_genes_from_study'] = list of genes
    barcoding_df['is_barcoding_experiment_probable'] = True

    :param df:
    :return: barcoding_df
    """
    logger.info("in analyse_all_study_details---------------------------")
    logger.info(len(df))
    barcoding_pattern = '12S|16S|18S|ITS|26S|5.8S|RBCL|rbcL|matK|MATK|COX1|CO1|mtCO|barcod'
    barcoding_title_df = df[df.study_title.str.contains(barcoding_pattern, regex = True, na = False)]
    logger.info(f"'study_title' with barcoding genes total={len(barcoding_title_df)}")
    logger.info(barcoding_title_df['study_title'].sample(n = 10))

    barcoding_description_df = df[df.study_description.str.contains(barcoding_pattern, regex = True, na = False)]
    logger.info(f"'study_description' with barcoding genes total={len(barcoding_description_df)}")
    logger.info(barcoding_description_df['study_description'].sample(n = 5))

    barcoding_df = pd.concat([barcoding_title_df, barcoding_description_df]).drop_duplicates().reset_index(drop = True)
    logger.info(f"barcoding total = {len(barcoding_df)}")
    barcoding_df['combined_tit_des'] = barcoding_df['study_title'] + barcoding_df['study_description']
    barcoding_df['is_barcoding_experiment_probable'] = True

    def get_barcoding_genes(value):

        sgenes_pattern = re.compile(r'^([1-9]{2}|5\.8)([sS])[ ]?(r)?([RD]NA)?', flags = 0)
        sgenes_pattern = re.compile(r'^([1-9]{2}|5\.8)([sS])[ ]?r?(RNA|DNA|ribo)?', flags = 0)
        rbcl_pattern = re.compile(r'^(RBCL)', re.IGNORECASE)
        its_pattern = re.compile(r'^(ITS)([1-2])?')
        matk_pattern = re.compile(r'^(matk)', re.IGNORECASE)
        COX1_pattern = re.compile('^COX1|CO1|COI|mtCO|Cytochrome c oxidase|cytochrome oxidase', re.IGNORECASE)

        def clean_name(my_list):
            """
             a clean harmonised list of barcoding gene names
            :param  my_list: # list of gene names
            :return: harmonised list.
            """
            clean_list = []
            # logger.info("-----------------------------------------------------------")
            for my_gene in my_list:
                # logger.info(my_gene)

                match = re.search(rbcl_pattern, my_gene)
                if match:
                    # logger.info(f"----------clean=rbcL")
                    clean_list.append("rbcL")
                    continue
                match = re.search(its_pattern, my_gene)
                if match:
                    if match.group(2):
                        # logger.info(f"----------clean=ITS{match.group(2)}")
                        clean_list.append("ITS" + match.group(2))
                    else:
                        # logger.info(f"----------clean=ITS")
                        clean_list.append("ITS")
                    continue
                match = re.search(matk_pattern, my_gene)
                if match:
                    # logger.info("----------clean=matK")
                    clean_list.append("matK")
                    continue
                match = re.search(COX1_pattern, my_gene)
                if match:
                    # logger.info("----------clean=COX1")
                    clean_list.append("COX1")
                    continue

                match = re.search(sgenes_pattern, my_gene)
                if match:
                    # logger.info(match.group(1))
                    # logger.info(match.group(2))
                    clean_gene_name = match.group(1) + "S"
                    if match.group(3):
                        # logger.info(f"---------------{match.group(3)}")
                        clean_gene_name += " r" + match.group(3)
                    clean_list.append(clean_gene_name)
                    # logger.info(clean_gene_name)
                    continue

                logger.info(f"remaining gene: -->{my_gene}<--")
                sys.exit()

            return clean_list

        barcode_genes_pattern = re.compile(
            r'16[sS][ ]?r?[RD]NA|16[sS][ ]?ribo|18S|12S|ITS[12]?|26[Ss]|5.8[Ss]|rbcL|rbcl|RBCL|matK|MATK|cox1|co1|COX1|CO1|COI|mtCO|cytochrome c oxidase|cytochrome oxidase')
        genes = list(set(re.findall(barcode_genes_pattern, value)))
        if len(genes) > 0:
            # logger.info(genes)
            return clean_name(genes)
        else:
            genes = list(set(re.findall(r'16[Ss]', value)))
            if len(genes) > 0:
                return clean_name(genes)
            return None

    barcoding_df['barcoding_genes_from_study'] = barcoding_df.combined_tit_des.apply(get_barcoding_genes)
    logger.info(barcoding_df['barcoding_genes_from_study'].value_counts())
    print_value_count_table(barcoding_df['barcoding_genes_from_study'])

    gene_list = delist_col(
        barcoding_df[barcoding_df['barcoding_genes_from_study'].notnull()]['barcoding_genes_from_study'].to_list())
    get_percentage_list(gene_list)
    gene_set = set(gene_list)
    total = len(barcoding_df)
    present_count, absent_count = get_presence_or_absence_col(barcoding_df, 'barcoding_genes_from_study')
    logger.info(f"barcoding_genes_from_study present_count {present_count}  {present_count / total * 100:.2f}%")
    logger.info(f"barcoding_genes_from_study absent_count {absent_count}   {absent_count / total * 100:.2f}%")

    return barcoding_df

def has_a_value(value):
    if type(value) is not str:
        return False
    elif value is None:
        return False
    elif len(value) == 0:
        return False
    return True

def process_geographical_data(old_df):
    """
    process geographical data, used in both this and the analysis script!
    :param old_df:
    :return:
    """
    logger.info(f"process geographical data rows in={len(old_df)}")

    df = old_df.copy()
    # if 'country_clean' in df.columns:
    #     logger.info("geographical data already processed, so skip and return df")
    #     return df

    df['has_geographical_coordinates'] = True
    df['has_geographical_coordinates'] = df['has_geographical_coordinates'].mask(df['lat'].isna(), False)
    print_value_count_table(df.has_geographical_coordinates)
    os.rename('../tables/has_geographical_coordinates.md','../tables/ena_has_geographical_coordinates.md')
    os.rename('../tables/has_geographical_coordinates.tsv', '../tables/ena_has_geographical_coordinates.tsv')

    df['has_broad_scale_environmental_context'] = True
    df['has_broad_scale_environmental_context'] = df['has_broad_scale_environmental_context'].mask(
        df['broad_scale_environmental_context'] == '', False)

    # print_value_count_table(df.has_broad_scale_environmental_context)
    # print_value_count_table(df.broad_scale_environmental_context)

    geography_obj = Geography()
    insdc_country_set = geography_obj.get_insdc_full_country_set()
    insdc_country_set.add("United States of America")
    insdc_country_set.add("missing")
    logger.debug(f"insdc_country_list: {sorted(insdc_country_set)}")
    lower_country_dict = {}
    for country in insdc_country_set:
        # logger.info(f"country: --->{country}<----")
        lc_country = str(country).lower()
        lower_country_dict[lc_country] = country
    countries_re = re.compile("|".join(insdc_country_set), re.IGNORECASE)
    blank_re = re.compile("^[,.] ?$|^$")
    special_country_dict = {
        "USA": "United States of America",
        "United States": "United States of America",
        "US": "United States of America",
        "UK": "United Kingdom",
        "England": "United Kingdom",
        "Wales": "United Kingdom",
        "Scotland": "United Kingdom",
        "Macedonia": "Northern Macedonia",
        "Tasmania": "Australia",
        "Yenisei river": "Mongolia",
        "NULL": "missing",
        "MISSING": "missing",
        "Cote d'Ivoire": "Cote d'Ivoire",
        "Faroe": "Faroe Islands",
        "Temperate Northern Atlantic": "Atlantic Ocean",
        "Atlantic": "Atlantic Ocean",
        "Antarctic Peninsula": "Antarctica",
        "PuertoRico": "Puerto Rico",
        "Korea": 'South Korea',
        "KOREA": 'South Korea',
        "korea": 'South Korea',
        "Darwin, Norther Territory": "Australia",
        'Vietnam': 'Viet Nam',
        "Nabanhe": "China",
        "Hanzhong": "China"
    }
    special_country_list = list(special_country_dict.keys())
    special_countries_re = re.compile("|".join(special_country_list), re.IGNORECASE)

    def extra_country_clean(value):
        """
        extra country clean to do a level of cleaning ip in addition to the easy ones.
        :param value:
        :return:
        """
        if value in special_country_dict:
            return special_country_dict[value]
        elif value in insdc_country_set:
            return value

        match = countries_re.search(value)
        if match:
            match_group = match.group()
            logger.debug(f"insdc_country_clean: {value} and {match} and '{match_group}'")
            if match_group in special_country_dict:
                return special_country_dict[match_group]
            elif match_group.lower() in lower_country_dict:  # copes with all upper and all lower case countries
                return lower_country_dict[match_group.lower()]

            return match.group()

        match = special_countries_re.search(value)
        if match:
            logger.debug(f"special_country_clean: {value} and {match} and '{match.group()}'")
            if match.group() in special_country_dict:
                return special_country_dict[match.group()]
            return match.group()
        elif blank_re.search(value):
            return "Unknown"
        else:
            logger.debug(f"insdc_country_clean:, not match for '{value}'")
            return value

    df['country_clean'] = df['country'].apply(select_first_part)
    df['country_clean'] = df['country_clean'].apply(extra_country_clean)
    logger.info(f"country_clean={df['country_clean'].value_counts()}")
    df_country_clean_count = df['country_clean'].value_counts().to_frame('count').reset_index()
    outfile = "clean_country.tsv"
    logger.info(f"df_country_clean_count={df_country_clean_count} the cleaner list is here {outfile}")
    df_country_clean_count.to_csv(outfile, sep = '\t', index = False)

    df['continent'] = df['country_clean'].apply(geography_obj.get_continent)
    logger.info(f"continent_counts={df.continent.value_counts()}")
    df['ocean'] = df['country_clean'].apply(geography_obj.get_ocean)


    logger.info(f"First doing a naive european sea search, then more comprehensive one")
    df['european_sea'] = df['country_clean'].apply(geography_obj.get_european_sea)
    logger.info(f"df['european_sea'].value_counts()={df['european_sea'].value_counts()}")

    df['is_european_sea'] = df['european_sea'].apply(has_a_value)
    df_europe = df.query('continent == "europe" | is_european_sea')
    # df_europe = df.query('is_european_sea')
    #df_europe = df.query('continent == "europe"')
    logger.info(f"len(df_europe)={len(df_europe)}")
    df_europe['has_geographical_coordinates'] = True
    df_europe['has_geographical_coordinates'] = df_europe['has_geographical_coordinates'].mask(df_europe['lat'].isna(), False)
    print_value_count_table(df_europe.has_geographical_coordinates)
    os.rename('../tables/has_geographical_coordinates.md','../tables/ena_europe_has_geographical_coordinates.md')
    os.rename('../tables/has_geographical_coordinates.tsv', '../tables/ena_europe_has_geographical_coordinates.tsv')

    df_europe_land = df.query('continent == "europe"')
    df_europe_land['has_geographical_coordinates'] = df_europe_land['has_geographical_coordinates'].mask(df_europe_land['lat'].isna(), False)
    print_value_count_table(df_europe_land.has_geographical_coordinates)

    df_europe_seas = df.query('is_european_sea')
    df_europe_seas['has_geographical_coordinates'] = df_europe_seas['has_geographical_coordinates'].mask(df_europe_seas['lat'].isna(), False)
    print_value_count_table(df_europe_seas.has_geographical_coordinates)

    # creating this pattern ready for the get_european_sea_value function
    europe_sea_list = list(geography_obj.europe_sea_set)
    logger.info(f"european_sea_list={europe_sea_list}")
    combined_list_joined = "|".join(europe_sea_list)
    europe_sea_re = re.compile(combined_list_joined, re.IGNORECASE)
    # df['match_european_sea'] = df['country_clean'].str.match(europe_sea_re)

    def get_european_sea_value(value):
        match = europe_sea_re.search(value)
        if match is None:
            return None
        rtn_value = capitalise(match.group())
        # logger.info(rtn_value)
        return rtn_value

    df['european_sea'] = df['country'].apply(get_european_sea_value)
    logger.info(f"df['european_sea'].value_counts()={df['european_sea'].value_counts()}")
    print_value_count_table(df.european_sea)

    freshwater_type_list = ['(river|lake|freshwater) sediment', 'groundwater', 'ground[ _-]water', 'spring',
                            'reservoir', 'river', 'lake', 'pond', 'stream', 'beck', 'brook', 'tarn', 'mere',
                            'waterfall', 'wetland', 'ghyll', 'canal', 'bog', 'drinking water']
    combined_list_joined = "|".join(freshwater_type_list)
    # freshwater_type_re = re.compile(r'\b(" + combined_list_joined + ")"', re.IGNORECASE)
    freshwater_type_re = re.compile(combined_list_joined, re.IGNORECASE)

    freshwater_consolidation_dict = {
        'river sediment': 'freshwater_sediment',
        'lake sediment': 'freshwater_sediment',
        'freshwater sediment': 'freshwater_sediment',
        'sediment': 'freshwater_sediment',
        'lake': 'lake',
        'mere': 'lake',
        'tarn': 'lake',
        'pond': 'pond',
        'river': 'river',
        'brook': 'river',
        'canal': 'canal',
        'reservoir': 'reservoir',
        'stream': 'stream',
        'beck': 'stream',
        'waterfall': 'waterfall',
        'wetland': 'wetland',
        'spring': 'spring',
        'ground_water': 'groundwater',
        'ground water': 'groundwater',
        'ground-water': 'groundwater',
        'groundwater': 'groundwater',
        'bog': 'bog',
        'drinking water': 'drinking_water'

    }

    #         'freshwater': 'freshwater_unknown_type'
    def get_freshwater_type_value(value):
        # used for both query country/region and ENVO
        match = freshwater_type_re.search(value)
        if match is None:
            return None
        rtn_value = match.group()
        rtn_value = rtn_value.lower()
        rtn_value = freshwater_consolidation_dict[rtn_value]
        # logger.info(rtn_value)
        return rtn_value

    logger.info(f"freshwater types worldwide")
    df['freshwater_type_country'] = df['country'].apply(get_freshwater_type_value)
    print_value_count_table(df.freshwater_type_country)

    logger.info(df.columns)

    df['freshwater_type_envo'] = df['broad_scale_environmental_context'].apply(get_freshwater_type_value)
    print_value_count_table(df.freshwater_type_envo)

    def choose_freshwater_type(row):
        if row['freshwater_type_envo'] is not None:
            # logger.info(f"choose freshwater_type_envo")
            return row['freshwater_type_envo']
        elif row['freshwater_type_country'] is not None:
            # logger.info(f"choose freshwater_type_country")
            return row['freshwater_type_country']
        return None

    logger.info(f"Combining freshwater types ENVO and COUNTRY")
    df['freshwater_type'] = df.apply(choose_freshwater_type, axis = 1)
    print_value_count_table(df.freshwater_type)

    # print(df.groupby(['broad_scale_environmental_context','freshwater_type_country','country']).size().to_frame('count').reset_index().sort_values('count').to_string())

    df_europe = df.query('continent == "europe"')
    logger.info(f"freshwater types in europe")
    print_value_count_table(df_europe.freshwater_type)

    logger.info(f"process geographical data rows out={len(df)}")
    return df



def filter_for_aquatic(df):
    """

    :param df: = pd.DataFrame.from_records(env_readrun_detail)
    :return: df

    TODO: add filter for environmental_medium

    """
    logging.info("filter_for_aquatic")

    # logger.info(df["broad_scale_environmental_context"].value_counts())
    # outfile = "broad_scale_environmental_context.txt"
    # with open(outfile, "w") as f:
    #     f.write(df["broad_scale_environmental_context"].value_counts().to_string())
    # logger.info(f"wrote {outfile}")

    # df = df.head(10000)
    logger.info(df.columns)

    aquatic_pattern = re.compile('marine|freshwater|coastal|brackish')
    aquatic_biome_pattern = re.compile(
        'marine|ocean|freshwater|coastal|brackish|estuar|fresh '
        'water|groundwater|glacial_spring|^sea|seawater|lake|river|wastewater|waste water|stormwater',
        re.IGNORECASE)

    def local_process_env_tags(value):
        my_tag_list = value.split(';')

        my_env_tags = [s for s in my_tag_list if "env_" in s]

        return my_env_tags

    def test_if_aquatic(value):
        if re.search(aquatic_pattern, value):
            return True

        return False

    def test_if_ocean(value):
        if len(value) == 0 or value == 'not ocean':
            return False

        return True

    def test_if_aquatic_biome_pattern(value):
        if re.search(aquatic_biome_pattern, value):
            return True

        return False

    logging.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    # print_value_count_table(df.tag)
    # logger.info(df.tag.head(50))
    # df['env_tax'] = df['tag'].str.extract("(env_tax:[^;]*)")[0]
    df['env_tag'] = df['tag'].apply(local_process_env_tags)
    df['env_tags'] = df['env_tag'].apply(lambda x: ';'.join(x))
    # print_value_count_table(df.env_tags)
    df['aquatic'] = df['env_tags'].apply(test_if_aquatic)
    logging.info(f"aquatic= {df['aquatic'].value_counts()}")
    df_aquatic = df[df['aquatic'] == True]
    df_remainder = df[df['aquatic'] == False]
    logging.info(
        f"total_records={len(df)}, using env_tags aquatic_filtered={len(df_aquatic)} and remainder={len(df_remainder)}")

    logging.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    df_remainder = process_geographical_data(df_remainder)
    logger.info(df_remainder.columns)
    logging.info(df_remainder['ocean'].value_counts())
    df_remainder['aquatic'] = df_remainder['ocean'].apply(test_if_ocean)
    logging.info(f"ocean: aquatic= {df_remainder['aquatic'].value_counts()}")
    df_remainder_aquatic = df_remainder[df_remainder['aquatic'] == True]
    df_remainder_nonaquatic = df_remainder[df_remainder['aquatic'] == False]
    logging.info(
        f"after using test_if_ocean aquatic_filtered={len(df_remainder_aquatic)}  not {len(df_remainder_nonaquatic)}")
    df_aquatic = pd.concat([df_aquatic, df_remainder_aquatic])
    logging.info(f"len(df_aquatic) = {len(df_aquatic)}")

    logging.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    df_remainder = df_remainder_nonaquatic.copy()
    df_remainder['aquatic'] = df_remainder['broad_scale_environmental_context'].apply(test_if_aquatic_biome_pattern)
    logging.info(f"broad_scale_environmental_context aquatic= {df_remainder['aquatic'].value_counts()}")
    df_remainder_aquatic = df_remainder[df_remainder['aquatic'] == True]
    df_remainder_nonaquatic = df_remainder[df_remainder['aquatic'] == False]
    logging.info(
        f"after using test_if_aquatic_biome_pattern={len(df_remainder_aquatic)}  not {len(df_remainder_nonaquatic)}")
    logging.info(f"after using broad_scale_environmental_context aquatic_filtered={len(df_remainder_aquatic)}")
    df_aquatic = pd.concat([df_aquatic, df_remainder_aquatic])
    logging.info(f"len(df_aquatic) = {len(df_aquatic)}")

    return df_aquatic


def main():


    # df_all_study_details = analyse_all_study_details(get_all_study_details())
    # logger.info(len(df_all_study_details))
    #
    # sample_ids = get_env_sample_ids()
    # logger.info(len(sample_ids))
    # readrun_ids = get_env_readrun_ids()
    # logger.info(len(readrun_ids))

    # get_all_study_details()
    # sys.exit()

    total_records_to_retrieve = 100000
    total_records_to_retrieve = 0

    env_readrun_detail = get_env_readrun_detail(total_records_to_retrieve)
    logging.info(f"env_readrun_detail len= {len(env_readrun_detail)}")
    sys.exit("exiting early")

    df_env_readrun_detail = pd.DataFrame.from_records(env_readrun_detail)


    sys.exit("Stopping this early as got what I need...")
    df_env_readrun_detail = filter_for_aquatic(df_env_readrun_detail)
    df_aquatic_env_readrun_detail_pickle = "df_aquatic_env_readrun_detail.pickle"
    pickle_data_structure(df_env_readrun_detail, df_aquatic_env_readrun_detail_pickle)
    logger.info(f"writing to = {df_aquatic_env_readrun_detail_pickle}")
    logger.info("WTF")
    # sys.exit()
    # logger.info(f"pickled to {df_aquatic_env_readrun_detail_pickle}")
    #
    #
    # This is what used to be run!
    # readrun_ids_set = set(df_env_readrun_detail['run_accession'])
    # logging.info(f"readrun_ids_set={len(readrun_ids_set)}")
    # sample_ids_set = set(df_env_readrun_detail['sample_accession'])
    # logging.info(f"sample_ids_set={len(sample_ids_set)}")
    #
    # analyse_readrun_detail(df_env_readrun_detail)


if __name__ == '__main__':
    logging.basicConfig(level = logging.INFO)

    coloredlogs.install(logger = logger)
    logger.propagate = False
    ch = logging.StreamHandler(stream = sys.stdout)
    ch.setFormatter(fmt = my_coloredFormatter)
    logger.addHandler(hdlr = ch)
    logger.setLevel(level = logging.INFO)

    # Read arguments from command line
    prog_des = "Script to query ENA(INSDC) resources, but mainly to analyse the eDNA metadata from the different work"

    parser = argparse.ArgumentParser(description = prog_des)

    # Adding optional argument, n.b. in debugging mode in IDE, had to turn required to false.
    parser.add_argument("-d", "--debug_status",
                        help = "Debug status i.e.True if selected, is verbose",
                        required = False, action = "store_true")

    parser.parse_args()
    args = parser.parse_args()

    if args.debug_status:
        logger.setLevel(level = logging.DEBUG)
    else:
        logger.setLevel(level = logging.INFO)
    logger.info(prog_des)

    main()
