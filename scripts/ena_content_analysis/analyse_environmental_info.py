#!/usr/bin/env python3
"""Script of analyse_environmental_info.py

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2024-05-09
__docformat___ = 'reStructuredText'
chmod a+x get_taxonomy_scientific_name.py
"""

import logging
import re
from math import log
import sys
import argparse
import pandas as pd
import plotly.express as px

from eDNA_utilities import print_value_count_table, \
    plot_sankey, my_coloredFormatter, plot_countries, plot_sunburst, \
    get_ena_checklist_dict, obj_print_and_display_md
from get_environmental_info import get_all_study_details, process_geographical_data
from taxonomy import *

logger = logging.getLogger(name = 'mylogger')
pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', None)
pd.options.mode.copy_on_write = True

def clean_dates_in_df(my_df):
    my_df['collection_year'] = my_df['collection_date'].apply(collection_date_year)
    my_df['collection_year'] = pd.to_numeric(my_df['collection_year'], errors = 'coerce').astype('Int64')
    my_df['collection_year_bin'] = my_df['collection_year'].apply(create_year_bins)
    return my_df

def select_first_part(value):
    """
    select just the first part of value before the :
    :param value:
    :return:
    """

    my_list = value.split(":")
    if len(my_list[0]) > 0:
        return my_list[0]
    else:
        return "missing"

    # #logger.info(value[:value.find(":")])
    # if value.find(":") >= 0:
    #     return value[:value.find(":")+1]
    # else:
    #     return value

def isNaN(num):
    return num != num

def get_presence_or_absence_col(df, col_name):
    # col with and without values
    # FFS isnull etc. did not work
    col_list = df[col_name].to_list()
    absent_count = 0
    present_count = 0

    for val in col_list:
        logger.debug("val: {}".format(val))
        if val is None or isNaN(val):
            absent_count += 1
        elif type(val) == list and len(val) == 0:
            absent_count += 1
        else:
            present_count += 1
    return present_count, absent_count

def filter_on_library_strategies(df, library_strategy_list_to_keep):

    logger.info("before filtering")
    print_value_count_table(df.library_source)
    print_value_count_table(df.library_strategy)

    logger.info(library_strategy_list_to_keep)

    df = df.loc[df['library_strategy'].isin(library_strategy_list_to_keep)]
    logger.info(f"after filtering count = {len(df)}")
    print_value_count_table(df.library_strategy)
    print_value_count_table(df.library_source)
    return df


def plot_simple_pie(df, count_field, key_field, title, plotfile):
    tmp_df = px.data.gapminder().query
    fig = px.pie(df, values = count_field, names = key_field, title = title)
    logger.info(f"writing {plotfile}")
    fig.write_image(plotfile)



def experimental_analysis_inc_filtering(df):
    logger.info(df.columns)

    logger.info(df['library_strategy'].value_counts())
    print_value_count_table(df.library_source)
    logger.info(f"type = {type(df)}")

    plot_df = df.groupby(['instrument_platform']).size().to_frame('record_count').reset_index().sort_values(by=['record_count'], ascending=False)
    obj_print_and_display_md(plot_df,"instrument_platform")

    plot_simple_pie(plot_df,'record_count','instrument_platform', '' , "../images/ena_instrument_platform_pie.png")

    print(df.groupby(['library_source', 'library_strategy']).size().reset_index().to_markdown(index=False))
    #logger.info(df.head(10).to_markdown(index=False))
    logger.info(df.columns)
    path_list = ['library_source', 'library_strategy', 'instrument_platform', 'collection_year_bin']
    plot_df = df.groupby(path_list).size().to_frame('record_count').reset_index()
    logger.info(plot_df.to_markdown(index=False))
    plotfile = "../images/experimental_analysis_strategy.png"
    logger.info(f"plotting {plotfile}")
    sankey_link_weight = 'record_count'
    plot_sankey(plot_df, sankey_link_weight, path_list,
                'Figure ENA Aquatic "Environmental" readrun record count: library_source, library_strategy, platform, collection_date',
                plotfile)

    return df

def get_filtered_study_details(df):
    """
    get_filtered_study_details provides a data frame on a limited set of study details
    :param df: where "study_accession" is a field
    :return: df_filtered_study_details
    """
    logger.info(df.columns)
    study_accession_list = list(set(df['study_accession'].unique()))
    logger.debug(f"study_accession_list from  all studies total={len(study_accession_list)}")


    df_all_study_details = get_all_study_details()
    all_aquatic_study_accession_list = list(set(df_all_study_details['study_accession'].to_list()))
    logger.debug(f"Number of TOTAL aquatic studies: {len(all_aquatic_study_accession_list)}")

    df_filtered_study_details = df_all_study_details[df_all_study_details['study_accession'].isin(study_accession_list)]
    logger.debug(f"Number of FILTERED aquatic study IDS: {len(all_aquatic_study_accession_list)}")

    return df_filtered_study_details

def target_gene_analysis(df):
    """
    for the target genes as a checklist field
    :param df:
    :return: # naught
    """
    logger.info(f"Coming into target gene analysis have total tows of {len(df)}")


    analyse_barcode_study_details(df)

    logger.info("for the target genes as a checklist field")
    logger.debug(df['target_gene'].value_counts().head())
    # print_value_count_table(df['target_gene'])
    total = len(df)

    df["target_gene_clean_set"] = df["target_gene"].apply(get_barcoding_genes)

    tmp_df = df[df['target_gene'] != ""]
    print_value_count_table(tmp_df['target_gene'])

    print_value_count_table(tmp_df['target_gene_clean_set'])

    total_w_tgs = len(tmp_df)
    logger.info(f"total target_gene count = {total_w_tgs} / {total} = {round((100 * total_w_tgs/ total),2)}%")
    logger.info("---------------+++++++++++++++++++----------------")

    return


def add_taxonomy_columns(df):
    """

    :param df:
    :return: df: with scientific_name, lineage and tax_lineage
    """
    def lineage_lookup(value):
        # logger.info(taxonomy_hash_by_tax_id[value])
        if value in taxonomy_hash_by_tax_id:
            return taxonomy_hash_by_tax_id[value]['lineage']

        logger.debug(f"warning  taxonomy_hash_by_tax_id: {value} does not exist")
        return ""

    def tax_lineage_lookup(value):
        # logger.info(taxonomy_hash_by_tax_id[value])
        if value in taxonomy_hash_by_tax_id:
            return taxonomy_hash_by_tax_id[value]['tax_lineage']

        logger.debug(f"warning  taxonomy_hash_by_tax_id: {value} does not exist")
        return ""

    def scientific_name_lookup(value):
        # logger.info(taxonomy_hash_by_tax_id[value])
        if value in taxonomy_hash_by_tax_id:
            return taxonomy_hash_by_tax_id[value]['scientific_name']

        logger.debug(f"warning  taxonomy_hash_by_tax_id: {value} does not exist")
        return ""

    if 'lineage' in df:
        logger.info(f"Already have the taxonomic columns, so can forgo this again")
        return df
    else:
        tax_id_list = df['tax_id'].unique()
        taxonomy_hash_by_tax_id = create_taxonomy_hash_by_tax_id(tax_id_list)
        df['scientific_name'] = df['tax_id'].apply(scientific_name_lookup)
        df['lineage'] = df['tax_id'].apply(lineage_lookup)
        df['tax_lineage'] = df['tax_id'].apply(tax_lineage_lookup)
        return df

def taxonomic_filter(df, taxonomy_to_filter):
    """

    :param taxonomy_to_filter:
    :param df:
    :return: df
    """

    def do_the_actual_filtering(my_df, my_taxonomy_to_filter):
        my_df = add_taxonomy_columns(my_df)
        start_total = len(my_df)
        sample_num = 3
        logger.info(f"samples to analyse({sample_num}) = {df.lineage.sample(sample_num).to_string()}")
        df['lineage_list'] = df['lineage'].str.split(';', expand = False).copy()
        if my_taxonomy_to_filter == "Fungi":

            logger.info(f"\n{df.sample(2)}")
            df['lineage_1'] = df['lineage_list'].str[0]
            df['lineage_2'] = df['lineage_list'].str[1]
            df['lineage_3'] = df['lineage_list'].str[2]
            my_df = my_df.query('lineage_2 == @my_taxonomy_to_filter')
        else:
            my_df = my_df.query('lineage.str.contains(@my_taxonomy_to_filter)')
        end_total = len(my_df)
        logger.info(f"before filtering for {my_taxonomy_to_filter}, start_total={start_total} after: end_total={end_total}")
        return my_df

    if taxonomy_to_filter == "fungi":
        df = do_the_actual_filtering(df, "Fungi")
    else:
        sys.exit(f"unknown taxonomy filter of {taxonomy_to_filter}")

    return df


def taxonomic_analysis(df):
    """
    Doing much taxonomic analysis
    :param df:
    :return:
    """




    logger.info("About to analyse_environment")
    #analyse_environment(df)
    logger.info("RETURNED FROM analyse_environment")
    df = add_taxonomy_columns(df)

    df['lineage_list'] = df['lineage'].str.split(';', expand=False).copy()
    logger.info(f"\n{df.sample(2)}")

    df['lineage_1'] = df['lineage_list'].str[0]
    df['lineage_2'] = df['lineage_list'].str[1]
    df['lineage_3'] = df['lineage_list'].str[2]
    df['lineage_8'] = df['lineage_list'].str[8]
    df['lineage_9'] = df['lineage_list'].str[9]

    logger.info(f"\n{df.sample(3)}")
    print_value_count_table(df.lineage_2)

    # print_value_count_table(df.lineage_3)
    df['lineage_minus2'] = df['lineage_list'].str[-2] # as the lineage ends ;$
    df['lineage_minus3'] = df['lineage_list'].str[-3]
    print("lineage_minus2")
    print_value_count_table(df.lineage_minus2)
    print("lineage_minus3")
    print_value_count_table(df.lineage_minus3)
    tax_id_list = df['tax_id'].unique()
    logger.info(len(tax_id_list))

    logger.info(f"\n{df.sample(3)}")

    path_list = ['lineage_2', 'lineage_minus2', 'scientific_name']
    plot_df = df.groupby(path_list).size().to_frame('record_count').reset_index()
    plotfile = "../images/taxonomic_analysis_sunburst.png"
    plot_sunburst(plot_df, 'Figure: ENA "Environmental" readrun records, tax lineage(select)', path_list, 'record_count', plotfile)
    logger.info("-----------------------------------------------------------------------------------------------------")
    path_list = ['lineage_1', 'lineage_minus3', 'lineage_minus2', 'scientific_name', 'lineage']
    plot_df = df.groupby(path_list).size().to_frame('record_count').reset_index()
    logger.info(f"\n{plot_df.head(3)}")
    logger.info(f"\n{plot_df['lineage_1'].value_counts()}")
    plot_df = plot_df[plot_df['lineage_1'] == 'Eukaryota']
    logger.info(f"\n{plot_df.head(3)}")
    obj_print_and_display_md(plot_df,   "ena_lineage_eukaryota")
    path_list = ['lineage_minus3', 'lineage_minus2', 'scientific_name']
    plotfile = "../images/taxonomic_analysis_euk_sunburst.png"
    plot_sunburst(plot_df, 'Figure: ENA "Environmental" readrun records, tax lineage(Euk)', path_list,
                  'record_count', plotfile)

    path_list = ['lineage_2', 'lineage_minus3', 'lineage_minus2', 'scientific_name', 'lineage']
    plot_df = df.groupby(path_list).size().to_frame('record_count').reset_index()
    plot_df = plot_df[plot_df['lineage'].str.contains('Vertebrata')]
    obj_print_and_display_md(plot_df, "ena_lineage_vertebrata")
    plotfile = "../images/taxonomic_analysis_vertebrata_sunburst.png"
    plot_sunburst(plot_df, 'Figure: ENA "Environmental" readrun records, Vertebrata', path_list,
              'record_count', plotfile)

    path_list = ["lineage_9"]
    plot_df = df[df['lineage'].str.contains('Vertebrata')]
    plot_df = plot_df.groupby(path_list).size().to_frame('record_count').reset_index().sort_values(by='record_count', ascending=False)
    obj_print_and_display_md(plot_df, "ena_lineage_vertebrata")

    path_list = ['library_source', 'library_strategy', 'lineage_1']
    plot_df = df.groupby(path_list).size().to_frame('record_count').reset_index()
    plotfile = "../images/experimental_analysis_strategy_tax.png"
    sankey_link_weight = 'record_count'
    plot_sankey(plot_df, sankey_link_weight, path_list, 'Figure ENA "Environmental" readrun record count: library_source, library_strategy & tax', plotfile)

    path_list = ['lineage_2', 'lineage_minus3', 'lineage_minus2', 'scientific_name', 'lineage']
    plot_df = df.groupby(path_list).size().to_frame('record_count').reset_index()
    plot_df = plot_df[plot_df['lineage'].str.contains('Fungi')]

    path_list = ['lineage_minus3', 'lineage_minus2', 'scientific_name']
    plotfile = "../images/taxonomic_analysis_fungi_sunburst.png"
    plot_sunburst(plot_df, 'Figure: ENA "Environmental" readrun records, Fungi', path_list,
              'record_count', plotfile)

    # plot_df = plot_df.sort_values(by='record_count', ascending=False)
    # print(plot_df.to_markdown(index=False))

    # commented as failing for aquatic... but works for fungi
    # path_list = ['lineage_3', 'lineage_4', 'lineage_minus2', 'scientific_name']
    # plot_df = df.groupby(path_list).size().to_frame('record_count').reset_index()
    # logger.info(f"\n{plot_df.sample(30)}")
    # plot_df = plot_df[plot_df['lineage_3'].str.contains('Fungi')]
    # plotfile = "../images/taxonomic_analysis_fungi_l3_sunburst.png"
    # plot_sunburst(plot_df, 'Figure: ENA "Environmental" readrun records, Fungi l2', path_list,
    #           'record_count', plotfile)

    return df


def de_list_col(my_list):
    """
    deconvolution of a list of list
    :param my_list:
    :return: list
    """
    gene_list = []
    for gene_row_list in my_list:
        for gene in gene_row_list:
             gene_list.append(gene)
    return gene_list

def get_barcoding_genes(value):
        """
        method to get barcoding genes
        it is typically run as an apply on a dataframe
        It is used for both analysing the target_genes from the defined metadata and the text in the study description.
        :param value: a string of one or more barcoding gene names
        :return: a set of cleaner values
        """

        sgenes_pattern = re.compile(r'^([1-9][0-9]{1,2}|5\.8)([sS])[ ]?r?(RNA|DNA|ribo)?', flags=0)
        rbcl_pattern = re.compile(r'^(RBCL)', re.IGNORECASE)
        its_pattern = re.compile(r'^(ITS)([1-2])?')
        matk_pattern = re.compile(r'^(matk)', re.IGNORECASE)
        COX1_pattern = re.compile('^COX1|CO1|COI|mtCO|Cytochrome c oxidase|cytochrome oxidase', re.IGNORECASE)
        def clean_name(my_list):
            """
             a clean harmonised list of barcoding gene names
            :param my_list:    # list of gene names:
            :return: harmonised list.
            """
            clean_set = set()
            #logger.info("-----------------------------------------------------------")
            for my_gene in my_list:
                #logger.info(my_gene)

                match = re.search(rbcl_pattern, my_gene)
                if match:
                    # logger.info(f"----------clean=rbcL")
                    clean_set.add("rbcL")
                    continue
                match = re.search(its_pattern, my_gene)
                if match:
                    if match.group(2):
                        # logger.info(f"----------clean=ITS{match.group(2)}")
                        clean_set.add("ITS" + match.group(2))
                    else:
                        # logger.info(f"----------clean=ITS")
                        clean_set.add("ITS")
                    continue
                match = re.search(matk_pattern, my_gene)
                if match:
                        # logger.info("----------clean=matK")
                        clean_set.add("matK")
                        continue
                match = re.search(COX1_pattern, my_gene)
                if match:
                        # logger.info("----------clean=COX1")
                        clean_set.add("COX1")
                        continue

                match = re.search(sgenes_pattern, my_gene)
                if match:
                    # logger.info(match.group(1))
                    # logger.info(match.group(2))
                    clean_gene_name = match.group(1) + "S"
                    if match.group(3):
                        # logger.info(f"---------------{match.group(3)}")
                        clean_gene_name += " r" + match.group(3)
                    clean_set.add(clean_gene_name)
                    # logger.info(clean_gene_name)
                    continue

                logger.warning(f"remaining gene in get_barcoding_genes -->{my_gene}<--")
                # sys.exit("exiting due to error above")


            return clean_set
        barcode_genes_pattern = re.compile(r'16[sS][ ]?r?[RD]NA|16[sS][ ]?ribo|12S|18S|ITS[12]?|26[Ss]|5.8[Ss]|rbcL|rbcl|RBCL|matK|MATK|cox1|co1|COX1|CO1|COI|mtCO|cytochrome c oxidase|cytochrome oxidase')
        genes = list(set(re.findall(barcode_genes_pattern, value)))
        if len(genes) > 0:
            # logger.info(genes)
            return list(clean_name(genes))
        else:
            genes = list(set(re.findall(r'16[Ss]', value)))
            if len(genes) > 0:
                return list(clean_name(genes))
            return None

def analyse_barcode_study_details(df):
    """
    Generates a subset of the df, indexed from sample_accession
    Plus annotates two columns using
    barcoding_df['barcoding_genes_from_study'] = list of genes
    barcoding_df['is_barcoding_experiment_probable'] = True

    :param df:
    :return: df: with extra annotations
    """
    dff = get_filtered_study_details(df)
    logger.info(f"in analyse_all_study_details for study_total={len(dff)} total unique study_accession={dff['study_accession'].nunique()}")
    logger.info(dff.columns)
    barcoding_pattern = '12S|16S|18S|ITS|26S|5.8S|RBCL|rbcL|matK|MATK|COX1|CO1|mtCO|barcod'
    barcoding_title_df = dff[dff.study_title.str.contains(barcoding_pattern, regex= True, na=False)]
    logger.info(f"'study_title' with barcoding genes total={len(barcoding_title_df)}")
    logger.debug(barcoding_title_df['study_title'].sample(n=3))

    barcoding_description_df = dff[dff.study_description.str.contains(barcoding_pattern, regex= True, na=False)]
    logger.info(f"'study_description' with barcoding genes total={len(barcoding_description_df)}")
    logger.debug(barcoding_description_df['study_description'].sample(n=3))

    # This will cope with the obvious use cases: including where genes may be in title, but not description
    barcoding_df = pd.concat([barcoding_title_df, barcoding_description_df]).drop_duplicates().reset_index(drop=True)
    logger.info(f"barcoding total = {len(barcoding_df)}")
    barcoding_df['combined_tit_des'] = barcoding_df['study_title'] + barcoding_df['study_description']
    barcoding_df['is_barcoding_experiment_probable'] = True

    barcoding_df['barcoding_genes_from_study'] = barcoding_df.combined_tit_des.apply(get_barcoding_genes)
    logger.debug(barcoding_df['barcoding_genes_from_study'].value_counts())
    print_value_count_table(barcoding_df['barcoding_genes_from_study'])

    # merge all the findings back into the main
    df = pd.merge(df, barcoding_df[['study_accession','barcoding_genes_from_study','is_barcoding_experiment_probable']], on='study_accession', how='left')
    # df[['barcoding_genes_from_study']].fillna(value=[], inplace=True)
    df.loc[df['barcoding_genes_from_study'].isnull()] = df.loc[df['barcoding_genes_from_study'].isnull()].apply(lambda x: [])

    logger.info("---The following are all whole filtered dataframe, not by study----")
    total = len(df)
    present_count, absent_count = get_presence_or_absence_col(df, 'barcoding_genes_from_study')
    logger.info(f"barcoding_genes_from_study present_count {present_count}  {present_count/total*100:.2f}%")
    logger.info(f"barcoding_genes_from_study absent_count {absent_count}   {absent_count/total*100:.2f}%")

    print_value_count_table(df['barcoding_genes_from_study'])
    df = df[['is_barcoding_experiment_probable']].fillna(value = False)
    logger.info(f"len of def being returned is {len(df)}")
    logger.info(df['is_barcoding_experiment_probable'].value_counts())
    logger.info("-------------------------------------------------------------------------------------------")
    return df
    
def add_insdc_member_receiver(df):
    logger.info("adding insdc member receiver")
    #df = df.sample(n=100)
    logger.debug(df.dtypes)
    def get_insdc_member_receiver(value):
        if value.startswith('SAMN'):
            return 'NCBI'
        elif value.startswith('SAME'):
            return 'ENA'
        elif value.startswith('SAMD'):
            return 'DDBJ'
        else:
            return None

    df['insdc_member_receiver'] = df['sample_accession'].apply(get_insdc_member_receiver)
    logger.debug("exiting add_insdc_member_receiver")
    return df


def do_geographical(df):
    """

    :param df:
    :return:
    """
    df = process_geographical_data(df)
    logger.info(f"after process_geographical_data count: {len(df)}")

    # logger.info(f"after process_geographical_data total: {df['country_clean'].value_counts()}")
    # logger.info(f"after process_geographical_data total: {df['country'].value_counts()}")
    print_value_count_table(df.country_clean)


    print_value_count_table(df.continent)

    tmp_df = df[df['ocean'] != 'not ocean']
    logger.info(f"after process_geographical_data count: {len(tmp_df)}")
    print("Oceans Count and Percentage")
    print_value_count_table(tmp_df.ocean)

    path_list = ['continent', 'country_clean']
    plot_df = df.groupby(path_list).size().to_frame('record_count').reset_index()
    plot_df = plot_df.sort_values(by=['record_count'], ascending=False)
    logger.info(f"after process_geographical_data count: {len(plot_df)}")
    plotfile = "../images/geography_sunburst.png"
    logger.info(f"plotting\n{plotfile}")
    plot_sunburst(plot_df, 'Figure: ENA Aquatic "Environmental" readrun records, by country', path_list,
                  'record_count', plotfile)

    tmp_df = plot_df[['country_clean', 'record_count']]
    logger.info(f"\n{tmp_df.head(20).to_string(index=False)}")
    #print_value_count_table(df.country_clean)
    country_record_count_dict = dict(zip(plot_df.country_clean, plot_df.record_count))


    plot_countries(country_record_count_dict, 'europe', "Reported eDNA related ALL readrun in Europe Frequencies",
               "../images/ena_european_countries.png")


    #for key in country_record_count_dict.keys():
    US_KEY = "United States of America"
    if US_KEY in country_record_count_dict:
        # logger.info(f"GREAT: -->{US_KEY}<-- found")
        # country_record_count_dict["USA"] = country_record_count_dict[US_KEY]
        country_record_count_dict["United States"] = country_record_count_dict[US_KEY] # by trial and error found this "std" is expected

    plot_countries(country_record_count_dict, 'all', "Reported ENA Aquatic eDNA related ALL readrun in World Frequencies",
                   "../images/ena_all_countries.png")

    path_list = ['ocean']
    plot_df = df.groupby(path_list).size().to_frame('record_count').reset_index()
    plot_df = plot_df[plot_df['ocean'] != 'not ocean']
    plotfile = "../images/ocean_sunburst.png"
    logger.info(f"plotting {plotfile}")
    plot_sunburst(plot_df, 'Figure: ENA Aquatic "Environmental" readrun records, by ocean', path_list,
                  'record_count', plotfile)

    return df


def collection_date_year(value):
    """
    Trys hard to find the year of collection date
    :param value:  could be 2 years or 4 years format
    :return: 4 digit year as a string
    """
    def predict_year(value):
        # want to return 4 digit year as string
        # and removes excessive years

        if value.isdigit():
            value = int(value)
        else:
            value = value.strip()
            match = re.findall(r'[0-9]+$', value)
            # print(match)
            if len(match) == 1:
                value = int(match[0])
                # print(f" but choosing -->{value}<--")
            else:
                print(f"The string -->{value}<-- cannot be easily converted to an integer.")
                return None

        if value >= 100:
            if value > 2025:
                return ""
            else:
                return str(value)
        elif value > 50:
            return '19' + str(value)
        else:
            return '20' + str(value)
    # print(value)
    if value == "":
        return ""
    elif re.search("^missing|Missing|^not|^[Nn][Aa]|^N/A|n/a|restricted access|^Not|^NOT|^unknown|^Unk|^UNK|none|^-$", value):
        return ""
    elif re.search("^[0-9]{4}$", value):

        logger.debug(f"extract_value pat=3 for {value}")
        return predict_year(value)
    elif re.search("^[0-9]{4}[/-]", value):
        extract_value = value[0:4]
        logger.debug(f"extract_value pat=4 {extract_value}")
        return predict_year(extract_value)
    elif re.search("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}", value):
        extract_value = value.split("/")[2]
        match = re.search("^[0-9]{2,4}", extract_value)   # having to cope with spaces, hyphens etc.
        extract_value = match.group(0)
        logger.debug(f"extract_value pat=5 for {extract_value}")
        return predict_year(extract_value)
    elif re.search(r"^[0-9]{1,2}.[0-9]{1,2}\.[0-9]{2,4}", value):
        extract_value = value.split(".")[2]
        logger.debug(f"extract_value pat=6 for {extract_value}")
        return predict_year(extract_value)
    elif re.search("[0-9]{4}$", value):
        extract_value = value[-4:]
        logger.debug(f"extract_value pat=7 for >>>>{extract_value}<<<<")
        return predict_year(extract_value)
    elif re.search("^[0-9]{4}$", value):
        extract_value = re.findall("^[0-9]{4}", value)[0]
        logger.debug(f"extract_value pat=8 for {extract_value}")
        return predict_year(extract_value)
    elif re.search("[0-2][0-9]$", value):
        extract_value = value[-2:]
        logger.debug(f"extract_value pat=9 for {extract_value}")
        return predict_year(extract_value)
    elif re.search("^[0-9]{1,2}-[A-Za-z]{1,12}-[0-9]{2,4}", value):
        extract_value = value.split("-")[2]
        extract_value = extract_value.split(" ")[0] # might have white space
        logger.debug(f"extract_value pat=10 for {extract_value}")
        predict_year(extract_value)
    elif re.search(" [12][0-9]{3} ", value):
        match = re.search(" [12=][0-9]{3} ", value)
        extract_value = match.group(0)
        logger.debug(f"extract_value pat=11 for {extract_value}")
        return predict_year(extract_value)
    else:
        # logger.info(f"no year match for -->{value}<--")  # e,g,  f"no year match for {value}": 'no year match for restricted access'
        return ""

def create_year_bins(value):
    """
    trying to bin years in 5 year
    :param value:
    :return:
    """
    min_year=1950
    max_year=2025
    if isinstance(value, int):
        if value <= min_year:
            return str(min) + "-pre"
        for x in range(min_year, max_year, 5):
            # 2023 far more likely than min so could try reversing the order
            # logger.info(value)
            if value <= x:
               return f"{str(x)}-{str(x+5)}"
    return None

def detailed_environmental_analysis(df):
    """
    uses the tags to predict the environment, is rather rough and ready
    assuming is all terrestrial with a low confidence
    :param df:
    :return: df        with the addition of ['env_prediction']  ['env_prediction_hl']  ['env_confidence']
    """
    logger.info(len(df))
    def process_env_tags(value):
        my_tag_list = value.split(';')
        my_env_tags = [s for s in my_tag_list if "env_" in s]
        return my_env_tags

    print_value_count_table(df.tag)
    # logger.info(df.tag.head(50))
    df['env_tag'] = df['tag'].apply(process_env_tags)
    df['env_tag_string'] = df['env_tag'].apply(lambda x: ';'.join(x))
    # logger.info(df['env_tag'].value_counts().head(5))

    def is_w_env_tags(value_list):
        if len(value_list) == 0:
            return False
        return True

    # tmp_df = cp_df[len(cp_df.env_tag)> 0]
    df['is_env_tags'] = df['env_tag'].apply(is_w_env_tags)
    logger.debug(f"{df['env_tag'].value_counts().head()}")
    logger.info(f"{df.columns}")
    tmp_df = df[df['is_env_tags'] == True]
    # print_value_count_table(tmp_df.env_tag)
    logger.debug(tmp_df['env_tag'].value_counts().head(5))
    logger.debug(tmp_df['env_tag'].explode().unique())
    # tmp_df['env_tag_string'] = tmp_df['env_tag'].apply(lambda x: ';'.join(x))
    # tmp_df['env_tag_string'] = tmp_df['env_tag'].str.join(';')
    # logger.info(tmp_df['env_tag_string'].unique())
    # logger.info(my_env_lists['env_tag'])
    #  for tag in tmp_df['env_tag'].unique():
    #      logger.info(tag)
    logger.info(f"starting len={len(df)} filtered len={len(tmp_df)}")

    tag_string_assignment = {}
    # f = tmp_df['env_tag_string'].str.contains("env_geo",na=False)

    logger.info("++++++++++++++++++++++++++++++++++++++++++++++++")
    not_assigned = []
    multiples = []
    aquatic_tag_set = ['env_geo:marine', 'env_geo:freshwater', 'env_geo:brackish', 'env_geo:coastal', 'env_tax:marine',
                        'env_tax:freshwater', 'env_tax:brackish', 'env_tax:coastal']
    terrestrial_tag_set = ['env_geo:terrestrial', 'env_tax:terrestrial']
    for tags in tmp_df['env_tag_string'].unique():
        logger.debug(tags)
        tag_list = tags.split(';')

        if 'env_geo' in tags:
            # logger.info(f"----------------------{tags}")
            matches = re.findall(r'env_geo[^;]*', tags)
            # logger.info(matches)
            if len(matches) > 1:
                msg = f"WARNING, multiple GEO matches={matches}, tags={tags} THAT IS NOT YET HANDLED"
                if 'env_geo:coastal' in matches and 'env_geo:marine' in matches:
                    if len(tag_list) == 2:
                        tag_string_assignment[tags] = {'prediction': 'coastal', 'confidence': 'medium'}
                    elif 'env_tax:marine' in tags or 'env_tax:coastal' or 'env_tax:brackish' in tags:
                        tag_string_assignment[tags] = {'prediction': 'coastal', 'confidence': 'high'}
                    else:
                        logger.debug(msg)
                        multiples.append(msg)
                elif 'env_geo:terrestrial' in tags:
                    if 'env_geo:freshwater' in tags:
                        tag_string_assignment[tags] = {'prediction': 'freshwater', 'confidence': 'low'}
                    elif 'env_geo:coastal' in tags:
                        tag_string_assignment[tags] = {'prediction': 'terrestrial', 'confidence': 'medium'}
                    else:
                        multiples.append(msg)
                elif 'env_geo:marine' in tags:
                        if 'env_tax:marine' in tags:
                            tag_string_assignment[tags] = {'prediction': 'marine', 'confidence': 'medium'}
                        elif 'env_geo:freshwater' in tags and 'env_tax:freshwater' in tags:
                            tag_string_assignment[tags] = {'prediction': 'freshwater', 'confidence': 'medium'}
                        elif 'env_geo:freshwater' in tags:
                            tag_string_assignment[tags] = {'prediction': 'brackish', 'confidence': 'low'}
                        else:
                            multiples.append(msg)
                else:
                    logger.debug(matches)
                    multiples.append(msg)
            else:  # i.e. one match
                if matches[0] == 'env_geo:marine' and 'env_tax:marine' in tags:
                    tag_string_assignment[tags] = {'prediction': 'marine', 'confidence': 'high'}
                elif matches[0] == 'env_geo:freshwater' and 'env_geo:freshwater' in tags:
                    tag_string_assignment[tags] = {'prediction': 'freshwater', 'confidence': 'high'}
                elif matches[0] == 'env_geo:coastal' and 'env_geo:coastal' in tags:
                    tag_string_assignment[tags] = {'prediction': 'coastal', 'confidence': 'high'}
                elif matches[0] == 'env_geo:brackish' and 'env_geo:brackish' in tags:
                    tag_string_assignment[tags] = {'prediction': 'brackish', 'confidence': 'high'}
                elif matches[0] == 'env_geo:terrestrial' and 'env_geo:terrestrial' in tags:
                    tag_string_assignment[tags] = {'prediction': 'terrestrial', 'confidence': 'high'}
                elif len(tag_list) == 2 and 'env_geo:terrestrial' not in tags:
                    if tag_list[0] in aquatic_tag_set and tag_list[1] in aquatic_tag_set:
                        tag_string_assignment[tags] = {'prediction': 'mixed_aquatic', 'confidence': 'medium'}
                    elif (tag_list[0] in terrestrial_tag_set and tag_list[1] in aquatic_tag_set) or (tag_list[1] in terrestrial_tag_set and tag_list[0] in aquatic_tag_set):
                            tag_string_assignment[tags] = {'prediction': 'mixed', 'confidence': 'low'}
                elif len(tag_list) == 3 and 'env_geo:terrestrial' not in tags:
                    if tag_list[0] in aquatic_tag_set and (tag_list[1] in aquatic_tag_set or tag_list[2] in aquatic_tag_set):
                        tag_string_assignment[tags] = {'prediction': 'mixed_aquatic', 'confidence': 'medium'}
                    else:
                        tag_string_assignment[tags] = {'prediction': 'mixed', 'confidence': 'low'}
                else:
                    logger.debug("________________________________________________________")
                    logger.debug(matches[0])
                    if len(tag_list) == 1:
                        value = re.findall(r'env_geo:(.*)', matches[0])[0]
                        tag_string_assignment[tags] = {'prediction': value, 'confidence': 'medium'}
                    elif matches[0] == 'env_geo:coastal' and "brackish" in tags:
                        tag_string_assignment[tags] = {'prediction': 'coastal', 'confidence': 'medium'}
                    elif matches[0] == 'env_geo:marine' and ("brackish" in tags or "coastal" in tags):
                        tag_string_assignment[tags] = {'prediction': 'coastal', 'confidence': 'medium'}
                    elif matches[0] == 'env_geo:marine' and ("terrestrial" in tags):
                        tag_string_assignment[tags] = {'prediction': 'mixed', 'confidence': 'low'}
                    else:
                        if re.match(r'^(env_tax:freshwater;env_geo:marine|env_tax:freshwater;env_tax:terrestrial;env_geo:marine)$',tags):
                            not_assigned.append(tags)
                        else:
                            logger.error(f"Not assigned--->{tags} len_tags={len(tag_list)}")
                            sys.exit(f"Not assigned--->{tags} len_tags={len(tag_list)}")

        # the following are where there are no env_geo: tgs
        elif len(tag_list) == 1:
            value = re.findall(r'env_tax:(.*)', tag_list[0])[0]
            tag_string_assignment[tags] = {'prediction': value, 'confidence': 'medium'}
        elif len(tag_list) == 2:
            if tag_list[0] in aquatic_tag_set and tag_list[1] in aquatic_tag_set:
                tag_string_assignment[tags] = {'prediction': 'mixed_aquatic', 'confidence': 'medium'}
            elif tag_list[0] in aquatic_tag_set and tag_list[1] in aquatic_tag_set:
                tag_string_assignment[tags] = {'prediction': 'mixed', 'confidence': 'low'}
            elif tag_list[0] in aquatic_tag_set and tag_list[1] in terrestrial_tag_set:
                tag_string_assignment[tags] = {'prediction': 'mixed', 'confidence': 'low'}
            elif ((tag_list[0] in terrestrial_tag_set and tag_list[1] in aquatic_tag_set) or
                  (tag_list[1] in terrestrial_tag_set and tag_list[0] in aquatic_tag_set)):
                tag_string_assignment[tags] = {'prediction': 'mixed', 'confidence': 'low'}
            else:
                not_assigned.append(tags)
        elif len(tag_list) == 3:
                if tag_list[0] in aquatic_tag_set and (
                        tag_list[1] in aquatic_tag_set or tag_list[2] in aquatic_tag_set):
                    tag_string_assignment[tags] = {'prediction': 'mixed_aquatic', 'confidence': 'low'}
                elif tag_list[1] in aquatic_tag_set and (
                        tag_list[2] in aquatic_tag_set):
                    tag_string_assignment[tags] = {'prediction': 'mixed_aquatic', 'confidence': 'low'}
                elif tag_list[0] in terrestrial_tag_set and (
                            tag_list[1] in aquatic_tag_set and tag_list[2] in aquatic_tag_set):
                    tag_string_assignment[tags] = {'prediction': 'mixed', 'confidence': 'low'}
                else:
                    not_assigned.append(tags)
        elif len(tag_list) == 4:
                if (tag_list[0] in aquatic_tag_set or tag_list[1] in aquatic_tag_set) and (
                        tag_list[2] in aquatic_tag_set or tag_list[3] in aquatic_tag_set):
                    tag_string_assignment[tags] = {'prediction': 'mixed_aquatic', 'confidence': 'low'}
                else:
                    not_assigned.append(tags)
        else:
            not_assigned.append(tags)
            logger.debug(f"Not assigned--->{tags} len_tags={len(tag_list)}")
    # END OF FOR
    logger.info("finished big for loop")

    # logger.info(tag_string_assignment)
    if len(multiples) > 0:
        logger.error("Apologies: you need to address these cases before proceeding")
        logger.error(f"multiples:{multiples}")
        logger.error(f"not_assigned: {not_assigned}")
        sys.exit("not_assigned:")
    elif len(not_assigned) > 0:
        logger.error("Apologies: you need to address these cases before proceeding")
        logger.error(f"not_assigned: {not_assigned}")
        sys.exit("not_assigned")

    # logger.info('env_tax:freshwater;env_tax:terrestrial;env_geo:marine')
    # tmp_df = df[df['env_tags'].str.contains('env_tax:freshwater;env_tax:terrestrial;env_geo:marine')]
    # logger.info(tmp_df['sample_accession'].unique())
    logger.info("about to do a bunch of assignments")

    def actually_assign_env_info_pred(value):
        # logger.info(value)
        if len(value) > 1:
            # return tag_string_assignment[value]['prediction'], tag_string_assignment[value]['confidence']
            return tag_string_assignment[value]['prediction']
        return "terrestrial_assumed"

    def actually_assign_env_info_conf(value):
        # logger.info(value)
        if len(value) > 1:
            # return tag_string_assignment[value]['prediction'], tag_string_assignment[value]['confidence']
            return tag_string_assignment[value]['confidence']
        return "low"

    def add_ocean_evidence(vec):
        env_prediction = vec[0]
        ocean_evidence = vec[1]
        if ocean_evidence != "not ocean" and env_prediction in ['terrestrial', 'mixed']:
            logger.debug(f"ocean_evidence: {ocean_evidence}")
            return "marine"
        return env_prediction

    aquatic_set = ('marine', 'brackish', 'coastal', 'freshwater', 'mixed_aquatic')
    logger.info(f"aquatic_set: {aquatic_set}")

    def actually_assign_env_info_pred_hl(value):
        # logger.info(value)
        if value != "terrestrial_assumed" and value is not None:
            if value == "terrestrial":
                   return value
            elif value in aquatic_set:
                    return "aquatic"
            else:
                   return "mixed"
        return "terrestrial_assumed"

        #, tag_string_assignment[value]['confidence']
    logger.info(len(df))

    df['env_prediction'] = df['env_tags'].apply(actually_assign_env_info_pred)
    df['env_confidence'] = df['env_tags'].apply(actually_assign_env_info_conf)
    df['env_prediction'] = df[['env_prediction', 'ocean']].apply(add_ocean_evidence, axis=1)


    df['env_prediction_hl'] = df['env_prediction'].apply(actually_assign_env_info_pred_hl)
    print()
    tmp_df = df.groupby(['env_prediction', 'env_confidence']).size().reset_index(name = 'count')
    logger.info("\n" + tmp_df.to_string())
    obj_print_and_display_md(tmp_df, "ena_aquatic_environment_predictions")
    print()
    print_value_count_table(df['env_prediction_hl'])
    # sys.exit("'env_prediction', 'env_confidence'")
    #

    path = ['env_prediction_hl', 'env_prediction', 'env_confidence']
    value_field = 'record_count'
    plot_df = df.groupby(path).size().to_frame('record_count').reset_index()
    plotfile = "../images/env_predictions.png"
    plot_sunburst(plot_df, "Figure: ENA readrun Aquatic environmental predictions using species and lat/lons (Sunburst Plot)", path, value_field, plotfile)

    logger.info("finished All the analysis for the environmental predictions<-------------------")
    return df

def ena_checklist_annotation_add(my_df):
    mandatory_field_file = "../data/ena_in/ena_checklists_mandatory_or_not.txt"
    df_ena = pd.read_csv(mandatory_field_file, sep="\t")
    logger.info(f"\n{df_ena.head(5)}")
    #
    #
    df_group = df_ena.groupby(['CHECKLIST_NAME', 'CHECKLIST_FIELD_MANDATORY']).size().to_frame('mandatory_count').reset_index()
    logger.info(f"\n{df_group.to_markdown(index=False)}")

    df_mandatory = df_ena.loc[df_ena['CHECKLIST_FIELD_MANDATORY'] == 'Y']
    logger.info(f"\n{df_mandatory.head(5)}")
    print("---------------------------------------------------------------")
    df_group = df_mandatory.groupby(['CHECKLIST_NAME']).agg({'CHECKLIST_FIELD_NAME': ['count', list]}).reset_index()
    df_group.columns = ['CHECKLIST_NAME', 'MANDATORY_COUNT', 'FIELD_NAMES_LIST']
    df_group['FIELD_NAMES'] = df_group['FIELD_NAMES_LIST'].apply(lambda x: ';'.join(x))
    df_group = df_group.drop(['FIELD_NAMES_LIST'], axis=1)
    logger.info(f"\n{df_group.head(100).to_markdown(index=False)}")

    # ena_checklist_name
    return my_df


def analyse_checklists(df):
    logger.info(f"inside analyse_checklists")
    logger.info(f"cols={df.columns}")


    print('NCBI "checklists":')
    logger.info(f"cols={df.columns}")
    print_value_count_table(df.ncbi_reporting_standard)

    def get_df_col_count(my_df,col_name, new_cat_col_name):
        # logger.info(f"cols={my_df.columns}")
        # logger.info(f"col_name={col_name}, new_cat_col_name={new_cat_col_name}<---")
        my_df = my_df.rename(columns={col_name: new_cat_col_name})
        # logger.info(f"vals=\n{my_df.head(3).to_markdown(index=False)}")
        my_df = my_df.groupby([new_cat_col_name,'insdc_member_receiver']).size().reset_index(name = 'count')
        return my_df

    new_cat_col_name = 'package-or-checklist_name'
    df_ncbi_reporting_standard_counts = get_df_col_count(df,'ncbi_reporting_standard',  new_cat_col_name)
    logger.info(f"ncbi_reporting_standard_counts=\n{df_ncbi_reporting_standard_counts.head(5)}")

    ena_checklist_dict = get_ena_checklist_dict()
    df['ena_checklist_name'] = df['checklist'].map(ena_checklist_dict)
    df_ena_checklist_counts = get_df_col_count(df,'ena_checklist_name', new_cat_col_name)
    logger.info(f"ena_checklist_name=\n{df_ena_checklist_counts.head(5)}")

    df_combined_checklist_counts = pd.concat([df_ncbi_reporting_standard_counts, df_ena_checklist_counts]).sort_values(by=['count'], ascending=False)
    # df_combined_checklist_counts = df_combined_checklist_counts.drop(df_combined_checklist_counts[df_combined_checklist_counts["check_list_name"] == ""].index)
    logger.info(f"combined_checklist_counts=\n{df_combined_checklist_counts.head(100).to_markdown()}")
    outfile = "../data/out/aquatic_combined_insdc_checklists_read_run_counts.tsv"
    logger.info(f"Writing to: {outfile}")
    df_combined_checklist_counts.to_csv(outfile, sep='\t', index=False)

    print_value_count_table(df.ena_checklist_name)


def analyse_dates(df):
    print_value_count_table(df.collection_year)
    print_value_count_table(df.collection_year_bin)
    sample_counts = df.groupby('collection_year').size().reset_index(name = 'count')
    sample_counts['collection_year'] = sample_counts['collection_year'].astype(int)
    sample_counts = sample_counts.query('collection_year > 1949')
    sample_counts = sample_counts.query('collection_year < 2025')

    # Calculate cumulative sum of the 'count' column
    sample_counts['cumulative_count'] = sample_counts['count'].cumsum()
    sample_counts['cumulative_count_log'] = sample_counts['cumulative_count'].apply(lambda x: log(x))

    # Plot the cumulative curve, could not get the log working without hardcoding the b.
    fig = px.line(sample_counts, x = 'collection_year', y = 'cumulative_count_log',
                  title = 'ENA/INSDC Aquatic readrun collection_date Count Cumulative(log)',
                  labels = {'cumulative_count_log': 'Cumulative Count(log)', 'collection_year': 'Collection Year'})

    outfile = "../images/ena_aquatic_collection_dates.png"
    logger.info(f"Writing to {outfile}")
    fig.write_image(outfile)

def clean_df(df):
    df['lat'] = pd.to_numeric(df['lat'], errors = 'coerce')
    df['lon'] = pd.to_numeric(df['lon'], errors = 'coerce')
    return df

def analyse_readrun_detail(df):
    logger.info("in analyse_readrun_detail")
    df = clean_df(df)
    strategy_list_to_keep = ['AMPLICON', 'WGS', 'RNA-Seq', 'WGA', 'Targeted-Capture', 'ssRNA-seq', 'miRNA-Seq']
    if args.type_of_data in ["fungi"]:
        before_filter_count = len(df)
        df = taxonomic_filter(df, args.type_of_data)
        logger.info(f"Done taxonomic filtering for {args.type_of_data} now have {len(df)} / {before_filter_count}")
        strategy_list_to_keep = ['AMPLICON']
    logger.info(f"in analyse_readrun_detail strategy_list_to_keep={strategy_list_to_keep}" )
    df = filter_on_library_strategies(df, strategy_list_to_keep)

    # df['sample_accession'] = df['sample_accession'].to_string()
    logger.info("cols:{}".format(df.columns))
    logger.info(df['sample_accession'])
    df = add_insdc_member_receiver(df)
    print_value_count_table(df.insdc_member_receiver)

    # outfile = all_sample_accessions.tsv"
    # logger.info(outfile)
    # df.sample_accession.to_csv(outfile)

    analyse_checklists(df)

    # uncomment when running for real
    target_gene_analysis(df)
    df = clean_dates_in_df(df)

    analyse_dates(df)

    logger.info(f"before experimental_analysis_inc_filtering filtered: rownum={len(df)}")
    df = experimental_analysis_inc_filtering(df)
    logger.info(f"after experimental_analysis_inc_filtering filtered: rownum={len(df)}")

    # logger.info("-------------about to do geographical------------------------")
    df = do_geographical(df)

    logger.info("-------------about to do taxonomic_analysis------------------------")
    df = taxonomic_analysis(df)

    logger.info("-------------about to do detailed_environmental_analysis------------------------")
    df = detailed_environmental_analysis(df)
    logger.info("-------------end of analyse_readrun_detail------------------------")
    


def main():

    # logger.info(len(df_all_study_details))
    #
    # sample_ids = get_env_sample_ids()
    # logger.info(len(sample_ids))
    # readrun_ids = get_env_readrun_ids()
    # logger.info(len(readrun_ids))

    # get_all_study_details()

    logger.info(f"in main with args.type_of_data={args.type_of_data}")
    if args.type_of_data in ["all","fungi"]:
        pickle_file = 'env_readrun_detail_all.pickle'
    elif args.type_of_data == "aquatic":
        pickle_file = 'df_aquatic_env_readrun_detail.pickle'
    else:
        sys.exit(f"args.type_of_data is unknown = {args.type_of_data}")

    # df_all_study_details = analyse_barcode_study_details(get_all_study_details())

    df_env_readrun_detail = pd.read_pickle(pickle_file)
    # df_env_readrun_detail = df_env_readrun_detail.sample(1000000)
    logger.info(f"unpickled from {pickle_file} row total={len(df_env_readrun_detail)}")
    logger.info(f"columns={df_env_readrun_detail.columns}")
    analyse_readrun_detail(df_env_readrun_detail)


if __name__ == '__main__':
    logging.basicConfig(level = logging.INFO)
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
    parser.add_argument("-t", "--type_of_data",
                         help = "--type_of_data aquatic|all|fungi",
                         required = True)
    parser.parse_args()
    args = parser.parse_args()

    if args.debug_status:
        logger.setLevel(level = logging.DEBUG)
    else:
        logger.setLevel(level = logging.INFO)
    logger.info(prog_des)

    main()
