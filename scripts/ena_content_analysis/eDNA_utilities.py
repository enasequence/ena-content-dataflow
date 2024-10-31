#!/usr/bin/env python3
"""Script of eDNA_utilities.py contains commonyl used functions

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2024-06-14
__docformat___ = 'reStructuredText'

"""
import pickle
import logging
import coloredlogs
import random
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from collections import Counter
import pprint
import plotly.express as px
import sys
import time
import requests

logger = logging.getLogger(name = 'mylogger')

my_coloredFormatter = coloredlogs.ColoredFormatter(
    fmt='[%(name)s] %(asctime)s %(funcName)s %(lineno)-3d  %(message)s',
    level_styles=dict(
        debug=dict(color='white'),
        info=dict(color='green'),
        warning=dict(color='yellow', bright=True),
        error=dict(color='red', bold=True, bright=True),
        critical=dict(color='black', bold=True, background='red'),
    ),
    field_styles=dict(
        name=dict(color='white'),
        asctime=dict(color='white'),
        funcName=dict(color='white'),
        lineno=dict(color='white'),
    )
)

def tsv2dict(tsv_file, col1, col2):
    """
    Takes a TSV file and converts it to a dictionary.
    :param tsv_file:
    :param col1:
    :param col2:
    :return:
    """
    logger.info(f"Inside tsv2dict for {tsv_file} for key={col1} and {col2}")
    df = pd.read_csv(tsv_file, sep="\t")
    dict_data = dict(zip(df[col1], df[col2]))
    return dict_data


def get_ena_checklist_dict():
    """
    Function to get ena checklist as a dictionary
    # ena_checklist_dict = tsv2dict("ena_checklists.tsv", 'CHECKLIST_ID', 'CHECKLIST_NAME')
    # print(ena_checklist_dict)

    :return: ena_checklist_dict
    """
    ena_checklist_dict = {
        'ERC000045': 'COMPARE-ECDC-EFSA pilot food-associated reporting standard',
        'ERC000044': 'COMPARE-ECDC-EFSA pilot human-associated reporting standard',
        'ERC000026': 'EGA default checklist',
        'ERC000035': 'ENA Crop Plant sample enhanced annotation checklist',
        'ERC000041': 'ENA Global Microbial Identifier Proficiency Test (GMI PT) checklist',
        'ERC000029': 'ENA Global Microbial Identifier reporting standard checklist GMI_MDM:1.1',
        'ERC000032': 'ENA Influenza virus reporting standard checklist', 'ERC000043': 'ENA Marine Microalgae Checklist',
        'ERC000027': 'ENA Micro B3', 'ERC000037': 'ENA Plant Sample Checklist', 'ERC000042': 'ENA RNA-Seq Checklist',
        'ERC000038': 'ENA Shellfish Checklist', 'ERC000030': 'ENA Tara Oceans',
        'ERC000040': 'ENA UniEuk_EukBank Checklist', 'ERC000050': 'ENA binned metagenome',
        'ERC000011': 'ENA default sample checklist', 'ERC000034': 'ENA mutagenesis by carcinogen treatment checklist',
        'ERC000039': 'ENA parasite sample checklist', 'ERC000028': 'ENA prokaryotic pathogen minimal sample checklist',
        'ERC000036': 'ENA sewage checklist', 'ERC000033': 'ENA virus pathogen reporting standard checklist',
        'ERC000047': 'GSC MIMAGS', 'ERC000048': 'GSC MISAGS', 'ERC000049': 'GSC MIUVIGS',
        'ERC000056': 'GSC MIxS Food and Production',
        'ERC000058': 'GSC MIxS Hydrocarbon', 'ERC000057': 'GSC MIxS Symbiont', 'ERC000055': 'GSC MIxS agriculture',
        'ERC000012': 'GSC MIxS air', 'ERC000031': 'GSC MIxS built environment', 'ERC000013': 'GSC MIxS host associated',
        'ERC000014': 'GSC MIxS human associated', 'ERC000015': 'GSC MIxS human gut', 'ERC000016': 'GSC MIxS human oral',
        'ERC000017': 'GSC MIxS human skin', 'ERC000018': 'GSC MIxS human vaginal',
        'ERC000019': 'GSC MIxS microbial mat biolfilm',
        'ERC000025': 'GSC MIxS miscellaneous natural or artificial environment',
        'ERC000020': 'GSC MIxS plant associated',
        'ERC000021': 'GSC MIxS sediment', 'ERC000022': 'GSC MIxS soil', 'ERC000023': 'GSC MIxS wastewater sludge',
        'ERC000024': 'GSC MIxS water', 'ERC000052': 'HoloFood Checklist', 'ERC000051': 'PDX Checklist',
        'ERC000046': 'Pan Prostate sample checklist', 'ERC000053': 'Tree of Life Checklist'}
    return ena_checklist_dict


def clean_list_replace_nan(data):
    return ['missing' if pd.isna(item) else item for item in data]

def mv_df_col2front(df, col2move):
    df = df[[col2move] + [col for col in df.columns if col != col2move]]
    return df

def obj_print_and_display_md(obj,base_file_name):
    """
    utility method to write out an object as both markdown and tsv.
    also to print it out as md
    :param obj:
    :return:
    """
    my_markdown = obj.to_markdown()
    show_markdown = obj.head(20).to_markdown()
    print(show_markdown)
    outfile = "../tables/" + base_file_name + '.md'
    logger.info(f"writing {outfile}")
    with open(outfile, "w") as f:
        f.write('# ' + base_file_name + '\n')
        f.write(my_markdown)

    outfile = "../tables/" + base_file_name + '.tsv'
    logger.info(f"writing {outfile}")
    with open(outfile, "w") as f:
        f.write(obj.to_csv(sep="\t"))

def pickle_data_structure(data_structure, filename):
    try:
        with open(filename, "wb") as f:
            pickle.dump(data_structure, f, protocol = pickle.HIGHEST_PROTOCOL)
    except Exception as ex:
        print("Error during pickling object (Possibly unsupported):", ex)


def unpickle_data_structure(filename):
    try:
        with open(filename, "rb") as f:
            return pickle.load(f)
    except Exception as ex:
        print("Error during unpickling object (Possibly unsupported):", ex)


def run_web_requests(my_requests_get):
    logger.info("----------inside run_web_requests-------------")
    r = my_requests_get
    if r.status_code == 200:
        return r.text
    else:
        logger.info(r.status_code)
        logger.info(f"for url={r.url}")
        logger.info("retrying in 5 seconds, once")
        time.sleep(5)
        if r.status_code == 200:
            return r.text
        else:
            logger.info(f"Still {r.status_code} so exiting")
        sys.exit(1)
def run_webservice(url):
    """

    :param url:
    :return:
    """
    r = requests.get(url)
    logger.info(url)

    run_web_requests(url)

def run_webservice_with_params(base_url,params):
    """

    :param base_url:
    :param params:
    :return: the output of run_webservice

    #curl - X POST - H  "Content-Type: application/x-www-form-urlencoded" - d  'result=read_run&query=tax_eq(9606)&fields=run_accession%2Cexperiment_title%2Ctax_id&format=tsv' "https://www.ebi.ac.uk/ena/portal/api/search"
    # base_url="https://www.ebi.ac.uk/ena/portal/api/search"
    # params = 'result=read_run&query=tax_eq(9606)&fields=run_accession%2Cexperiment_title%2Ctax_id&format=tsv&limit=5'
    """
    # logger.info("----------inside run_webservice_with_params-------------")
    # logger.info(f"base_url={base_url}")
    # logger.info(f"params={params}")

    response = requests.get(base_url, params=params)
    return run_web_requests(response)


def un_split_list(my_list):
    clean_list = []
    for item in my_list:
        if item != item:
            continue
        item = item.strip().replace('[', '').replace(']', '')
        local_list = item.split(';')
        clean_list.extend(local_list)
    return clean_list

def get_shorter_list(record_list, k):
    """
    shortens a list of records randomly, if k =0 it returns all records
    :param record_list: list of records
    :param k: number of records to return
    :return: randomly chosen shorter record_list
    """
    if k == 0:
        return record_list
    elif len(record_list) < k:
        logger.warning(f"in shorten_list k={k}, len(record_list)={len(record_list)}, so returning full list")
        return record_list

    shortened_list = random.sample(record_list, k)
    logger.info(f"shortened_list to {k} from {len(record_list)}")
    return shortened_list

def get_lists_from_df_column(df, col):
    my_list = df[col].to_list()
    # logger.info(f"\n{my_list}")
    my_list = un_split_list(my_list)
    # logger.info(f"\n{my_list}")
    print(f"Total of {len(my_list)} in col={col} , unique count= {len(set(my_list))}")
    duplist = get_duplicates_in_list(my_list)
    print(f"\nDuplicated {col} list:  {duplist}")
    print(pprint.pprint(dict(Counter(my_list)), width=4))

    return my_list

def get_duplicates_in_list(mylist):
    newlist = []  # empty list to hold unique elements from the list
    duplist = []  # empty list to hold the duplicate elements from the list
    for i in mylist:
        if i not in newlist:
            newlist.append(i)
        else:
            duplist.append(i)
    return sorted(set(duplist))
    
def list_freq_pie(my_list, label, my_title, outfile):
    my_counter = Counter(my_list)
    pprint.pprint(dict(my_counter))
    my_df = pd.DataFrame(dict(my_counter).items(),
                         columns = [label, 'count']).sort_values(by = label, ascending = True)
    logger.info(f"\n{my_df.to_string(index = False)}")
    fig = px.pie(my_df, values = 'count', names = label, title = my_title)
    logger.info(f"{outfile}")
    fig.write_image(outfile)
    

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

def prepare_value_count_table(df_var):
    """

    :param df_var:
    :return: tmp_df : cat count percentage
    """
    # dropping empty or NaN strings
    df_var = df_var.replace('', np.nan)
    df_var = df_var.dropna()

    counts = df_var.value_counts()
    percs = df_var.value_counts(normalize = True)

    tmp_df = pd.concat([counts, percs], axis = 1, keys = ['count', 'percentage'])
    tmp_df['percentage'] = pd.Series(["{0:.2f}%".format(val * 100) for val in tmp_df['percentage']], index = tmp_df.index)
    return tmp_df


def print_value_count_table(df_var):
    logger.debug(f"type={type(df_var)} value={df_var}")
    logger.debug(f"--------print_value_count_table---------col_header ={df_var.name}\n{df_var.head(5)}")
    tmp_df = prepare_value_count_table(df_var)
    max_rows = 20
    tmp_df = tmp_df.head(max_rows)
    # print(tmp_df.to_string(index = False))
    obj_print_and_display_md(tmp_df,df_var.name)


def plot_sunburst(df, title, path_list, value_field, plotfile):
    """

    :param df:
    :param title:
    :param path_list:
    :param value_field:
    :param plotfile:
    :return:
    """
    fig = px.sunburst(
            df,
            path = path_list,
            values = value_field,
            title = title,
        )
    # fig.update_layout(legend = dict(
    #     orientation = 'h',
    #     yanchor = 'bottom',
    #     y = 1.02,
    #     xanchor = 'right',
    #     x = 1
    # ))
    # fig.show()
    if plotfile == "plot":
            fig.show()
    else:
            logger.info(f"Sunburst plot to {plotfile}")
            fig.write_image(plotfile)




def generate_sankey_chart_data(df, columns: list, sankey_link_weight: str):

    column_values = [df[col] for col in columns]
    # logger.info(column_values)

    # this generates the labels for the sankey by taking all the unique values
    labels = sum([list(node_values.unique()) for node_values in column_values], [])
    # logger.info(labels)

    # initializes a dict of dicts (one dict per tier)
    link_mappings = {col: {} for col in columns}

    # each dict maps a node to unique number value (same node in different tiers
    # will have different number values
    i = 0
    for col, nodes in zip(columns, column_values):
        for node in nodes.unique():
            link_mappings[col][node] = i
            i = i + 1

    # specifying which columns are serving as sources and which as sources
    # ie: given 3 df columns (col1 is a source to col2, col2 is target to col1 and
    # a source to col 3 and col3 is a target to col2
    source_nodes = column_values[: len(columns) - 1]
    target_nodes = column_values[1:]
    source_cols = columns[: len(columns) - 1]
    target_cols = columns[1:]
    links = []

    # loop to create a list of links in the format [((src,tgt),wt),(),()...]
    for source, target, source_col, target_col in zip(source_nodes, target_nodes, source_cols, target_cols):
        for val1, val2, link_weight in zip(source, target, df[sankey_link_weight]):
            links.append(
                (
                    (
                        link_mappings[source_col][val1],
                        link_mappings[target_col][val2]
                    ),
                    link_weight,
                )
            )

    # creating a dataframe with 2 columns: for the links (src, tgt) and weights
    df_links = pd.DataFrame(links, columns = ["link", "weight"])

    # aggregating the same links into a single link (by weight)
    df_links = df_links.groupby(by = ["link"], as_index = False).agg({"weight": sum})

    # generating three lists needed for the sankey visual
    sources = [val[0] for val in df_links["link"]]
    targets = [val[1] for val in df_links["link"]]
    weights = df_links["weight"]

    # logger.info(labels, sources, targets, weights)
    return labels, sources, targets, weights

def plot_sankey(df, sankey_link_weight, columns, title, plotfile):

    # list of list: each list are the set of nodes in each tier/column

    (labels, sources, targets, weights) = generate_sankey_chart_data(df, columns, sankey_link_weight)

    color_link = ['#000000', '#FFFF00', '#1CE6FF', '#FF34FF', '#FF4A46',
                  '#008941', '#006FA6', '#A30059', '#FFDBE5', '#7A4900',
                  '#0000A6', '#63FFAC', '#B79762', '#004D43', '#8FB0FF',
                  '#997D87', '#5A0007', '#809693', '#FEFFE6', '#1B4400',
                  '#4FC601', '#3B5DFF', '#4A3B53', '#FF2F80', '#61615A',
                  '#BA0900', '#6B7900', '#00C2A0', '#FFAA92', '#FF90C9',
                  '#B903AA', '#D16100', '#DDEFFF', '#000035', '#7B4F4B',
                  '#A1C299', '#300018', '#0AA6D8', '#013349', '#00846F',
                  '#372101', '#FFB500', '#C2FFED', '#A079BF', '#CC0744',
                  '#C0B9B2', '#C2FF99', '#001E09', '#00489C', '#6F0062',
                  '#0CBD66', '#EEC3FF', '#456D75', '#B77B68', '#7A87A1',
                  '#788D66', '#885578', '#FAD09F', '#FF8A9A', '#D157A0',
                  '#BEC459', '#456648', '#0086ED', '#886F4C', '#34362D',
                  '#B4A8BD', '#00A6AA', '#452C2C', '#636375', '#A3C8C9',
                  '#FF913F', '#938A81', '#575329', '#00FECF', '#B05B6F',
                  '#8CD0FF', '#3B9700', '#04F757', '#C8A1A1', '#1E6E00',
                  '#7900D7', '#A77500', '#6367A9', '#A05837', '#6B002C',
                  '#772600', '#D790FF', '#9B9700', '#549E79', '#FFF69F',
                  '#201625', '#72418F', '#BC23FF', '#99ADC0', '#3A2465',
                  '#922329', '#5B4534', '#FDE8DC', '#404E55', '#0089A3',
                  '#CB7E98', '#A4E804', '#324E72', '#6A3A4C'
                  ]

    # ---------------------------------
    fig = go.Figure(data = [go.Sankey(
        node = dict(
            pad = 15,
            thickness = 20,
            line = dict(color = "black", width = 0.5),
            # label = ["A1", "A2", "B1", "B2", "C1", "C2"],
            label = labels,
            color = "blue"
        ),
        link = dict(
            # source = [0, 1, 0, 2, 3, 3],  # indices correspond to labels, e.g. A1, A2, A1, B1, ...
            # target = [2, 3, 3, 4, 4, 5],
            # value = [8, 4, 2, 8, 4, 2]
            source = sources,
            target = targets,
            value = weights,
            color=color_link
        ))])

    fig.update_layout(title_text = title, font_size = 10)
    if plotfile == "plot":
        fig.show()
    else:
        logger.info(f"Sankey plot to {plotfile}")
        fig.write_image(plotfile)


def plot_countries(my_f_dict, location, my_title, plot_file_name):
    """
    Scope is quite limited, just europe or world
    had to hard code the ranges in to get the filters to approximate useful

    :param my_f_dict:
    :param location:
    :param my_title:
    :param plot_file_name:
    :return:
    """
    logger.debug(f"\n{my_f_dict}")
    df = pd.DataFrame(my_f_dict.items(), columns = ['country', 'count'])
    logger.debug(f"\n{df}")
    database = px.data.gapminder().query('year == 2007')

    df = pd.merge(database, df, how = 'inner', on = 'country')
    url = (
        "https://raw.githubusercontent.com/python-visualization/folium/master/examples/data"
    )

    if location == 'europe':
        scope = location
        max_colour = 25000
    else:
        scope = "world"
        max_colour = 250000
    geojson_file = f"{url}/world-countries.json"
    # see https://github.com/python-visualization/folium/tree/main/examples

    fig = px.choropleth(df,
                        title = my_title,
                        locations = "country",  # "iso_alpha",
                        locationmode = "country names",  # "ISO-3",
                        geojson = geojson_file,
                        scope = scope,
                        range_color=(0, max_colour),
                        color = "count"
                        )

    # fig.show()
    logger.info(f"\nWriting {plot_file_name}")
    fig.write_image(plot_file_name)     


def capitalise(string):
    # Capitalising any of the first words or after a white space, ignores "and" though

    if " " in string:
        mylist = []
        for sub_str in string.split(' '):
            if sub_str == 'and':
                mylist.append(sub_str)
            else:
                mylist.append(sub_str.capitalize())
        return ' '.join(mylist)
    else:
        # simpler
        return string.capitalize()
