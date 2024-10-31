#!/usr/bin/env python3
"""Script of taxonomy.py is to taxonomy.py

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2023-09-08
__docformat___ = 'reStructuredText'
chmod a+x taxonomy.py
"""

from eDNA_utilities import logger
import os
import pickle
from ena_portal_api import ena_portal_api_call, get_ena_portal_url, chunk_portal_api_call

class taxon:
    """
        self.scientific_name
        self.tax_id
        self.tax_list
    """
    def print_summary(self):
       out_string = ""
       for property in self.get_taxon_dict():
           out_string += f"{property.ljust(30)}: {self.taxon_dict[property]}\n"

       return out_string

    def get_taxon_dict(self):
        if not hasattr(self,'taxon_dict'):
            self.taxon_dict = {
            'scientific_name': self.scientific_name,
            'tax_id': self.tax_id,
            'tag_list': self.tag_list,
            'isTerrestrial': self.isTerrestrial,
            'isMarine': self.isMarine,
            'isCoastal': self.isCoastal,
            'isFreshwater': self.isFreshwater
            }
        return self.taxon_dict


    def __init__(self, hit):
        """
        :param hit:
            # {'scientific_name': 'Chloephaga melanoptera',
            #     'tag': 'marine;marine_low_confidence;coastal_brackish;coastal_brackish_low_confidence;freshwater;freshwater_low_confidence;terrestrial;terrestrial_low_confidence',
            #     'tax_division': 'VRT',
            #     'tax_id': '8860'},
        #fails safe, but where high or medium confidence they are marked True

        """
        #intialise:
        self.scientific_name = ''
        self.tax_id = ''
        self.tag_list = []
        self.isTerrestrial = False
        self.isMarine = False
        self.isCoastal = False
        self.isFreshwater = False

        if hit['tax_id'] != "":
            self.scientific_name = hit['scientific_name']
            self.tax_id = hit['tax_id']
            self.tag_list = sorted(hit['tag'].split(';'))
        # else: #ie. create a dummy {tax_id = ''}

        for tag in self.tag_list:
           splits = tag.split("_")
           #logger.info(f"inspecting tags {splits}")
           if len(splits)==3 and splits[2] == "confidence" and (splits[1] == "medium" or splits[1] == "high"):
               #logger.info(splits[0])
               if splits[0] == "terrestrial":
                   self.isTerrestrial = True
               elif splits[0] == "marine":
                   self.isMarine = True
               elif splits[0] == "coastal":
                    self.isCoastal = True
               elif  splits[0] == "freshwater":
                    self.isFreshwater = True
               else:
                   print(f"WARNING: {splits[0]} is not yet handled for {splits[0]} for tax_id:{self.tax_id}")


class taxon_collection:
    def __init__(self, hit_list):
        self.tax_id_dict = {}
        self.tax_obj_list = []
        for hit in hit_list:
            taxon_obj = taxon(hit)
            self.tax_id_dict[taxon_obj.tax_id] = taxon_obj
            self.tax_obj_list.append(taxon_obj)

    def get_taxon_obj_by_id(self, tax_id):
        """
        N.B. copes with id="", as dummy created
        :param tax_id:
        :return:
        """
        #logger.info()
        if tax_id in self.tax_id_dict:
           #logger.info(f"YIPPEE: {tax_id} found")
           return self.tax_id_dict[tax_id]
        elif tax_id == "":
            #logger.info(f"ERROR: '{tax_id}' not found as it was blank")
            return None
        else:
            logger.info(f"ERROR: '{tax_id}' not found")
            return None

    def get_all_taxon_obj_list(self):
        return self.tax_obj_list

    def get_dummy_taxon_obj(self):
        return self.dummy_taxon_obj
    
        

    def print_summary(self):
        outstring = "*** Summary of Taxonomy Collection ***\n"
        outstring += f"\ttaxon_objects_total={len(self.get_all_taxon_obj_list())}"
        return outstring


def do_portal_api_tax_call(result_object_type, query_accession_ids, return_fields):
    """

    :param result_object_type:
    :param query_accession_ids:
    :param return_fields:
    :return: data # (as list of row hits) e.g.
    [{'scientific_name': 'root', 'tag': '', 'tax_division': 'UNC', 'tax_id': '1'},
               {'scientific_name': 'Chloephaga melanoptera',
                'tag': 'marine;marine_low_confidence;coastal_brackish;coastal_brackish_low_confidence;freshwater;freshwater_low_confidence;terrestrial;terrestrial_low_confidence',
                'tax_division': 'VRT',
                'tax_id': '8860'},
               {'scientific_name': 'Homo sapiens',
                'tag': '',
                'tax_division': 'HUM',
                'tax_id': '9606'}]
    """
    ena_api_url = get_ena_portal_url()
    ena_search_url = f"{ena_api_url}search?"
    # Define the query parameters
    #get rid of duplicates and blank
    query_accession_id_set = set(query_accession_ids)
    query_accession_id_set.discard("")
    query_accession_ids = list(query_accession_id_set)
    #logger.info(query_accession_ids)
    tax_accessions_string = ','.join(query_accession_ids)
    # "query": f"accession={sample_accession}",
    #logger.info(tax_accessions_string)
    return_fields_string = ','.join(return_fields)

    params = {
        "result": result_object_type,
        "includeAccessions": tax_accessions_string,
        "includeAccessionType": result_object_type,
        "format": "json",
        "fields": return_fields_string,
        "limit": 0
    }

    #logger.info(ena_search_url)
    my_url = ena_search_url
    # Make a GET request to the ENA API
    # logger.info(my_url)
    # logger.info(params)
    # logger.info(query_accession_ids)
    (data, response) = ena_portal_api_call(my_url, params, result_object_type, query_accession_ids)

    #  curl -X POST -H "Content-Type: application/x-www-form-urlencoded" -d 'result=taxon&fields=tax_id%2Cscientific_name%2Ctag%2Cdescription%2Ctax_division&includeAccessionType=taxon&includeAccessions=9606%2C8802%2C8888%2C1&format=tsv' "https://www.ebi.ac.uk/ena/portal/api/search"
    # curl   'https://www.ebi.ac.uk/ena/portal/api/search?result=taxon&fields=tax_id%2Cscientific_name%2Ctag%2Cdescription%2Ctax_division&includeAccessionType=taxon&includeAccessions=9606%2C8802%2C8888%2C1&format=tsv'
    return data

def clean_tax_list(mylist):
    """
    remove duff entries from list
    but adding the first id..
    :param mylist:
    :return: clean_id_list, duff_id_dict - the value puts to a the first id
    """
    new_set  = set()
    bad_id_dict = {}
    for id in mylist:
        if ';' in id:
            if id in bad_id_dict:
                continue
            new_id = id.partition(';')[0]
            if len(new_id) > 0:
                bad_id_dict[id] = new_id
                # print(f"Warning bad tax id entry, ignoring: {id} and replacing with {new_id}")
        else:
            new_set.add(id)
    return list(new_set), bad_id_dict

def create_taxonomy_hash(tax_list):
    """
        allows chunking, so can run a very long list and do in chunks (currently 500, seems to be what
    :param tax_list:
    :return: a list of hits: and a hash
    [{'scientific_name': 'root', 'tag': '', 'tax_division': 'UNC', 'tax_id': '1'},
               {'scientific_name': 'Chloephaga melanoptera',
                'tag': 'marine;marine_low_confidence;coastal_brackish;coastal_brackish_low_confidence;freshwater;freshwater_low_confidence;terrestrial;terrestrial_low_confidence',
                'tax_division': 'VRT',
                'tax_id': '8860'},
               {'scientific_name': 'Homo sapiens',
                'tag': '',
                'tax_division': 'HUM',
                'tax_id': '9606'}]
    """
    print("inside create_taxonomy_hash")
    (tax_list, bad_id_hash) = clean_tax_list(tax_list)

    tax_hash = []
    # logger.info(sample_obj_dict)
    #curl - X POST - H "Content-Type: application/x-www-form-urlencoded" - d
    # 'result=taxon&query=tax_eq(9606)%20OR%20tax_eq(1080)&fields=tax_id%2Cscientific_name%2Ctax_lineage%2Clineage&format=tsv' "https://www.ebi.ac.uk/ena/portal/api/search"
    taxonomy_rtn_fields = ['tax_id','tax_division', 'tag','scientific_name', 'tax_lineage','lineage']
    with_obj_type = "taxon"
    ena_portal_api_url = get_ena_portal_url()
    ena_search_url = f"{ena_portal_api_url}search?"
    # print(f"{ena_search_url} {with_obj_type} {taxonomy_rtn_fields} {tax_list}")

    tax_combined_data_picklefile = 'tax_combined_data_pickle'
    if os.path.isfile(tax_combined_data_picklefile):
        print(f"WARNING: am using {tax_combined_data_picklefile} which is just a subset")
        combined_data = pickle.load(open(tax_combined_data_picklefile, "rb"))
    else:
        combined_data = chunk_portal_api_call(ena_search_url, with_obj_type, taxonomy_rtn_fields, None, tax_list)
        pickle.dump(combined_data, open(tax_combined_data_picklefile, "wb"))

    return combined_data, bad_id_hash

def create_taxonomy_hash_by_tax_id(tax_list):
    """
    Also fudge fixes for many bad_tax_ids e.g. if ';'
    Example
                1352': {'lineage': 'Bacteria; Bacillota; Bacilli; Lactobacillales; '
                                   'Enterococcaceae; Enterococcus; ',
                        'scientific_name': 'Enterococcus faecium',
                        'tag': 'pathogen;pathogen:bacterium;priority;pathogen:priority;env_tax:marine',
                        'tax_division': 'PRO',
                        'tax_id': '1352',
                        'tax_lineage': '1;131567;2;1783272;1239;91061;186826;81852;1350;1352'},
    :param tax_list:
    :return:
    """

    hash_col, bad_id_hash = create_taxonomy_hash(tax_list)
    by_tax_id = {}
    for record in hash_col:
        # print(f"record = {record}")
        record['lineage'] = record['lineage'].replace("; ",";")
        by_tax_id[record['tax_id']] = record

    # trying to fix where we have "bad_ids" e.g. ids that have ';' in them
    for bad_id in bad_id_hash:
        if bad_id not in by_tax_id and bad_id in bad_id_hash:
            subst_id = bad_id_hash[bad_id]
            if subst_id in by_tax_id:
                by_tax_id[bad_id] = by_tax_id[subst_id]
            else:
                print(f"WARNING: bad_id={bad_id} not able to be handled")
        else:
            print(f"WARNING: bad_id={bad_id} not able to be handled")

    return by_tax_id



def generate_taxon_collection(tax_id_list):
    combined_data = create_taxonomy_hash(tax_id_list)
    taxon_collection_obj = taxon_collection(combined_data)
    logger.info(taxon_collection_obj.print_summary())

    return taxon_collection_obj


def main():
    tax_id_list = ['9606', '8860', '1']
    logger.info(tax_id_list)
    create_taxonomy_hash(tax_id_list)

if __name__ == '__main__':
    main()
