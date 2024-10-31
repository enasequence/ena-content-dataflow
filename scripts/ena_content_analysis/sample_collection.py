#!/usr/bin/env python3
"""class of sample_collection.py is to
provide much information about a supplied list of samples

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2023-09-07
__docformat___ = 'reStructuredText'
chmod a+x sample_collection.py
"""


import sys
import os
import argparse
import random
from sample import Sample
import time
from itertools import islice
from ena_portal_api import *
from taxonomy import generate_taxon_collection, taxon
from datetime import datetime
import pandas as pd
from eDNA_utilities import logger
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

class SampleCollection:
    """
    SampleCollection class of samples
    the initialisation is quite basic, as it then get added to
    some of the methods are lazy (just in time), but do store the data structures in case used again
    """

    def __init__(self, category):
        self.type = "SampleCollection"
        self.category = category
        self.sample_obj_dict = {}
        self.environmental_sample_set = set()
        self.environmental_study_accession_set = set()
        self.european_environmental_set = set()
        self.european_sample_set = set()
        self.freshwater_sample_tag_set = set() #
        self.marine_sample_tag_set = set()
        self.terrestrial_sample_tag_set = set()
        self.coastal_brackish_sample_tag_set = set()
        self._get_aquatic_sample_acc_by_sample_tag = set() # aquatic populated on decorate_sample_tags
        self.freshwater_sample_acc_tag_set = set()
        self.marine_sample_acc_tag_set = set()
        self.coastal_brackish_sample_acc_tag_set = set()
        self.tax_id_set = set()
        self.sample_fields = ['sample_accession', 'description', 'study_accession', 'environment_biome', 'tax_id', 'taxonomic_identity_marker', 'country', 'location_start', 'location_end', 'tag']
        self.total_archive_sample_size = 0
        self._all_sample_accs_set = set()
        self._all_read_run_accs_set = set() # the read_run ids are derived from the sample_accs
        #self.total_archive_sample_size = self.get_total_archive_sample_size()

    def put_sample_set(self, sample_set):
        self.sample_set = sample_set

    def get_total_read_run_accession_set(self):
        if len(self._all_read_run_accs_set) > 0:
            return self._all_read_run_accs_set
        self._all_read_run_accs_set = get_sample_run_accessions(self.get_all_sample_acc_set())
        return self._all_read_run_accs_set

    def get_sample_accession_list(self, sample_obj_list):
        """
        written in generic way so can be re-used.
        This allows subsets, see also self.get_all_sample_accession_set()
        :param sample_obj_list:  # or set
        :return: sample_acc_list
        """
        sample_acc_list = []
        for sample_obj in sample_obj_list:
            sample_acc_list.append(sample_obj.sample_accession)
        return sample_acc_list

    def get_european_sample_accession_list(self):
        if hasattr(self, 'european_sample_accession_list'):
            return self.european_sample_accession_list
        self.european_sample_accession_list = self.get_sample_accession_list(self.european_sample_set)
        return self.european_sample_accession_list

    #freshwater_sample_tag_set
    def get_sample_tag_list(self, tag_name):
        """

        :param tag_name:  where tag name is one of  the allowable  tags
        :return:
        """
        allowable_tags = ['freshwater', 'marine', 'coastal_brackish', 'terrestrial']
        if tag_name not in allowable_tags:
            logger.info(f"Error: the tag_name {tag_name} is unknown in get_sample_tag_list")
            return []
        sample_tag_list = tag_name + '_sample_tag_list'
        if tag_name == 'freshwater' and hasattr(self, sample_tag_list):
            return self.freshwater_sample_tag_list
        elif tag_name == 'marine' and hasattr(self, sample_tag_list):
            return self.marine_sample_tag_list
        elif tag_name == 'coastal_brackish' and hasattr(self, sample_tag_list):
            return self.coastal_brackish_sample_tag_list
        elif tag_name == 'terrestrial' and hasattr(self, sample_tag_list):
            return self.terrestrial_sample_tag_list

        if tag_name == 'freshwater':
             self.freshwater_sample_tag_list = self.get_sample_accession_list(self.freshwater_sample_tag_set)
             return self.freshwater_sample_tag_list
        elif tag_name == 'marine':
            self.marine_sample_tag_list = self.get_sample_accession_list(self.marine_sample_tag_set)
            return self.marine_sample_tag_list
        elif tag_name == 'coastal_brackish':
            self.coastal_brackish_sample_tag_list = self.get_sample_accession_list(self.coastal_brackish_sample_tag_set)
            return self.coastal_brackish_sample_tag_list
        elif tag_name == 'terrestrial':
            self.terrestrial_sample_tag_list = self.get_sample_accession_list(self.terrestrial_sample_tag_set)
            return self.terrestrial_sample_tag_list


    def get_total_archive_sample_size(self):
        if hasattr(self, 'total_archive_sample_size') and self.total_archive_sample_size > 0:
            return self.total_archive_sample_size
        url='https://www.ebi.ac.uk/ena/portal/api/count?result=sample&dataPortal=ena'
        (total, response) = ena_portal_api_call_basic(url)
        self.total_archive_sample_size = total
        logger.info(self.total_archive_sample_size)
        return self.total_archive_sample_size

    def get_sample_coll_df(self):
        """
        put all into a big column orientated dict. Tried to do in a field independent way
        then generate the df.
        There is probably a more efficient way to do this.
        :return: self._sample_df
        """
        print("++++++++++++++++++++++++++++++++++++++++++++")
        if hasattr(self, '_sample_df'):
            return self._sample_df
        else:
            count = 0
            columns_list = []
            coll_dict = {}
            for sample_obj in self.sample_set:
                sample_dict = sample_obj.get_summary_dict()
                if count == 0:   # first time around
                    columns_list = sorted(sample_dict.keys())
                    for field in columns_list:
                        coll_dict[field] = []
                for field in columns_list:
                    coll_dict[field].append(sample_dict[field])
                count += 1
            self._sample_df = pd.DataFrame.from_dict(coll_dict)
            return self._sample_df

    def addSampleEnvironmentAnnotation(self):
        pass

    def addTaxonomyAnnotation(self):
        """
        # creates these
        self.tax_id_set - tax_id
        self.tax_isMarine_set - sample_obj
        self.tax_isTerrestrial_set - sample_obj
        self.tax_isCoastal_set - sample_obj
        self.tax_isFreshwater_set - sample_obj
        :return:
        """
        logger.info(len(self.sample_set))
        for sample_obj in self.sample_set:
            self.tax_id_set.add(sample_obj.tax_id)
        logger.info(len(self.tax_id_set))
        tax_id_list = sorted(self.tax_id_set)
        self.tax_isMarine_set = set()
        self.tax_isTerrestrial_set = set()
        self.tax_isCoastal_set = set()
        self.tax_isFreshwater_set = set()

        taxon_collection_obj = generate_taxon_collection(tax_id_list)
        for sample_obj in self.sample_set:
            #logger.info(sample_obj.tax_id)

            #logger.info(sample_obj.print_values())
            taxonomy_obj = taxon_collection_obj.get_taxon_obj_by_id(sample_obj.tax_id)
            if taxonomy_obj and hasattr(taxonomy_obj, 'tax_id'):
                sample_obj.taxonomy_obj = taxonomy_obj  # this is very important!
                #logger.info(sample_obj.taxonomy_obj.tax_id)
                if taxonomy_obj.isMarine:
                    #logger.info(f"\tmarine {sample_obj.taxonomy_obj.tax_id}")
                    self.tax_isMarine_set.add(sample_obj)
                if taxonomy_obj.isTerrestrial:
                    logger.info(f"\tterrestrial {sample_obj.taxonomy_obj.tax_id}")
                    self.tax_isTerrestrial_set.add(sample_obj)
                if taxonomy_obj.isCoastal:
                    logger.info("\tcoastal {sample_obj.taxonomy_obj.tax_id}")
                    self.tax_isCoastal_set.add(sample_obj)
                if taxonomy_obj.isFreshwater:
                    logger.info("\tfreshwater {sample_obj.taxonomy_obj.tax_id}")
                    self.tax_isFreshwater_set.add(sample_obj)
            else:
                #logger.info(f"Warning: for {sample_obj.tax_id} generating a dummy")
                sample_obj.taxonomy_obj = taxon({'tax_id': ''})  # generates a dummy
        #sys.exit()

    def get_sample_objs(self):
        return self.sample_set

    def get_sample_set_size(self):
        return int(len(self.sample_set))

    def get_all_sample_acc_set(self):
        if len(self._all_sample_accs_set) > 0:
            return self._all_sample_accs_set
        for sample_obj in self.get_sample_objs():
            self._all_sample_accs_set.add(sample_obj.sample_accession)
        #logger.info(self._all_sample_accs)
        return self._all_sample_accs_set

    def decorate_sample_tags(self, tag_dict):
        aquatic_tags = ['freshwater', 'marine', 'coastal_brackish']
        for tag in aquatic_tags:
            if tag == "freshwater":
                self.freshwater_sample_acc_tag_set = self.freshwater_sample_acc_tag_set.union\
                    (set(tag_dict[tag]['sample_accession']))
            if tag == "marine":
                self.marine_sample_acc_tag_set = self.marine_sample_acc_tag_set.union\
                    (set(tag_dict[tag]['sample_accession']))
            if tag == "coastal_brackish":
                 self.coastal_brackish_sample_acc_tag_set = self.coastal_brackish_sample_acc_tag_set.union\
                    (set(tag_dict[tag]['sample_accession']))

    def get_aquatic_sample_acc_by_sample_tag_set(self):
        if len(self._get_aquatic_sample_acc_by_sample_tag) > 0:
            return self._get_aquatic_sample_acc_by_sample_tag
        self._get_aquatic_sample_acc_by_sample_tag = self.freshwater_sample_acc_tag_set.union\
                        (self.marine_sample_acc_tag_set, self.coastal_brackish_sample_acc_tag_set)
        # logger.info(self._get_aquatic_sample_acc_by_sample_tag)
        return(self._get_aquatic_sample_acc_by_sample_tag)

    def get_aquatic_run_read_by_sample_tag_set(self):
        return get_sample_run_accessions(self.get_aquatic_sample_acc_by_sample_tag_set())

    def print_summary(self):
        self.get_all_sample_acc_set()  # in case this has not been run, it forces some lazy methods to be called.
        #logger.info(self.get_all_sample_accs())
        outstring = f"**** collection_obj Summary ****"
        outstring += f"On run date={datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z')}\n"
        outstring += f"sample_set_size={self.get_sample_set_size()}\n"
        outstring += f"sample_dict_size={len(self.sample_obj_dict)}\n"
        outstring += f"total_ena_sample_size={self.total_archive_sample_size}\n"
        outstring += f"total_ena_tax_id_count={len(self.tax_id_set)}\n"
        outstring += f"environmental_sample_total: {len(self.get_environmental_sample_list())}\n"
        outstring += f"European_environmental_sample_total: {len(self.european_environmental_set)}\n"
        outstring += f"European_sample_total: {len(self.european_sample_set)}\n"
        outstring += f"environmental_study_total: {len(self.get_environmental_study_accession_list())}\n"

        outstring += '#####################################\n'
        outstring += f"The following information was derived the taxonomy tags\n"
        outstring += f"  total_ena_tax_marine_count={len(self.tax_isMarine_set)}\n"
        outstring += f"  total_ena_tax_terrestrial_count={len(self.tax_isTerrestrial_set)}\n"
        outstring += f"  total_ena_tax_coastal_count={len(self.tax_isCoastal_set)}\n"
        outstring += f"  total_ena_tax_freshwater_count={len(self.tax_isFreshwater_set)}\n"

        outstring += '#####################################\n'
        outstring += f"The following information was derived the sample tags\n"
        outstring += f"  total_ena_count of samples={len(self.get_all_sample_acc_set())}\n"
        outstring += f"  total_ena_count of read_run={len(self.get_total_read_run_accession_set())}\n"
        outstring += f"  total_ena_tax_marine_count of samples={len(self.tax_isMarine_set)}\n"
        outstring += f"  total_ena_tax_terrestrial_count of samples={len(self.tax_isTerrestrial_set)}\n"
        outstring += f"  total_ena_tax_coastal_count of samples={len(self.tax_isCoastal_set)}\n"
        outstring += f"  total_ena_tax_freshwater_count of samples={len(self.tax_isFreshwater_set)}\n"

        outstring +='#####################################\n'
        sample_obj1 = random.choice(list(self.sample_set))
        # outstring += f"Random sample:\n{sample_obj1.print_values()}\n"

        # print('#####################################')
        # sample_obj2 = random.choice(list(self.sample_set))
        # outstring += f"Random sample:\n{sample_obj2.print_values()}\n"
        #
        # print('#####################################')
        # sample_obj3 = random.choice(list(self.sample_set))
        # outstring += f"Random sample:\n{sample_obj3.print_values()}\n"


        return outstring

    def get_sample_collection_stats(self):

        if hasattr(self, "sample_collection_stats_dict"):
            return self.sample_collection_stats_dict
        else:
            sample_collection_stats_dict = {'by_sample_id': {}, 'by_study_id': {}}

            for sample_obj in self.sample_set:
                sample_collection_stats_dict['by_sample_id'][sample_obj.sample_accession] = \
                    {
                        "sample_accession": sample_obj.sample_accession,
                         "study_accession": sample_obj.study_accession,
                         "is_environmental_sample": sample_obj.is_environmental_sample
                      }
                if sample_obj.is_environmental_sample:
                    # print(".", end="")
                    self.environmental_sample_set.add(sample_obj)
                    if sample_obj.country_is_european:
                        self.european_environmental_set.add(sample_obj)
                if sample_obj.country_is_european:
                        self.european_sample_set.add(sample_obj)
                # logger.info(sample_collection_stats_dict['by_study_id'])
                if sample_obj.study_accession != "":
                    for study_accession in sample_obj.study_accession.split(';'):
                      sample_collection_stats_dict['by_study_id'][study_accession] = {'sample_id': { sample_obj.sample_accession : sample_collection_stats_dict['by_sample_id'][sample_obj.sample_accession]} }
                      self.environmental_study_accession_set.add(study_accession)
                if sample_obj.sample_tag_is_freshwater:
                    self.freshwater_sample_tag_set.add(sample_obj)
                if sample_obj.sample_tag_is_terrestrial:
                    self.terrestrial_sample_tag_set.add(sample_obj)
                if sample_obj.sample_tag_is_marine:
                    self.marine_sample_tag_set.add(sample_obj)
                if sample_obj.sample_tag_is_coastal_brackish:
                    self.coastal_brackish_sample_tag_set.add(sample_obj)
                self.sample_collection_stats_dict = sample_collection_stats_dict
            self.sample_count = len(sample_collection_stats_dict['by_sample_id'])
            # logger.info(self.sample_collection_stats_dict)
        return self.sample_collection_stats_dict

    def get_environmental_sample_list(self):
        """
          list of object tagged with environment_sample in ENA
        :return:
        """
        return list(self.environmental_sample_set)

    def get_environmental_study_accession_list(self):
        return list(self.environmental_study_accession_set)

# def do_portal_api_sample_call(result_object_type, query_accession_ids, return_fields):
#     """
#
#     :param result_object_type:
#     :param query_accession_ids:
#     :param return_fields:
#     :return: data # (as list of row hits) e.g.
#        [{'description': 'Waikite Restoration Feature 5',
#             'sample_accession': 'SAMEA110453696',
#             'study_accession': 'PRJEB55115'},
#            {'description': 'Waikite Restoration Feature 3',
#             'sample_accession': 'SAMEA110453715',
#             'study_accession': 'PRJEB55115'},
#            {'description': 'Radiata Pool',
#             'sample_accession': 'SAMEA110453701',
#             'study_accession': 'PRJEB55115'}]
# ic| len(data): 3
#
# #        curl 'https://www.ebi.ac.uk/ena/portal/api/search?includeAccessions=SAMEA110453690,SAMEA110453698&result=sample&fields=sample_accession,description,study_accession,environment_biome,tax_id,taxonomic_identity_marker,country,location_start,location_end&format=json&limit=0'
#     """
#
#     result_object_type = 'sample'
#     ena_portal_api_url = get_ena_portal_url()
#     ena_search_url = f"{ena_portal_api_url}search?"
#
#     # Define the query parameters
#     sample_accessions = ','.join(query_accession_ids)
#     params = {
#         "result": result_object_type,
#         "includeAccessions": sample_accessions,
#         "format": "json",
#         "fields": return_fields,
#         "limit": 0
#     }
#     #my_url = ena_search_url + '?includeAccessions=' + sample_accessions
#     # Make a GET request to the ENA API
#     # logger.info(my_url)
#     (data, response) = ena_portal_api_call(ena_search_url, params, result_object_type, query_accession_ids)
#
#     if response.status_code != 200:
#         doze_time = 10
#         print(f"Due to response {response.status_code}, having another try for {ena_search_url} {params}, after a little doze of {doze_time} seconds")
#         time.sleep(doze_time)
#         (data, response) = ena_portal_api_call(ena_search_url, params, result_object_type, query_accession_ids)
#         if response.status_code != 200:
#             print(f"Due to response exiting {response.status_code}, tried twice")
#             logger.info()
#             sys.exit()
#
#     return data

def sample_obj_list_2_sample_acc_list(sample_list):
    sample_set = set()
    for sample_obj in sample_list:
        sample_set.add(sample_obj.sample_accession)
    return sorted(sample_set)

def get_sample_field_data(sample_list, return_fields):
    """

    :param sample_list: #are sample objects
    :param return_fields:  #ecpect a list
    :return:
    """
    with_obj_type = 'sample'
    ena_search_url = f"{get_ena_portal_url()}search?"
    sample_id_list = sample_obj_list_2_sample_acc_list(sample_list)
    #sample_id_list = sample_id_list[0:5]
    #ic| f"{chunk_pos}/{list_size} in chunk_portal_api_call()": '880000/1541315 in chunk_portal_api_call()'
    # low = 880000
    # high = low + 50000
    # sample_id_list = sample_id_list[low:high]

    # logger.info(len(sample_id_list))
    # logger.info(sample_id_list[0:5])
    # logger.info(return_fields)

    #all_sample_data = []
    all_sample_data = chunk_portal_api_call(ena_search_url, with_obj_type, return_fields, None,  sample_id_list)
    # logger.info(all_sample_data)

    return all_sample_data



def main():
    pass

if __name__ == '__main__':
    main()
