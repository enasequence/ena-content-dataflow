#!/usr/bin/env python3
"""Script of processed_categories.py is to processed_categories.py

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2023-12-05
__docformat___ = 'reStructuredText'
chmod a+x taxonomy.py
"""


from eDNA_utilities import logger
import os
import argparse
from ena_portal_api import ena_portal_api_call, get_ena_portal_url, chunk_portal_api_call
from itertools import islice
import sys


class ProcessedCategory:
    """

    """

    def __init__(self, category, sample_accs_by_specific_category):
        """
        :param

        """
        #intialise:
        self._category = category
        self._sample_accs_by_specific_category = sample_accs_by_specific_category

    def print_summary(self):
        print(f"category={self._category}")
        #logger.info(self._sample_accs_by_specific_category)

        print(self.get_sample_acc_total())
        sample_collection_obj = self.get_sample_collection_obj()
        print(sample_collection_obj.print_summary())

        sys.exit()

    def get_sample_acc_list(self):
        return self._sample_accs_by_specific_category['sample_acc_list']

    def get_sample_acc_total(self):
        return len(self.get_sample_acc_list())

    def get_sample_collection_obj(self):
        return self._sample_accs_by_specific_category['sample_collection_obj']

class ProcessedCategories:
    """
    processed_categories_obj = processed_categories(sample_accs_by_category ):
    """
    def __init__(self, sample_accs_by_category):
        self._sample_accs_by_category = sample_accs_by_category

    def print_summary(self):
        logger.info(self.get_category_list())

        category_objs = self.get_category_objects()
        sys.exit()
        logger.info("...............................................................")
        for category_obj in category_objs:
            logger.info(category_obj.print_summary())
        logger.info("...............................................................")

    def get_category_list(self):
        return sorted(self._sample_accs_by_category.keys())

    def get_category_objects(self):
        objects = []
        for category in self.get_category_list():
            category_obj = ProcessedCategory(category, self._sample_accs_by_category[category])
            category_obj.print_summary()
            objects.append(category_obj)
        logger.info("...............................................................")
        return objects

def main():
    pass

if __name__ == '__main__':
    main()
