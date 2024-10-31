#!/usr/bin/env python3
"""Script of sample.py is to sample.py

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2023-09-07
__docformat___ = 'reStructuredText'
chmod a+x sample.py
"""

class Sample:
    """
    objects for storing the environmental status of each sample
    They also get much annotation added by other function,
    """

    def __init__(self, sample_accession):
        self.sample_accession = sample_accession

        self.study_accession = ""
        self.description = ""
        self.tax_id = ""
        self.is_environmental_sample = False
        self.environment_biome = ""
        self.taxonomic_identity_marker = ""
        self.country = ""  # country	locality of sample isolation
        self.location_start = ""
        self.location_end = ""
        self.country_clean = ""
        #also these, but not defined until later, hence defining as False
        self.country_is_european = False
        self.sample_tag_is_freshwater = False
        self.sample_tag_is_marine = False
        self.sample_tag_is_terrestrial = False
        self.sample_tag_is_coastal_brackish = False
        self.taxonomy_obj = None
        self.source_category = ""

    def setCategory(self, category):
        self.source_category = category

    def setEnvironmentalSample(self, boolean_flag):
        """
        this is what is tagged in ENA archive if experiment.is_environmental_sample
        :param boolean_flag:
        :return:
        """
        self.is_environmental_sample = boolean_flag

    
    def get_summary_dict(self):
        if hasattr(self, 'sample_summary_dict'):
            return self.sample_summary_dict
        else:
            self.sample_summary_dict  ={
               "sample_accession": self.sample_accession,
               "is_environmental_sample": self.is_environmental_sample,
               "study_accession": self.study_accession,
               "description": self.description,
               "tax_id": self.tax_id,
               "environment_biome": self.environment_biome,
               "taxonomic_identity_marker": self.taxonomic_identity_marker,
               "country": self.country,
               "country_clean": self.country_clean,
               "country_is_european": self.country_is_european,
               "location_start": self.location_start,
               "location_end": self.location_end,
                "category": self.source_category
             }

            if self.taxonomy_obj != None:
                self.sample_summary_dict.update(self.taxonomy_obj.get_taxon_dict())
                #ic(self.sample_summary_dict)
            return self.sample_summary_dict
        
    def print_values(self):
        out_string = ""

        #ic(inspect.getmembers(self))

        summary_dict = self.get_summary_dict()
        for field in sorted(summary_dict.keys()):
            out_string += f"{field.ljust(30)}: {summary_dict[field]}\n"

        if hasattr(self,"taxonomy_obj") and self.taxonomy_obj != None:
            taxonomy_obj = self.taxonomy_obj
            print(taxonomy_obj.print_summary())

            #out_string += f"{'tax_isMarine'.ljust(30)}: {self.taxonomy_obj.tax_isMarine}\n"

        return out_string


def main():
    test_id="test_id"
    sample = Sample(test_id)

if __name__ == '__main__':
    main()
