import unittest
from taxonomy import *

class Test(unittest.TestCase):
    tax_id_list = ['8860']
    test_hash = [{'scientific_name': 'Chloephaga melanoptera',
                  'tag': 'marine;marine_low_confidence;coastal_brackish;coastal_brackish_low_confidence;freshwater;freshwater_low_confidence;terrestrial;terrestrial_low_confidence',
                  'tax_division': 'VRT',
                  'tax_id': '8860'}]
    portal_hit_hash = do_portal_api_tax_call("taxon", ['8860'],
                                             ['tax_id', 'tax_division', 'tag', 'scientific_name'])

    ic(portal_hit_hash)

    def test_create_taxonomy_hash(self):
        # curl 'https://www.ebi.ac.uk/ena/portal/api/search?result=taxon&includeAccessions=8860&format=json&fields=tax_id,tag,scientific_name'
        hit_hash = create_taxonomy_hash(self.tax_id_list)
        # ic(hit_hash)
        self.assertListEqual(hit_hash, self.test_hash)

    def test_do_portal_api_tax_call(self):
        self.assertListEqual(self.portal_hit_hash, self.test_hash)


    def test_taxon_collection(self):

        taxon_collection_obj = taxon_collection(self.portal_hit_hash)
        taxon_obj = taxon_collection_obj.get_taxon_obj_by_id('8860')
        self.assertEqual(taxon_obj.tax_id,'8860')

    def test_get_all_taxon_obj_list(self):
        taxon_collection_obj = taxon_collection(self.portal_hit_hash)
        self.assertEqual(len(taxon_collection_obj.get_all_taxon_obj_list()), 1)

    def test_generate_taxon_collection(self):
        taxon_collection_obj = generate_taxon_collection(self.tax_id_list)
        self.assertEqual(len(taxon_collection_obj.get_all_taxon_obj_list()), 1)

    def test_scientific_name(self):
        taxon_collection_obj = taxon_collection(self.portal_hit_hash)
        taxon_obj = taxon_collection_obj.get_taxon_obj_by_id('8860')
        self.assertEqual(taxon_obj.scientific_name,'Chloephaga melanoptera')

    def test_tag_status(self):
        taxon_collection_obj = taxon_collection(self.portal_hit_hash)
        taxon_obj = taxon_collection_obj.get_taxon_obj_by_id('8860')

        self.assertEqual(taxon_obj.isTerrestrial, False)
        self.assertEqual(taxon_obj.isMarine, False)
        self.assertEqual(taxon_obj.isCoastal, False)
        self.assertEqual(taxon_obj.isFreshwater, False)

if __name__ == '__main__':
    unittest.main()
