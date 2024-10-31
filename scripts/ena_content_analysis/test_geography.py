import unittest
from geography import Geography

class TestGeography(unittest.TestCase):

    geography = Geography()
    def test_is_insdc_country(self):
        self.assertEqual(self.geography.is_insdc_country('France'), True)

    def test_is_insdc_country_in_europe(self):
        self.assertEqual(self.geography.is_insdc_country_in_europe('France'), True)
        self.assertEqual(self.geography.is_insdc_country_in_europe('United Kingdom'), False)

    def test_is_insdc_country_in_europe(self):
        self.assertEqual(self.geography.is_insdc_country_in_europe('France'), True)
        self.assertEqual(self.geography.is_insdc_country_in_europe('United Kingdom'), True)

    def test_clean_insdc_country_term(self):
        self.assertEqual(clean_insdc_country_term('France'), 'France')
        self.assertEqual(clean_insdc_country_term('FRANCE:Paris'), 'France')
        self.assertEqual(clean_insdc_country_term('Antigua and barbuda'), 'Antigua and Barbuda')

if __name__ == '__main__':
    unittest.main()