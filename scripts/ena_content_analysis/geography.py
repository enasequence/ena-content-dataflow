#!/usr/bin/env python3
"""Script of geography.py is to geograph.py

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2023-09-08
__docformat___ = 'reStructuredText'
chmod a+x geography.py
"""

import re
import sys
from eDNA_utilities import logger

def clean_insdc_country_term(country):
    """
    Does some basic cleaning of country terms, is far from exhaustive, but > 80:20
    :param country:
    :return: county
    """

    def remove_before(string, suffix):
        # Trims anything after the first "suffix", uses that is the wrong terminology...
        if suffix not in string:
            return string
        else:
            return string[:string.index(suffix) + (len(suffix) - 1)]

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

    clean_country = remove_before(country, ":")
    clean_country = remove_before(clean_country, ";")
    clean_country = capitalise(clean_country)
    return clean_country


class Geography:
    def __init__(self):
        self.insdc_full_set = {}
        self.not_europe_set = {}
        self.europe_country_set = {}
        self.europe_all_set = {}
        self.eu_set = {}
        self.europe_sea_set = {}
        self.none_eu_europe_set = {}
        self.north_america_set = {}
        self.ocean_sea_set = {}
        self.country_set = {}
        self.build_insdc_lists()

    def is_insdc_country(self, country):
        """
        any eu country or sea
        assumes that you have cleaned the country term!
       """
        if country in self.insdc_full_set:
            return True
        else:
            return False


    
    def is_sea_in_europe(self, country):
        """
        any eu country or sea
        assumes that you have cleaned the country term!
       """
        if country in self.europe_sea_set:
            return True
        else:
            return False

        

    def is_insdc_country_in_europe(self, country):
        """
        any european country or seas around europe
        assumes that you have cleaned the country term!
       """
        # print(f"is_insdc_country_in_europe {country}")
        if country in self.europe_all_set:
            return True
        return False

    def is_insdc_country_in_north_america(self, country):
        """
       any North American country  that you have cleaned the country term!
       """
        # print(f"   is_insdc_country_in_north_america for {country}")
        # print(self.north_america_set)
        if country in self.north_america_set:
            return True

        # print(f"   is_insdc_country_in_north_america for {country} returning False")
        return False

    def get_continent(self, country):
        """
        assuming it is the cleaned country term!
        :param country:
        :return:
        """
        # print(f"inside get_continent: {country}")

        if country is None:
            return 'undetermined'
        elif country in self.ocean_sea_set :
            return 'ocean'
        elif country in self.europe_country_set:
            return 'europe'
        elif country in self.north_america_set:
            return 'north america'
        elif country in self.south_america_set:
            return 'south america'
        elif country in self.africa_set:
            return 'africa'
        elif country in self.asia_set:
            return 'asia'
        elif country in self.australasia_set:
            return 'australasia'
        elif country in self.antartica_set:
            return 'australasia'
        return 'undetermined'

    def get_ocean(self, country):
        """
        assuming it is the cleaned country term!
        :param country:
        :return:
        """
        # print(f"inside get_continent: {country}")

        if country is None:
            return None
        elif country in self.ocean_sea_set:
            return country
        return 'not ocean'

    def get_european_sea(self, country):
        """
        is any European Sea and parts of Atlantic ocean
        assuming it is the cleaned country term!
        :param country:
        :return:
        """

        if country is None:
            return None
        elif country in self.europe_sea_set:
            return country
        return None


    def is_insdc_country_in_eu(self, country):
        """
        any eu country
        assumes that you have cleaned the country term!
       """
        if country in self.eu_set:
            return True
        else:
            return False

    def build_insdc_lists(self):
        print("inside build_insdc_lists:")

        # cmd="curl 'https://www.insdc.org/submitting-standards/country-qualifier-vocabulary/' 2> /dev/null | tr '\n'
        # '@' | sed 's/^.*the-world-factbook\/<\/a><\/p>//;s/<p class.*//;s/<\/ul>.*//' | tr '@' '\n' | sed 's/^[
        # ^>]*>//;s/<\/li>$//'"
        insdc_raw = ('Afghanistan,Albania,Algeria,American Samoa,Andorra,Angola,Anguilla,Antarctica,Antigua and '
                     'Barbuda,Arctic Ocean,Argentina,Armenia,Aruba,Ashmore and Cartier Islands,Atlantic Ocean,'
                     'Australia,Austria,Azerbaijan,Bahamas,Bahrain,Baltic Sea,Baker Island,Bangladesh,Barbados,'
                     'Bassas da India,Belarus,Belgium,Belize,Benin,Bermuda,Bhutan,Bolivia,Borneo,Bosnia and '
                     'Herzegovina,Botswana,Bouvet Island,Brazil,British Virgin Islands,Brunei,Bulgaria,Burkina Faso,'
                     'Burundi,Cambodia,Cameroon,Canada,Cape Verde,Cayman Islands,Central African Republic,Chad,Chile,'
                     'China,Christmas Island,Clipperton Island,Cocos Islands,Colombia,Comoros,Cook Islands,'
                     'Coral Sea Islands,Costa Rica,Cote d&#8217;Ivoire,Croatia,Cuba,Curacao,Cyprus,Czechia,'
                     'Czech Republic,Democratic Republic of the Congo,Denmark,Djibouti,Dominica,Dominican Republic,'
                     'Ecuador,Egypt,El Salvador,Equatorial Guinea,Eritrea,Estonia,Eswatini,Ethiopia,Europa Island,'
                     'Falkland Islands (Islas Malvinas),Faroe Islands,Fiji,Finland,France,French Guiana,'
                     'French Polynesia,French Southern and Antarctic Lands,Gabon,Gambia,Gaza Strip,Georgia,Germany,'
                     'Ghana,Gibraltar,Glorioso Islands,Greece,Greenland,Grenada,Guadeloupe,Guam,Guatemala,Guernsey,'
                     'Guinea,Guinea-Bissau,Guyana,Haiti,Heard Island and McDonald Islands,Honduras,Hong Kong,'
                     'Howland Island,Hungary,Iceland,India,Indian Ocean,Indonesia,Iran,Iraq,Ireland,Isle of Man,'
                     'Israel,Italy,Jamaica,Jan Mayen,Japan,Jarvis Island,Jersey,Johnston Atoll,Jordan,Juan de Nova '
                     'Island,Kazakhstan,Kenya,Kerguelen Archipelago,Kingman Reef,Kiribati,Kosovo,Kuwait,Kyrgyzstan,'
                     'Laos,Latvia,Lebanon,Lesotho,Liberia,Libya,Liechtenstein,Line Islands,Lithuania,Luxembourg,'
                     'Macau,Madagascar,Malawi,Malaysia,Maldives,Mali,Malta,Marshall Islands,Martinique,Mauritania,'
                     'Mauritius,Mayotte,Mediterranean Sea,Mexico,Micronesia,Federated States of,Midway Islands,'
                     'Moldova,Monaco,Mongolia,Montenegro,Montserrat,Morocco,Mozambique,Myanmar,Namibia,Nauru,'
                     'Navassa Island,Nepal,Netherlands,New Caledonia,New Zealand,Nicaragua,Niger,Nigeria,Niue,'
                     'Norfolk Island,North Korea,North Macedonia,North Sea,Northern Mariana Islands,Norway,Oman,'
                     'Pacific Ocean,Pakistan,Palau,Palmyra Atoll,Panama,Papua New Guinea,Paracel Islands,Paraguay,'
                     'Peru,Philippines,Pitcairn Islands,Poland,Portugal,Puerto Rico,Qatar,Republic of the Congo,'
                     'Reunion,Romania,Ross Sea,Russia,Rwanda,Saint Barthelemy,Saint Helena,Saint Kitts and Nevis,'
                     'Saint Lucia,Saint Martin,Saint Pierre and Miquelon,Saint Vincent and the Grenadines,Samoa,'
                     'San Marino,Sao Tome and Principe,Saudi Arabia,Senegal,Serbia,Seychelles,Sierra Leone,Singapore,'
                     'Sint Maarten,Slovakia,Slovenia,Solomon Islands,Somalia,South Africa,South Georgia and the South '
                     'Sandwich Islands,South Korea,South Sudan,Southern Ocean,Spain,Spratly Islands,Sri Lanka,'
                     'State of Palestine,Sudan,Suriname,Svalbard,Sweden,Switzerland,Syria,Taiwan,Tajikistan,Tanzania,'
                     'Tasman Sea,Thailand,Timor-Leste,Togo,Tokelau,Tonga,Trinidad and Tobago,Tromelin Island,Tunisia,'
                     'Turkey,Turkmenistan,Turks and Caicos Islands,Tuvalu,USA,Uganda,Ukraine,United Arab Emirates,'
                     'United Kingdom,Uruguay,Uzbekistan,Vanuatu,Venezuela,Viet Nam,Virgin Islands,Wake Island,'
                     'Wallis and Futuna,West Bank,Western Sahara,Yemen,Zambia,Zimbabwe')
        insdc_full_list = insdc_raw.split(',')
        self.insdc_full_set = set(insdc_full_list)
        # print(self.insdc_full_list)

        # https://www.gov.uk/eu-eea
        eu_raw = ("Austria, Belgium, Bulgaria, Croatia, Republic of Cyprus, Czechia, Czech Republic, Denmark, Estonia, "
                  "Finland, France, Germany, Greece, Hungary, Ireland, Italy, Latvia, Lithuania, Luxembourg, Malta, "
                  "Netherlands, Poland, Portugal, Romania, Slovakia, Slovenia, Spain, Sweden")
        eu_list = eu_raw.split(', ')
        # print(eu_list)

        noneu_euro_raw = ("Iceland,Liechtenstein,Norway,Switzerland,United Kingdom,Albania,Belarus,Gibraltar,Jersey,"
                          "Montenegro,San Marino,Svalbard,Moldova,Isle of Man,Monaco,Cyprus,Bosnia and Herzegovina,"
                          "North Macedonia,Serbia,Andorra,Russia,Guernsey,Ukraine,Jan Mayen")
        noneu_euro_list = noneu_euro_raw.split(',')
        europe_list = sorted(noneu_euro_list + eu_list)
        not_europe_set = set()

        ocean_sea_set = set()
        country_set = set()
        for term in sorted(insdc_full_list):
            if re.search(r"Sea|Ocean|Gulf|English Channel|Bay of Biscay", term):
                ocean_sea_set.add(term)
            else:
                country_set.add(term)
        # print("++++++++++++++++++++++++++++++++++++++++++++")
        for country in sorted(country_set):
            if country not in europe_list:
                # print(country)
                not_europe_set.add(country)
        self.not_europe_set = not_europe_set
        self.europe_country_set = set(europe_list)
        europe_sea_list = ['Mediterranean Sea', 'North Sea', 'Baltic Sea', 'Black Sea', 'Sea of Azov', 'Caspian Sea', 'White Sea', 'Barents Sea','Norwegian Sea', 'North Atlantic Ocean', 'North-east Atlantic Ocean', 'Kattegat', 'Iceland Sea', 'English Channel', 'Irish Sea', 'Bay of Biscay', 'Iberian Coast', 'Adriatic Sea', 'Ionian Sea', 'Aegean-Levantine Sea']
        self.europe_sea_set = set(europe_sea_list)
        
        self.europe_all_set = self.europe_country_set.union(self.europe_sea_set)
        self.eu_set = set(eu_list)
        self.none_eu_europe_set = self.europe_all_set.difference(self.eu_set)
        self.north_america_set = {'Saint Barthelemy','Navassa Island', 'Virgin Islands', 'Anguilla', 'Antigua and Barbuda', 'Bahamas', 'Barbados', 'Belize', 'Bermuda',
                                  'Canada', 'Cayman Islands', 'Costa Rica', 'Dominica', 'Dominican Republic', 'Ecuador',
                                  'El Salvador', 'Greenland', 'Grenada', 'Guatemala', 'Martinique', 'Mexico', 'Nicaragua',
                                  'Niger', 'Nigeria', 'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Martin',
                                  'Saint Pierre and Miquelon', 'Saint Vincent and the Grenadines', 'Sint Maarten',
                                  'Trinidad and Tobago', 'Turks and Caicos Islands', 'USA', 'United States of America', 'Puerto Rico'}
        self.africa_set = {'Cote d&#8217;Ivoire',"Cote d'Ivoire",'Europa Island', 'Reunion', 'Juan de Nova Island', 'Sao Tome and Principe', 'Malawi', 'Sierra Leone', 'Eritrea', 'Western Sahara', 'Syria', 'Benin', 'Algeria',  'Lesotho', 'Liberia',  'Comoros',  'Tunisia',  'Djibouti', 'Republic of the Congo', 'Nauru', 'Cameroon', 'Honduras', 'Mauritius', 'Guinea-Bissau',  'Gambia', 'South Africa', 'Ghana', 'Democratic Republic of the Congo', 'Central African Republic', 'Botswana', 'Chad','Kiribati', 'Rwanda', 'Libya', 'Somalia', 'Gabon', 'Mauritania', 'Senegal', 'Togo', 'Sri Lanka', 'Sudan', 'Egypt', 'Burundi', "Cote d'Ivoire", 'Madagascar', 'Equatorial Guinea', 'Guinea', 'Angola', 'French Guiana', 'Morocco', 'South Sudan', 'Tanzania', 'Zimbabwe', 'Namibia','Kenya', 'Saint Helena', 'Uganda', 'Mozambique', 'Ethiopia', 'Zambia'}
        self.australasia_set = {'Australia','New Zealand', 'Cook Islands','Papua New Guinea','New Caledonia'}
        self.asia_set = {'Paracel Islands','Gaza Strip', 'Spratly Islands', 'Georgia', 'Palau', 'Tuvalu', 'Singapore', 'Thailand', 'Azerbaijan', 'Turkey', 'Fiji', 'Taiwan', 'Kuwait', 'Israel', 'Kazakhstan', 'Guam', 'British Virgin Islands', 'China', 'American Samoa', 'Northern Mariana Islands', 'South Korea',  'Saudi Arabia', 'Armenia', 'Wallis and Futuna', 'French Southern and Antarctic Lands', 'Malaysia', 'Bangladesh', 'Iraq', 'Eswatini', 'Myanmar', 'Federated States of', 'Jamaica', 'Afghanistan', 'Glorioso Islands', 'Christmas Island', 'Timor-Leste', 'Bahrain', 'Philippines', 'Yemen', 'Oman', 'United Arab Emirates', 'Mongolia', 'Palmyra Atoll', 'Cuba', 'State of Palestine', 'Micronesia', 'Nepal', 'Indonesia', 'Maldives', 'North Korea', 'Seychelles', 'Johnston Atoll', 'Mayotte', 'Solomon Islands', 'Iran', 'Guadeloupe', 'Macau', 'Jarvis Island', 'Tromelin Island', 'Lebanon', 'Kingman Reef', 'Falkland Islands (Islas Malvinas)', 'Heard Island and McDonald Islands', 'Turkmenistan', 'Clipperton Island', 'Faroe Islands', 'French Polynesia', 'Niue', 'West Bank', 'Qatar', 'Burkina Faso', 'Jordan', 'Japan', 'Line Islands', 'Samoa', 'Borneo', 'Uzbekistan', 'Tokelau', 'Bouvet Island', 'Ashmore and Cartier Islands', 'Haiti', 'Uruguay', 'Tajikistan', 'Kyrgyzstan', 'Guyana', 'South Georgia and the South Sandwich Islands', 'Norfolk Island', 'Viet Nam', 'Mali', 'Pakistan', 'Vanuatu', 'India', 'Bassas da India', 'Bhutan', 'Montserrat', 'Cape Verde', 'Tonga',  'Kosovo', 'Howland Island', 'Laos', 'Hong Kong', 'Brunei', 'Wake Island', 'Cambodia'}
        self.south_america_set = {'Curacao', 'Aruba', 'Brazil', 'Paraguay', 'Panama', 'Suriname', 'Bolivia', 'Peru', 'Colombia', 'Argentina', 'Chile', 'Venezuela'}
        self.antartica_set ={ 'Antarctica', 'Kerguelen Archipelago'}
        self.other_set = {'Midway Islands',  'Pitcairn Islands','Marshall Islands', 'Baker Island','Cocos Islands'}
        remainder_set = self.insdc_full_set.difference(self.europe_all_set)
        remainder_set = remainder_set.difference(self.north_america_set)
        remainder_set = remainder_set.difference(ocean_sea_set)
        remainder_set = remainder_set.difference(self.africa_set)
        remainder_set = remainder_set.difference(self.australasia_set)
        remainder_set = remainder_set.difference(self.asia_set)
        remainder_set = remainder_set.difference(self.south_america_set)
        remainder_set = remainder_set.difference(self.antartica_set)
        remainder_set = remainder_set.difference(self.other_set)
        self.ocean_sea_set = ocean_sea_set
        self.country_set = country_set

    def get_insdc_full_country_set(self):
        return self.insdc_full_set

    def print_summary(self):

        out_string = ""
        out_string += f"insdc_full_set:\t{len(self.insdc_full_set)}\n"
        out_string += f"country_set:\t{len(self.country_set)}\n"
        out_string += f"ocean_sea_set:\t{len(self.ocean_sea_set)}\n"
        out_string += f"europe_country_set:\t{len(self.europe_country_set)}\n"
        out_string += f"not_europe_set:\t{len(self.not_europe_set)}\n"
        out_string += f"europe_all_set:\t{len(self.europe_all_set)}\n"
        out_string += f"eu_set:\t{len(self.eu_set)}\n"
        out_string += f"none_eu_europe_set:\t{len(self.none_eu_europe_set)}\n"

        return out_string


def main():
    print("running main in geography.py")
    geography = Geography()
    print(geography.print_summary())

    print("\n-------------Some Simple Tests----------")
    test_hash = {
        'France': {"in_europe": True, "in_eu": True, "insdc_country": True, "sea_in_europe": False},
        'FRANCE': {"in_europe": True, "in_eu": True, "insdc_country": True, "sea_in_europe": False},
        'France:Paris': {"in_europe": True, "in_eu": True, "insdc_country": True, "sea_in_europe": False},
        'United Kingdom;London': {"in_europe": True, "in_eu": False, "insdc_country": True, "sea_in_europe": False},
        'Australia': {"in_europe": False, "in_eu": False, "insdc_country": True, "sea_in_europe": False},
        'North Sea': {"in_europe": True, "in_eu": False, "insdc_country": True, "sea_in_europe": True},
        "North sea": {"in_europe": True, "in_eu": False, "insdc_country": True, "sea_in_europe": True},
        "Pacific Ocean": {"in_europe": False, "in_eu": False, "insdc_country": True, "sea_in_europe": False},
        'Antigua and Barbuda': {"in_europe": False, "in_eu": False, "insdc_country": True, "sea_in_europe": False}
        }

    test_terms = test_hash.keys()
    for test_term in test_terms:
        logger.info(test_term)
        clean_term = clean_insdc_country_term(test_term)
        print(f"clean_version:-->{clean_term}<--")
        logger.info(geography.is_insdc_country_in_europe(clean_term))
        if test_hash[test_term]['in_europe'] != geography.is_insdc_country_in_europe(clean_term):
            print("ERROR")
            print(f"test_hash[test_term]['in_europe'] = {test_hash[test_term]['in_europe'] }")
            sys.exit()
        logger.info(geography.is_insdc_country_in_eu(clean_term))
        if test_hash[test_term]['in_eu'] != geography.is_insdc_country_in_eu(clean_term):
            print("ERROR")
            print(f"test_hash[test_term]['in_eu'] = {test_hash[test_term]['in_eu'] }")
            sys.exit()
        logger.info(geography.is_insdc_country(clean_term))
        if test_hash[test_term]['insdc_country'] != geography.is_insdc_country(clean_term):
            print("ERROR")
            print(f"test_hash[test_term]['indsc_country'] = {test_hash[test_term]['insdc_country'] }")
            sys.exit()
        logger.info(geography.is_sea_in_europe(clean_term))
        if test_hash[test_term]['sea_in_europe'] != geography.is_sea_in_europe(clean_term):
            print("ERROR")
            print(f"test_hash[test_term]['sea_in_europe'] = {test_hash[test_term]['sea_in_europe'] }")
            sys.exit()
        logger.info("+++++++++++++++++++++++++++++++++++")


if __name__ == '__main__':
    main()
