# ena_content_annotation

These are a bunch off annotation files to use to decorate ENA exports.

## INSDC countries

INSDC Countries + Geography.xlsx  - as a multisheet spreadsheet

This data came from CIA Factbook and UN WHO web pages.
These were collected in July 2024 by Samuel Woollard during his work experience week.

### GDP.tsv
| INSDC_country            |     GDP ($) |   Population |   GDP per capita ($) | Income Level   |   Year | Note |
|:-------------------------|------------:|-------------:|---------------------:|:---------------|-------:|-----:|
| Burundi                  | 3.33872e+09 |  1.35901e+07 |              245.673 | Low income     |   2022 |      |
| Eritrea                  | 2.065e+09   |  6.34396e+06 |              325.507 | Low income     |   2011 |      |
| Afghanistan              | 1.50216e+10 |  4.01216e+07 |              374.402 | Low income     |   2022 |      |
| Syria                    | 8.98006e+09 |  2.38654e+07 |              376.279 | Low income     |   2021 |      |
| Central African Republic | 2.38262e+09 |  5.65096e+06 |              421.631 | Low income     |   2022 |      |

### Physical_Geography.tsv
| INSDC_country   | Continent   |   Land Area (km2) |   Distance of Coastline (km) | Hemisphere   |   Average Temperature (°C) | Climate(s)      |
|:----------------|:------------|------------------:|-----------------------------:|:-------------|---------------------------:|:----------------|
| Afghanistan     | Asia        |            652230 |                            0 | Northern     |                      15.7  | Arid, semiarid  |
| Albania         | Europe      |             28748 |                          362 | Northern     |                      15.17 | Temperate       |
| Algeria         | Africa      |           2381740 |                          998 | Northern     |                      20    | Arid, semiarid  |
| American Samoa  | Oceania     |               224 |                          116 | Southern     |                      28    | Tropical marine |
| Andorra         | Europe      |               468 |                            0 | Northern     |                       7.17 | Temperate       |

### country_list.tsv
| INSDC_country   | UN Country  | Note                                                                                                  | country_or_ocean   |
|:----------------|:------------|:------------------------------------------------------------------------------------------------------|:-------------------|
| Afghanistan     | Afghanistan |                                                                                                       | country            |
| Albania         | Albania     |                                                                                                       | country            |
| Algeria         | Algeria     |                                                                                                       | country            |
| American Samoa  |             | n/a- country is not recognised by UN, it is owned by another country or no country officially owns it | country            |
| Andorra         | Andorra     |                                                                                                       | country            |