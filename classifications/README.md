# Pathogen Classifcations README
This folder contains files in various formats describing the taxa used to define the "Pathogen" tag in the European Nucleotide Archive (ENA).

## Rationale for classification

To define a "pathogen" is difficult, and not something the ENA is well placed to do. Therefore, we rely on extneral sources and user requests to assert what is considered a pathogen, in order to include it in the defining lists kept here within.

## Defintion Sources

### Viruses
As all viruses are by definition are obligate intracellular parasites, the simplest rule for inclusion was to consider all viruses to be pathogens. This everything falling under NCBI taxonomy ID "2" are included in this defintion.

### United Kingdom Health & Safety Executive - The Approved List of biological agents 
The HSE in the UK produces a list of biological agents. The approved list is for use by people who deliberately work with biological agents, especially those in research, development, teaching or diagnostic laboratories and industrial processes, or those who work with humans or animals who are (or are suspected to be) infected with such an agent.

[Link to full list here](https://www.hse.gov.uk/pubns/misc208.htm)

This list is divided into Hazard Groups as defined below.

| Group Name | Definition |
| ---------- | ---------- |
| Group 1 | Unlikely to cause human disease. |
| Group 2 | Can cause human disease and may be a hazard to employees; it is unlikely to spread to the community and there is usually effective prophylaxis or treatment available.|
| Group 3 | Can cause severe human disease and may be a serious hazard to employees; it may spread to the community, but there is usually effective prophylaxis or treatment available.|
| Group 4 | Causes severe human disease and is a serious hazard toemployees; it is likely to spread to the community and there is usually no effective prophylaxis or treatment available.|

Based on the above definitions, everything contained in Group 2 or higher was included.

### WHO Priority List
The WHO produced a list of pathogens considered to be of utmost importance from the persepctive of outbreak potential and the challenges posed by drug/vaccine development against said pathogens.

There is a list for [bacteria](https://www.who.int/publications/i/item/WHO-EMP-IAU-2017.12) and a list for [fungi](https://www.who.int/publications/i/item/9789240060241) which consitute priority pathogens.

### User asserted/requested taxa

| Scientific Name | TaxID | Reason for Inclusion | Requested by |
| ---------- | ------- | --------- | ------- |
