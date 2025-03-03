# Pathogen Classifcations README
This folder contains files in various formats describing the taxa used to define the "Pathogen" tag in the European Nucleotide Archive (ENA).

## Rationale for classification

To define a "pathogen" is difficult, and not something the ENA is well placed to do. Therefore, we rely on extneral sources and user requests to assert what is considered a pathogen, in order to include it in the defining lists kept here within.

## Defintion Sources

### Viruses
As all viruses are by definition are obligate intracellular parasites, the simplest rule for inclusion was to consider all viruses to be pathogens. Thus everything falling under NCBI taxonomy ID "10239" is covered by this defintion - including arboviruses.

### United Kingdom Health & Safety Executive - The Approved List of biological agents
The HSE in the UK produces a list of biological agents. The approved list is for use by people who deliberately work with biological agents, especially those in research, development, teaching or diagnostic laboratories and industrial processes, or those who work with humans or animals who are (or are suspected to be) infected with such an agent.

[Link to full list here](https://www.hse.gov.uk/pubns/misc208.htm)

This list is divided into Hazard Groups as defined below.

| Group Name | Definition |
| ---------- | ---------- |
| Group 1 | Unlikely to cause human disease. |
| Group 2 | Can cause human disease and may be a hazard to employees; it is unlikely to spread to the community and there is usually effective prophylaxis or treatment available.|
| Group 3 | Can cause severe human disease and may be a serious hazard to employees; it may spread to the community, but there is usually effective prophylaxis or treatment available.|
| Group 4 | Causes severe human disease and is a serious hazard to employees; it is likely to spread to the community and there is usually no effective prophylaxis or treatment available.|

Based on the above definitions, everything contained in Group 2 or higher was included.

### WHO Priority List
The WHO produced a list of pathogens considered to be of utmost importance from the persepctive of outbreak potential and the challenges posed by drug/vaccine development against said pathogens.

There is a list for [bacteria](https://www.who.int/publications/i/item/9789240093461) and a list for [fungi](https://www.who.int/publications/i/item/9789240060241) which consitute priority pathogens. Additionally, several diseases have been marked as high priority for research and development ([list](https://www.who.int/activities/prioritizing-diseases-for-research-and-development-in-emergency-contexts)), so the causative agents of each disease (where applicable) have also been included.

### The Specified Animal Pathogens Order 2008

The UK Department for Environment, Food and Rural Affairs maintains a list of animal pathogen that are of interest/concern within the UK. Its purpose is to prevent the release of dangerous animal pathogens into the environment where they may cause serious animal or human disease.

[Link to full list here](https://www.legislation.gov.uk/uksi/2008/944/schedule/1/made)

### NIAID Priority Pathogens
The National Institute of Allergy and Infectious Diseases (NIAID) conducts and supports basic and applied research to better understand, treat, and ultimately prevent infectious, immunologic, and allergic diseases. NIAID's Division of Microbiology and Infectious Diseases (DMID), supports extramural research to control and prevent diseases caused by virtually all human infectious agents except HIV. DMID have produced a priority pathogen list below:

[Link to resource here](https://github.com/enasequence/ena-content-dataflow/blob/PP-326/classifications/NIAID_priority_pathogens_Additional_Info_Nov2024.docx.pdf)

### Global Arbovirus Initiative (WHO)
The Global Arbovirus Initiative provides a framework of objectives and priority activities to tackle emerging and re-emerging arboviruses with epidemic and pandemic potential. It is a joint initiative across the World Health Organisation's Health Emergencies Program, Department of Control of Neglected Tropical Diseases and Immunization, Vaccines and Biologicals Department. Specific Aedes-borne and non-Aedes borne arboviruses considered to be of significant risk for global disease outbreak are mentioned by the Global Arbovirus Initiative. 

[Link to publication mentioning arboviruses of interest can be found here](https://iris.who.int/bitstream/handle/10665/376630/9789240088948-eng.pdf?sequence=1)

### Arbovirus Encephalitides - StatPearls Publishing
Arbovirus Encephalitides is an online arbovirus resource written by clinicians at the University of Rochester, New York and published by StatPearls. StatPearls is a professional healthcare education and technology company that publishes a variety of peer-reviewed medical resources for healthcare professionals. 

[Link to resource here](https://www.ncbi.nlm.nih.gov/books/NBK560866/)

### User asserted/requested taxa

| Scientific Name | TaxID | Reason for Inclusion | Requested by |
| ---------- | ------- | --------- | ------- |


## Generation of files

Each source is represented by a JSON file, which defines information about the source, as well as a list of taxa. It takes the basic format:

```json
{
    "source_full_name": "Full name of source authority/organisation",
    "source_short_name": "Acronym",
    "source_urls": [ "links to listed taxa" ],
    "last_updated": "date of last update in this repo",
    "taxa": [
        {
            "name": "Bacteria 1",
            "taxon_id": 12345,
            "taxon_rank": "Species",
            "classification": "Bacteria"
        },
        ...
        {
            "name": "Virus N",
            "taxon_id": 67890,
            "taxon_rank": "Species",
            "classification": "Viruses"
        },
    ]
}
```

To generate CSV files for the Pathogens Portal classifications view, we use the `pathogen_taxa/portal_csvs_from_jsons.py`. This script will:

1. validate all taxon IDs and names listed in the source JSON against a taxonomy API (unless the `--no-validate` flag is used)
2. remove duplicates and group common taxa across sources
3. print some stats/counts
4. extract priority pathogens based on the list of priority sources (currently just WHO) and write CSV file
5. write CSV file for each classification (bacteria, fungi, etc)

#### To run:
```
cd pathogen_taxa/
python portal_csvs_from_jsons.py source.*.json
```
