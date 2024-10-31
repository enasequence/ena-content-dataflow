#!/usr/bin/env python3
"""Script of data_utils.py is to provided data_utilities for the other classes here etc.

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2023-12-04
__docformat___ = 'reStructuredText'
"""

import pandas as pd

def get_data_location_dict():
    """

    :return: data_location_dict
    """
    base_dir = '/Users/woollard/projects/eDNAaquaPlan/eDNAAqua-Plan/'
    base_data_dir = base_dir + 'data/'
    data_location_dict = {
        'base_dir': base_dir,
        'base_data_dir': base_data_dir,
        'requirements_dir': base_data_dir + 'requirements_in/',
        'out_dir': base_data_dir + 'out/'
    }
    return data_location_dict

def get_requirements_df():
    """

    :return: requirements_df
    """
    data_location_dict = get_data_location_dict()
    print(data_location_dict['requirements_dir'])
    req_xlsx = data_location_dict['requirements_dir'] + 'WP2-WP3_metadata_reqs.xlsx'
    requirements_df = pd.read_excel(req_xlsx, sheet_name=0, index_col='No.')
    return requirements_df

def get_required_metadata_field_list():
    """

    :return: required_metadata_field_list
    """
    requirements_df = get_requirements_df()
    return list(requirements_df['Name'])

def get_metadata_preknown_dict():
    metadata_preknown_dict = {
        "data_metadata_link": {"value": "The main metadata objects: study, analysis. sample, and experiment all have accession ids that ultimately link to a run and this sequences e.g. SRR19634423. metadata model: https://ena-docs.readthedocs.io/en/latest/submit/general-guide/metadata.html accession numbers: https://ena-docs.readthedocs.io/en/latest/submit/general-guide/accessions.html", "example": "https://www.ebi.ac.uk/ena/browser/view/SRR19634423", "comment": ""},
        "record_URL": {"value": True, "example": "https://www.ebi.ac.uk/ena/browser/view/PRJNA847784", "comment": ""},
        "metadata_record_URL": {"value": True, "example": "https://www.ebi.ac.uk/ena/browser/view/SAMN28957904", "comment": ""},
        "paper_DOI_number": {"value": True, "example": "https://www.ebi.ac.uk/ena/browser/view/PRJDA18707?show=publications", "comment": "not all records have publications"},
        "paper_link": {"value": True, "example": "https://www.ebi.ac.uk/ena/browser/view/PRJDA18707?show=publications", "comment": "not all records have publications"},
        "DB_name": {"value": {"INSDC": ["ENA","GenBank","DDBJ"]}, "example": "ENA", "comment": "Almost all records are shared across all INDSC members"},
        "DB_use_index": {"value": 50, "example": "", "comment": "Almost a complete guess"},
        "DB_using community": {"value": "mixed", "example": "", "comment": "INSDC does contain sequence data from practially any community including the aquatic sources"},
        "number of records": {"value": "determine_programmatically", "example": "24000000", "comment": ""},
        "web_interface": {"value": True, "example": "https://www.ebi.ac.uk/ena/browser/view/PRJNA847784", "comment": ""},
        "API": {"value": "https://www.ebi.ac.uk/ena/portal/api/swagger-ui/index.html", "example": "", "comment": "A Swagger interface"},
        "application": {"value": False, "example": "", "comment": "Although 1) ftp and Aspera cam be used  2) third parties e.g. Elixir Belgium do provide a Galaxy module"},
        "GPS_coordinates_available": {"value": True, "example": "44.785325 N 0.5773358 W", "comment": "latitude and longitude, for ~20% of samples"},
        "data_export_format": {"value": ["EMBL", "XML", "JSON", "fasta", "fastq"], "example": "fastq", "comment": ""},
        "metadata_export_format": {"value": ["EMBL", "XML", "JSON", "TSV"], "example": "JSON", "comment": "The API exports as JSON or TSV. From the browser can get XML, EMBL or JSON."},
        "env_data_contains": {"value": True, "example": "SAMN28957904", "comment": "Technically, most of the samples are in biosamples."},
        "barcode_taxonomy_confidence": {"value": False, "example": "", "comment": "Not aware of any confidence"},
        "annually_updated": {"value": False, "example": "", "comment": "records are being added around the clock."},
        "data_paper_location": {"value": "not being recorded", "example": "", "comment": "the section of the paper is not being recorded, just the reference to the paper"},
        "sequences_processed": {"value": "both", "example": "", "comment": "Both raw and processed sequences if provided are archived and retrievable"},
        "DB_standard_consistent": {"value": True, "example": "", "comment": "INSDC"},
        "DB_mandatory_metadata": {"value": ["Taxonomy id", "country", "collection_date"], "example": "", "comment": ""},
        "data_file_format": {"value": ["fastq", "BAM", "CRAM"], "example": "", "comment": "This is for the sequencing read files"},
        "metadata_file_format": {"value": ["EMBL", "XML", "JSON"], "example": "JSON", "comment": ""},
        "metadata_file_schema": {"value": ["GSC MIxS", 'ENA'], "example": "", "comment": "Most of the INSDC terms are based on GSC MIxS. There are still some unique ones, especially in marine."},
        "DB_active": {"value": True, "example": "", "comment": "records are being added around the clock."},
        "DB_curation": {"value": True, "example": "", "comment": "Although this varies across INSDC partners, e.g. GenBank and DDBJ probably curate a higher proportion than ENA. In ENA some curation is retrospectively done via the ELIXIR ClearingHouse"},
        "sample_collection": {"value": True, "example": "", "comment": "although compliance and depth vary"},
        "DNA_extraction": {"value": True, "example": "", "comment": "although compliance and depth vary"},
        "sequence_methodology": {"value": False, "example": "", "comment": "Although this is short term aim of ENA to add to add the URL"},
        "sequencing_strategy": {"value": True, "example": "AMPLICON", "comment": "In INSDC there is are a controlled vocabulary for this"},
        "analysis_workflow": {"value": True, "example": "", "comment": "Although this is rare"},
        "directly_uploaded": {"value": True, "example": "", "comment": "Can tell from the identifiers which INSDC partner it was uploaded to"},
        "barcode_name": {"value": True, "example": "16S", "comment": "Although not always"},
        "barcode_certain": {"value": False, "example": "", "comment": "Not observed a confidence measure"},
        "taxonomy_origin": {"value": True, "example": "", "comment": "all from the NCBI"},
        "taxonomy_URL": {"value": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Tree&id=6720&lvl=3&p=has_linkout&p=blast_url&p=genome_blast&p=mapview&lin=f&keep=1&srchmode=1&unlock", "example": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Tree&id=6720&lvl=3&p=has_linkout&p=blast_url&p=genome_blast&p=mapview&lin=f&keep=1&srchmode=1&unlock", "comment": ""},
        "taxonomically_identified": {"value": True, "example": "", "comment": ""},
        "taxonomical_name": {"value": True, "example": "", "comment": "Provides the taxonomic name and identifier, if not eactialyl known may be 'genus sp'"},
        "taxonomy_linking": {"value": True, "example": "", "comment": "Practically yes as points to and up to date comprehensived NCBI Taxonomy dump data, but there is no URL to the NCBI Taxonomy website."}
    }
    return metadata_preknown_dict

def main():
    data_location_dict = get_data_location_dict()
    print(data_location_dict)
    print(get_requirements_df())
    print(get_required_metadata_field_list())

if __name__ == '__main__':

    main()
