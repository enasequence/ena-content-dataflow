#!/usr/bin/env python3
"""Script of study_collection.py is to study_collection.py

___author___ = "woollard@ebi.ac.uk"
___start_date___ = 2023-09-18
__docformat___ = 'reStructuredText'
chmod a+x study_collection.py
"""

from eDNA_utilities import logger
from ena_portal_api import get_ena_portal_url, ena_portal_api_call_basic, chunk_portal_api_call_w_ands



class StudyCollection:
    """

    """

    def __init__(self):
        self.name = "TBD"
        self.study_dict = {'study': {}, 'sample': {}}

    def get_name(self):
        return self.name

    def get_global_study_dict(self):
        return self.study_dict

    def get_sample_id_list(self):
        # my_dict = self.get_global_study_dict()
        global_sample_set = set()
        for study_id in self.study_dict['study']:
            global_sample_set.update(self.study_dict['study'][study_id]['sample_acc_set'])
        return sorted(list(global_sample_set))

def study2sample(study_id_list, study_collection, debug_status):
    """

    :param debug_status:
    :param study_id_list:
    :param study_collection:  # if None creates it!
    :return: sample_acc_list

    curl -X POST -H "Content-Type: application/x-www-form-urlencoded" -d 'result=sample&query=study_accession%3D%22PRJDB13387%22&fields=sample_accession%2Csample_description%2Cstudy_accession&format=tsv' "https://www.ebi.ac.uk/ena/portal/api/search"
    sample_accession	sample_description	study_accession
    SAMD00454395	Mus musculus	PRJDB13387
    SAMD00454397	Mus musculus	PRJDB13387
    SAMD00454399	Mus musculus	PRJDB13387
    SAMD00454401	Mus musculus	PRJDB13387
    SAMD00454396	Mus musculus	PRJDB13387
    SAMD00454398	Mus musculus	PRJDB13387
    SAMD00454400	Mus musculus	PRJDB13387
    SAMD00454394	Mus musculus	PRJDB13387
    """
    #
    # if study_collection == None:
    #     study_collection = StudyCollection()

    #ic(study_collection.get_global_study_dict())
    sample_acc_set = set()
    result_object_type = 'sample'
    limit = 0
    study_id = 'PRJDB13387'
    global_sample_acc_set = set()

    # curl -X POST -H "Content-Type: application/x-www-form-urlencoded"
    # -d 'result=sample&query=study_accession%3DPRJDB13387&fields=sample_accession%2Csample_description%2Cstudy_accession&format=tsv'
    # 'https://www.ebi.ac.uk/ena/portal/api/search'
    # curl 'https://www.ebi.ac.uk/ena/portal/api/search?result=sample&query=study_accession%3D%22PRJNA505510%22%20OR%20study_accession%3D%22PRJEB32543%22&fields=sample_accession%2Csample_description%2Cstudy_accession&format=tsv'

    #currently very inefficient doing one call per study_id
    pre_url = get_ena_portal_url() + "search?" + 'result=' + result_object_type + '&fields=sample_accession&format=tsv'
    pre_url += '&limit=' + str(limit) + '&query=study_accession' + '%3D'

    return_fields = ['sample_accession','study_accession']
    #the following does not work as not as study is not a valid accessionType
    #data = chunk_portal_api_call(get_ena_portal_url() + "search?" + "&includeAccessionType=study", result_object_type, return_fields, study_id_list)
    url = get_ena_portal_url() + "search?"
    data = chunk_portal_api_call_w_ands(url, result_object_type, return_fields, 'study_accession', study_id_list)
    # logger.info(data)

    #parse the data into a simple dictionary
    study_hash = {}
    for row_dict in data:
        #logger.info(f"{row_dict['study_accession']} {row_dict['sample_accession']}")
        if row_dict['study_accession'] not in study_hash:
            study_hash[row_dict['study_accession']] = set()
        study_hash[row_dict['study_accession']].add(row_dict['sample_accession'])

    # Build the study_collection.study_dict
    for study_id in study_id_list:
       if study_id in study_collection.study_dict:
           sample_acc_set.add(study_collection.study_dict[study_id]['sample_acc_set'])
       else:
          study_collection.study_dict['study'][study_id] = {}
          #logger.info(study_collection.study_dict)f
          if study_id in study_hash:
              # logger.info(f"{study_id} in study_hash")
              study_collection.study_dict['study'][study_id]['sample_acc_set'] = study_hash[study_id]
              global_sample_acc_set.update(study_hash[study_id])
          else:
              #logger.info(f"{study_id} NOT in study_hash")
              study_collection.study_dict['study'][study_id]['sample_acc_set'] = set()  #i.e. no samples found for study!
          if debug_status:
            logger.info(f"\tfor {study_id} found a total of {len(study_hash[study_id])} samples: {study_hash[study_id]}")
    #print(f"sample_ids={global_sample_acc_set}")
    return sorted(list(global_sample_acc_set))


def main():
    study_collection = StudyCollection()

    logger.info(study_collection.get_name())
    study_acc_list = ['PRJNA435556',
                     'PRJEB32543',
                     'PRJNA505510',
                     'PRJEB25385',
                     'PRJNA993105',
                     'PRJNA522285',
                     'PRJEB28751',
                     'PRJEB36404',
                     'PRJEB27360',
                     'PRJEB40122'
        ]
    #, "madeup"]
    #logger.info(study_acc_list)

    sample_acc_list = study2sample(study_acc_list, study_collection,False)

    logger.info(len(study_collection.get_sample_id_list()))

if __name__ == '__main__':
    main()
