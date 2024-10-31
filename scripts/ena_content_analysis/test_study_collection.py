import unittest
from study_collection import *


class TestStudyCollection(unittest.TestCase):

    def setUp(self):
        self.study_collection = StudyCollection()


    #sample_acc_list = study2sample(study_acc_list, study_collection, False)

    #ic(len(study_collection.get_sample_id_list()))

    if __name__ == '__main__':
        ic()
        main()
    def test_get_name(self):
        self.assertEqual(self.study_collection.get_name(),'TBD')

    def test_get_global_study_dict(self):
        test_study_dict = {'study': {}, 'sample': {}}
        self.assertDictEqual(self.study_collection.get_global_study_dict(),test_study_dict)

    def test_get_sample_id_list(self):
        self.assertEqual(self.study_collection.get_sample_id_list(), [])

    def test_study2sample(self):
        study_acc_list = ['PRJNA435556',
                          'PRJEB32543',
                          'PRJNA505510',
                          'PRJEB25385',
                          'PRJNA993105',
                          'PRJNA522285',
                          'PRJEB28751',
                          'PRJEB36404',
                          'PRJEB27360',
                          'PRJEB40122',
                          "madeup"]
        sample_acc_list = study2sample(study_acc_list, self.study_collection, False)
        self.assertEqual(len(sample_acc_list), 121)



if __name__ == '__main__':
    unittest.main()

