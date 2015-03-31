from unittest import TestCase
from renamer import *

__author__ = 'California Audio Visual Preservation Project'


class TestRenamer(TestCase):
    def setUp(self):
        self.test = renamer()
    def test__calculate_md5(self):
        self.assertEquals(self.test._calculate_md5("/Users/lpsdesk/PycharmProjects/rename_images/testImages/dummy.tif"), "31329d32359d1aad5f70e92dad4ef392")