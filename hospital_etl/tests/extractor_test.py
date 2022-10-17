"""
This file demonstrates common uses for the Python unittest module
https://docs.python.org/3/library/unittest.html
"""
import unittest

from datajob.extract.naver_jisik import NaverJisikExtractor


class MTest(unittest.TestCase):

    def test1(self):
        NaverJisikExtractor.extract_data()


if __name__ == '__main__':
    unittest.main()
