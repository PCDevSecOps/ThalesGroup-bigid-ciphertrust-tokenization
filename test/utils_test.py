import unittest

import utils.utils as ut


class UtilsTest(unittest.TestCase):

    def test_get_unique_id_record(self):
        self.assertTrue(True)
    
    def test_read_categories(self):
        inputs_expected = [
            ("", set()),
            ("cat1", {"cat1"}),
            ("cat1 ", {"cat1"}),
            ("   cat1 ", {"cat1"}),
            ("cat1,cat2,cat3", {"cat1", "cat2", "cat3"}),
            ("  cat1,  cat2  ,cat3  ", {"cat1", "cat2", "cat3"}),
            ("  cat1,  cat2  ,cat1  ", {"cat1", "cat2"}),
        ]
        for input, expected in inputs_expected:
            self.assertEquals(expected, ut.read_categories(input),
                f"Input '{input}' did not generate the expected results: '{expected}'")
    
    def test_category_allowed(self):
        found_allowed_expected = [
            ([])
        ]
