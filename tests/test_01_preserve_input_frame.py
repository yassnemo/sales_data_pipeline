import unittest
import pandas as pd
from utils.data_cleaner import clean_sales_data


class CleanerCopyTests(unittest.TestCase):
    def test_original_frame_remains_unchanged(self):
        original = pd.DataFrame({"date": ["2024-01-01", None], "product": ["A", "B"], "quantity": [1, 2], "price": [3.0, 4.0]})
        before = original.copy(deep=True)
        clean_sales_data(original)
        pd.testing.assert_frame_equal(original, before)
