import unittest
import pandas as pd
from utils.data_cleaner import clean_sales_data


class InvalidNumberTests(unittest.TestCase):
    def test_bad_numeric_row_does_not_abort_batch(self):
        rows = pd.DataFrame({"date": ["2024-01-01"] * 2, "product": ["A", "B"], "quantity": [2, "bad"], "price": ["3.50", "4.00"]})
        result = clean_sales_data(rows)
        self.assertEqual(result["product"].tolist(), ["A"])
        self.assertEqual(result.iloc[0]["price"], 3.5)
