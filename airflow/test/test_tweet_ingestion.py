import os
import shutil
import unittest

from pyspark.sql.types import StructType

from include import TweetIngestion


class TestTweetIngestion(unittest.TestCase):
    def setUp(self):
        self.tmp_landing_path = ".tmp/ingestion/landing"
        self.tmp_raw_path = ".tmp/ingestion/raw"
        os.makedirs(self.tmp_landing_path, exist_ok=True)
        os.makedirs(self.tmp_raw_path, exist_ok=True)

        self.transformer = TweetIngestion(
            landing_path=self.tmp_landing_path,
            raw_path=self.tmp_raw_path,
        )
        self.transformer.spark = self.transformer.get_spark()

    def tearDown(self):
        shutil.rmtree("/".join(self.tmp_landing_path.split("/")[:-1]))

    def test_get_field_list(self):
        field_list = self.transformer.get_field_list()
        self.assertIsNotNone(field_list)
        self.assertIsInstance(field_list, StructType)

    def test_is_delta(self):
        is_delta = self.transformer.is_delta(self.tmp_landing_path)
        self.assertFalse(is_delta)


if __name__ == "__main__":
    unittest.main()
