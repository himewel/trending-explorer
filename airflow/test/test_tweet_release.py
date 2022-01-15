import os
import shutil
import unittest

from include import TweetRelease


class TestTweetRelease(unittest.TestCase):
    def setUp(self):
        self.tmp_raw_path = ".tmp/release/raw"
        self.tmp_trusted_path = ".tmp/release/trusted"
        os.makedirs(self.tmp_raw_path, exist_ok=True)
        os.makedirs(self.tmp_trusted_path, exist_ok=True)

        self.transformer = TweetRelease(
            raw_path=self.tmp_raw_path,
            trusted_path=self.tmp_trusted_path,
        )
        self.transformer.spark = self.transformer.get_spark()

    def tearDown(self):
        shutil.rmtree("/".join(self.tmp_raw_path.split("/")[:-1]))

    def test_is_delta(self):
        is_delta = self.transformer.is_delta(self.tmp_raw_path)
        self.assertFalse(is_delta)


if __name__ == "__main__":
    unittest.main()
