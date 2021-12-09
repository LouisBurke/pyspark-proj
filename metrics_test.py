import unittest
import logging

import pandas as pd

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pandas.testing import assert_frame_equal

import metrics


class PySparkTest(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return SparkSession.builder.getOrCreate()

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_data_frame(self):
        row = {
            "id": [
                "1c269ade-d654-4374-aafb-3deaebe5376b",
                "1c269ade-d654-4374-aafb-3deaebe5376b"
            ],
            "datetime": [
                "2021-01-23 10:23:51",
                "2021-01-23 10:23:51"
            ],
            "domain": [
                "www.domain-A.eu",
                "www.domain-A.eu"
            ],
            "type": [
                "pageview",
                "pageview"
            ],
            "user":[
                {
                    "id": "1705c98b-367c-6d09-a30f-da9e6f4da700",
                    "country": "FR",
                    "token": "{\"vendors\":{\"enabled\":[],\"disabled\":[]},\"purposes\":{\"enabled\":[],\"disabled\":[]}}"
                },
                {
                    "id": "1705c98b-367c-6d09-a30f-da9e6f4da700",
                    "country": "FR",
                    "token": "{\"vendors\":{\"enabled\":[],\"disabled\":[]},\"purposes\":{\"enabled\":[],\"disabled\":[]}}"
                }
            ]
        }

        data_pandas = pd.DataFrame(row)

        # Turn the data into a Spark DataFrame, self.spark comes from our PySparkTest base class
        data_spark = self.spark.createDataFrame(data_pandas)

        # Invoke the unit we'd like to test
        results_spark = metrics.deduplicate_frame(data_spark, 'id')

        # Turn the results back to Pandas
        results_pandas = results_spark.toPandas()
        # Our expected results crafted by hand, again, this could come from a CSV
        # in case of a bigger example
        expected_results = pd.DataFrame(
            {
                "id": [
                    "1c269ade-d654-4374-aafb-3deaebe5376b"
                ],
                "datetime": [
                    "2021-01-23 10:23:51",
                ],
                "domain": [
                    "www.domain-A.eu",
                ],
                "type": [
                    "pageview",
                ],
                "user": [
                    {
                        "id": "1705c98b-367c-6d09-a30f-da9e6f4da700",
                        "country": "FR",
                        "token": "{\"vendors\":{\"enabled\":[],\"disabled\":[]},\"purposes\":{\"enabled\":[],\"disabled\":[]}}"
                    }
                ]
            }
        )

        # Assert that the 2 results are the same. We'll cover this function in a bit
        assert_frame_equal(results_pandas , expected_results)