import unittest
import logging

import pandas as pd

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pandas.testing import assert_frame_equal

import metrics


class PySparkTest(unittest.TestCase):

    ROW = {
        "id": [
            "1c269ade-d654-4374-aafb-3deaebe5376a",
            "1c269ade-d654-4374-aafb-3deaebe5376b",
            "1c269ade-d654-4374-aafb-3deaebe5376c"
        ],
        "datetime": [
            "2021-01-23 10:23:51",
            "2021-01-23 10:23:52",
            "2021-01-23 10:23:53"
        ],
        "domain": [
            "www.domain-A.eu",
            "www.domain-A.eu",
            "www.domain-A.eu"
        ],
        "type": [
            'pageview',
            'consent.given',
            'consent.asked'
        ],
        "user":[
            {
                "id": "1705c98b-367c-6d09-a30f-da9e6f4da700",
                "country": "FR",
                "token": "{\"vendors\":{\"enabled\":[],\"disabled\":[]},\"purposes\":{\"enabled\":[],\"disabled\":[]}}"
            },
            {
                "id": "1705c98b-367c-6d09-a30f-da9e6f4da701",
                "country": "FR",
                "token": "{\"vendors\":{\"enabled\":[],\"disabled\":[]},\"purposes\":{\"enabled\":[],\"disabled\":[]}}"
            },
            {
                "id": "1705c98b-367c-6d09-a30f-da9e6f4da702",
                "country": "FR",
                "token": "{\"vendors\":{\"enabled\":[],\"disabled\":[]},\"purposes\":{\"enabled\":[],\"disabled\":[]}}"
            }
        ]
    }

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

    def test_deduplicate_frame(self):
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

        # Turn the data into a Spark DataFrame.
        data_spark = self.spark.createDataFrame(data_pandas)

        # The unit test.
        results_spark = metrics.deduplicate_frame(data_spark, 'id')

        # Turn the results back to Pandas
        pandas_results = results_spark.toPandas()

        # Expected result is a frame with one row.
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

        # Assert that the 2 results are the same. 
        assert_frame_equal(pandas_results, expected_results)


    def test_distinct_types(self):
        data_pandas = pd.DataFrame(self.ROW)

        # Turn the data into a Spark DataFrame
        data_spark = self.spark.createDataFrame(data_pandas)
        deduplicated_data = metrics.deduplicate_frame(data_spark, 'id')
        deduplicated_data.createOrReplaceTempView("Events")

        # The unit test.
        results_spark_list = metrics.get_distinct_types(self.spark)

        expected_list = [
            'pageview',
            'consent.given',
            'consent.asked'
        ]

        # Assert that the 2 results are the same. 
        self.assertEqual(set(expected_list), set(results_spark_list))


    def test_get_event_type_metrics(self):
        data_pandas = pd.DataFrame(self.ROW)

        # Turn the data into a Spark DataFrame
        data_spark = self.spark.createDataFrame(data_pandas)
        data_spark.createOrReplaceTempView("Events")

        metric_frame = metrics.get_event_type_metrics('pageview', self.spark)


if __name__ == "__main__":
    unittest.main()