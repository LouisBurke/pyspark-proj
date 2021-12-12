import unittest
import logging
import datetime
import pandas as pd

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pandas.testing import assert_frame_equal

import metrics


class PySparkTest(unittest.TestCase):

    ROWS = {
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
        data_pandas = pd.DataFrame(self.ROWS)

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
        expected_out_pandas = pd.DataFrame(
            {
                'count': {0: 1, 1: 4, 2: 4, 3: 7, 4: 9, 5: 4, 6: 2},
                'datehour': {0: '2021-01-23-10', 1: '2021-01-23-10', 2: '2021-01-23-10', 3: '2021-01-23-10', 4: '2021-01-23-11', 5: '2021-01-23-11', 6: '2021-01-23-11'},
                'domain': {0: 'my-other-website.com', 1: 'www.domain-A.eu', 2: 'www.domain-A.eu', 3: 'www.mywebsite.com', 4: 'www.domain-A.eu', 5: 'www.mywebsite.com', 6: 'www.mywebsite.com'}, 
                'country': {0: 'FR', 1: 'ES', 2: 'FR', 3: 'FR', 4: 'ES', 5: 'DE', 6: 'FR'}
            }
        )

        input_data = metrics.read_data(self.spark, "/Users/burkel/pyspark-proj/input/")
        deduplicated_data = metrics.deduplicate_frame(input_data, 'id')
        deduplicated_data.createOrReplaceTempView("Events")

        actual_out_pandas = metrics.get_event_type_metrics('pageview', self.spark).toPandas()

        assert_frame_equal(expected_out_pandas, actual_out_pandas)

    def test_get_event_type_metrics_consented(self):
        expected_out_pandas = pd.DataFrame(
            {
                'count': {0: 2, 1: 3, 2: 3, 3: 3, 4: 3, 5: 2}, 
                'datehour': {0: '2021-01-23-10', 1: '2021-01-23-10', 2: '2021-01-23-10', 3: '2021-01-23-11', 4: '2021-01-23-11', 5: '2021-01-23-11'}, 
                'domain': {0: 'www.domain-A.eu', 1: 'www.domain-A.eu', 2: 'www.mywebsite.com', 3: 'www.domain-A.eu', 4: 'www.mywebsite.com', 5: 'www.mywebsite.com'}, 
                'country': {0: 'ES', 1: 'FR', 2: 'FR', 3: 'ES', 4: 'DE', 5: 'FR'}
            }
        )

        input_data = metrics.read_data(self.spark, "/Users/burkel/pyspark-proj/input/")
        deduplicated_data = metrics.deduplicate_frame(input_data, 'id')
        deduplicated_data.createOrReplaceTempView(metrics.RAW_EVENTS)

        deduplicated_data_token = deduplicated_data.withColumn('token', deduplicated_data.user.token)
        dedupded_with_consent = deduplicated_data_token.withColumn('consented', metrics.UDF_JSON_MANIP(col("token")))
        dedupded_with_consent.createOrReplaceTempView('EventsConsented')

        actual_out_pandas = metrics.get_event_type_metrics_consented('pageview', self.spark).toPandas()

        assert_frame_equal(expected_out_pandas, actual_out_pandas)


    def test_get_average_pageviews_per_user(self):
        test_input_data = metrics.read_data(self.spark, "./test_input/")

        input_data_with_token = test_input_data.withColumn('token', test_input_data.user.token)
        input_data_with_token_and_consent = input_data_with_token.withColumn('consented', metrics.UDF_JSON_MANIP(col("token")))
        input_data_with_token_and_consent.createOrReplaceTempView('EventsConsented')

        expected_out_pandas = pd.DataFrame(
            {
                'id': {0: '1705c98b-367c-6d09-a30f-da9e6f4da701', 1: '1705c98b-367c-6d09-a30f-da9e6f4da701', 2: '1705c98b-367c-6d09-a30f-da9e6f4da701'}, 
                'avg(view)': {0: 2.0, 1: 2.0, 2: 2.0}, 
                'datehour': {0: datetime.date(2021, 1, 23), 1: datetime.date(2021, 1, 24), 2: datetime.date(2021, 1, 25)}, 
                'domain': {0: 'www.domain-A.eu', 1: 'www.domain-A.eu', 2: 'www.domain-A.eu'}, 
                'country': {0: 'FR', 1: 'FR', 2: 'FR'}
            }
        )

        actual_out_pandas = metrics.get_average_pageviews_per_user('pageview', self.spark).toPandas()

        assert_frame_equal(expected_out_pandas, actual_out_pandas)


if __name__ == "__main__":
    unittest.main()