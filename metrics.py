import findspark
findspark.init()

from pyspark.sql import SparkSession

from pyspark.sql.functions import udf,col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType

from json import loads


def json_manip(json_string):
    if len(loads(json_string)['purposes']['enabled']) > 0:
        return True
    else:
        return False


UDF_JSON_MANIP = udf(lambda x: json_manip(x),BooleanType())
RAW_EVENTS = 'Events'


def read_data(spark, path):
    return spark.read.json(path)


def deduplicate_frame(frame, column_name):
    return frame.drop_duplicates([column_name])


def get_distinct_types(spark):
    distinct_types = spark.sql("select distinct(type) from " + RAW_EVENTS).collect()
    return [row.type for row in distinct_types]


def get_event_type_metrics(type, spark):
    return spark.sql(
        ' SELECT count(type) as count, datehour, domain, user[\'country\'] as country from Events \
          where type = "{event_type}" \
          group by datehour, domain, country order by datehour, domain, country'.format(event_type = type)
    )


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    input_data = read_data(spark, "/Users/burkel/pyspark-proj/input/")

    deduplicated_data = deduplicate_frame(input_data, 'id')

    deduplicated_data.createOrReplaceTempView(RAW_EVENTS)

    distinct_types_list = get_distinct_types(spark)

    for type in distinct_types_list:
        print(type)
        get_event_type_metrics(type, spark).show()

    deduplicated_data_token = deduplicated_data.withColumn('token', deduplicated_data.user.token)

    dedupded_with_consent = deduplicated_data_token.withColumn('consented',  UDF_JSON_MANIP(col("token")))

    dedupded_with_consent.createOrReplaceTempView('EventsConsented')

    print('Page views with consent.')
    spark.sql(
        'SELECT count(type) as count, datehour, domain, user[\'country\'] as country from EventsConsented \
         where type = \'pageview\' AND consented = \'true\' \
         group by datehour, domain, country order by datehour, domain, country'
    ).show()

    print('Consent given with consent.')
    spark.sql(
        'SELECT count(type) as count, datehour, domain, user[\'country\'] as country from EventsConsented \
         where type = \'consent.given\' AND consented = \'true\' \
         group by datehour, domain, country order by datehour, domain, country'
    ).show()

    print('avg_pageviews_per_user')
    spark.sql('select user.id, datehour, domain, user[\'country\'] as country, count(type) as view from EventsConsented where type = \'pageview\' group by datehour, domain, country, user.id order by datehour, user.id').createOrReplaceTempView('userpageviews')
    spark.sql('select id, mean(view), datehour, domain, country from userpageviews group by datehour, domain, country, id').show()