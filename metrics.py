import findspark
findspark.init()

from pyspark.sql import SparkSession


def read_data(spark, path):
    return spark.read.json(path)


def deduplicate_frame(frame, column_name):
    return frame.drop_duplicates([column_name])


def get_distinct_types(spark):
    distinct_types = spark.sql("select distinct(type) from Events").collect()
    return [row.type for row in distinct_types]


def get_event_type_metrics(type, spark):
    return spark.sql(
        ' SELECT count(type) as type, datehour, domain, user[\'country\'] as country from Events \
          where type = "{event_type}" \
          group by datehour, domain, country order by datehour, domain, country'.format(event_type = type)
    )


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    input_data = read_data(spark, "/Users/burkel/pyspark-proj/input/")

    deduplicated_data = deduplicate_frame(input_data, 'id')
    deduplicated_data.createOrReplaceTempView("Events")

    distinct_types_list = get_distinct_types(spark)

    for type in distinct_types_list:
        print(type)
        get_event_type_metrics(type, spark).show()