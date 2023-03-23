from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def parse_json(json_data):
    # parse json_data using defined schema
    parsed_data = spark.read.json(json_data, schema=schema)
    return parsed_data

if __name__ == "__main__":
    try:
        # create SparkSession
        spark = SparkSession.builder.appName("json_parser").getOrCreate()

        # define schema for json records
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True)
        ])

        # read json with defined schema
        json_data = spark.read.json("path/to/json/file.json", schema=schema)

        # call parse_json function to parse json using schema
        parsed_data = parse_json(json_data)

        # print parsed data
        parsed_data.show()

    except Exception as e:
        print("Error occurred: {}".format(str(e)))

    finally:
        # stop SparkSession
        spark.stop()
