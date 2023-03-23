import spark as spark
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, MapType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, MapType
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as func
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.sparkContext.setLogLevel('WARN')


spark = SparkSession \
    .builder \
    .appName("NGPC_Interface_subinterface_JsonPasing") \
    .getOrCreate()


# Schema Strucrute for Interface

Interface_schema = StructType([
    StructField("ucgDeviceName", StringType(), True),
    StructField("ucgJsonData", StructType([
        StructField("interfaces", StructType([
            StructField("interface", MapType(StringType(), StructType([
                StructField("name", StringType(), True),
                StructField("state", StructType([
                    StructField("counters", StructType([
                        StructField("carrier-transitions", LongType(), True),
                        StructField("in-broadcast-pkts", LongType(), True),
                        StructField("in-discards", LongType(), True),
                        StructField("in-errors", LongType(), True),
                        StructField("in-fcs-errors", LongType(), True),
                        StructField("in-multicast-pkts", LongType(), True),
                        StructField("in-octets", LongType(), True),
                        StructField("in-pause-pkts", LongType(), True),
                        StructField("in-pkts", LongType(), True),
                        StructField("in-unicast-pkts", LongType(), True),
                        StructField("in-unknown-proto-pkts", LongType(), True),
                        StructField("out-broadcast-pkts", LongType(), True),
                        StructField("out-discards", LongType(), True),
                        StructField("out-errors", LongType(), True),
                        StructField("out-multicast-pkts", LongType(), True),
                        StructField("out-octets", LongType(), True),
                        StructField("out-pause-pkts", LongType(), True),
                        StructField("out-pkts", LongType(), True),
                        StructField("out-unicast-pkts", LongType(), True),
                        StructField("out-unknown-proto-pkts", LongType(), True),
                        StructField("last-clear", LongType(), True)
                    ]), True)
                ]), True)
            ]), True))
        ]), True),
        StructField("timestamp", LongType(), True)
    ]), True),
    StructField("ucgMessage", StringType(), True),
    StructField("ucgSequenceNum", StringType(), True),
    StructField("ucgSource", StringType(), True),
    StructField("ucgTimestamp", StringType(), True),
    StructField("ucgTopic", StringType(), True),
    StructField("ucgType", StringType(), True),
    StructField("ucgYangTopic", StringType(), True)
])

#Schema Structure for Subinterface

subInterface_Schema = StructType([
    StructField("ucgDeviceName", StringType(), True),
    StructField("ucgJsonData", StructType([
        StructField("interfaces", StructType([
            StructField("interface", MapType(StringType(), StructType([
                StructField("name", StringType(), True),
                StructField("subinterfaces", StructType([
                    StructField("subinterface", MapType(StringType(), StructType([
                        StructField("index", StringType(), True),
                        StructField("state", StructType([
                            StructField("counters", StructType([
                                StructField("in-octets", IntegerType(), True),
                                StructField("in-pkts", IntegerType(), True),
                                StructField("out-octets", IntegerType(), True),
                                StructField("out-pkts", IntegerType(), True)
                            ]), True)
                        ]), True)
                    ])), True)
                ]), True)
            ])), True)
        ]), True),
        StructField("timestamp", StringType(), True)
    ]), True),
    StructField("ucgMessage", StringType(), True),
    StructField("ucgSequenceNum", StringType(), True),
    StructField("ucgSource", StringType(), True),
    StructField("ucgTimestamp", StringType(), True),
    StructField("ucgTopic", StringType(), True),
    StructField("ucgType", StringType(), True),
    StructField("ucgYangTopic", StringType(), True)
])

# Reading data with Interface Schema
interface_rawdf = spark.read.json("gs://h6zv-dev-npipr-0-usmr-public_ip/vz.public_ip.eclipse.stat.ngpc_perf.orig.v0/date=20230320/*.json", schema=Interface_schema)



# Reading data with Subinterface Schema
subinterface_rawdf = spark.read.json("gs://h6zv-dev-npipr-0-usmr-public_ip/vz.public_ip.eclipse.stat.ngpc_perf.orig.v0/date=20230320/*.json", schema=subInterface_Schema)


_interface_parsed = interface_rawdf.select("ucgDeviceName",col("ucgJsonData.interfaces.interface").alias("interfaces"), col("ucgJsonData.timestamp")).alias("timestamp") \
    .select("ucgDeviceName","timestamp",explode(col("interfaces"))) \
    .select("ucgDeviceName","timestamp","key","value.*") \
    .select("ucgDeviceName","timestamp","name","state.counters.*","state.high-speed") \
    .withColumn("timestamp_c",from_unixtime(col("timestamp")/1000000000, 'yyyy-MM-dd HH:mm:ss.SSS').cast('timestamp')).withColumnRenamed("name","interface")

"""
_subinterface_parsed = subinterface_rawdf.selectExpr("ucgDeviceName", "ucgJsonData.timestamp", "explode(ucgJsonData.interfaces.interface) as (interface, interface_info)") \
    .selectExpr("ucgDeviceName","timestamp", "interface", "explode(interface_info.subinterfaces.subinterface) as (subinterface_index, subinterface_info)") \
    .select("ucgDeviceName", "timestamp", "interface",col("subinterface_info.index").alias("subinterface_name")\
           ,col("subinterface_info.state.counters.in-octets").alias("in_octets_sub")\
           ,col("subinterface_info.state.counters.in-pkts").alias("in_pkts_sub")\
           ,col("subinterface_info.state.counters.out-octets").alias("out_octets_sub")\
           ,col("subinterface_info.state.counters.out-pkts").alias("out_pkts_sub")).withColumn("timestamp",from_unixtime(col("timestamp")/1000000000.0))
           
"""

_subinterface_parsed = subinterface_rawdf.selectExpr("ucgDeviceName", "ucgJsonData.timestamp", "explode(ucgJsonData.interfaces.interface) as (interface, interface_info)") \
    .selectExpr("ucgDeviceName","timestamp", "interface", "explode(interface_info.subinterfaces.subinterface) as (subinterface_index, subinterface_info)") \
    .select("ucgDeviceName", "timestamp", "interface",col("subinterface_info.index").alias("subinterface_name") \
            ,col("subinterface_info.state.counters.in-octets").alias("in_octets").cast('Long') \
            ,col("subinterface_info.state.counters.in-pkts").alias("in_pkts").cast('Long') \
            ,col("subinterface_info.state.counters.out-octets").alias("out_octets").cast('Long') \
            ,col("subinterface_info.state.counters.out-pkts").alias("out_pkts").cast('Long')).withColumn("new_timestamp",from_unixtime(col("timestamp")/1e9, 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("fraction_of_seconds", format_number((col("timestamp")%1e9)/1e9, 9)) \
    .withColumn("fraction_of_seconds", substring( col("fraction_of_seconds"), 3,9)) \
    .withColumn("timestamp", concat( col("new_timestamp"), lit("."), col("fraction_of_seconds")).cast('timestamp')).drop(col("fraction_of_seconds"))

#write the parsed data to GCS bucket
_interface_parsed.coalesce(1).write.format("json").save()
_subinterface_parsed.coalesce(1).write.format("json").save()



# selecting the columns for delta calculation purpose  and removing the null values in primary subset of columns

interface_selected_cols = _interface_parsed.select("ucgDeviceName","timestamp","interface","carrier-transitions","in-discards","in-errors","in-octets","in-pkts","out-discards","out-errors","out-octets","out-pkts") \
    .na.drop(subset=["ucgDeviceName","timestamp","interface","in-octets"])


### Defining the window
Windowspec=Window.partitionBy("ucgDeviceName","interface").orderBy("timestamp")

### Calculating lag of price at each day level
prev_time_calc= interface_selected_cols.withColumn('prev_time',
                                                   func.lag(interface_selected_cols['timestamp'])
                                                   .over(Windowspec)) \
    .withColumn('prev_in_octets',
                func.lag(interface_selected_cols['in-octets'])
                .over(Windowspec)) \
    .withColumn('prev_in_pkts',
                func.lag(interface_selected_cols['in-pkts'])
                .over(Windowspec)) \
    .withColumn('prev_out_octets',
                func.lag(interface_selected_cols['out-octets'])
                .over(Windowspec)) \
    .withColumn('prev_out_pkts',
                func.lag(interface_selected_cols['out-pkts'])
                .over(Windowspec))

### Calculating the delta
interface_delta_result = prev_time_calc.withColumn('delta_time', when(col("prev_time").isNotNull(),(prev_time_calc['timestamp'] - prev_time_calc['prev_time']).cast(StringType())).otherwise(lit(0))) \
    .withColumn('delta_in_octets',
                when( col("prev_in_octets").isNotNull(), (prev_time_calc['in-octets'] - prev_time_calc['prev_in_octets'])).otherwise(lit(0))) \
    .withColumn('delta_in_pkts',
                when( col("prev_in_pkts").isNotNull(), (prev_time_calc['in-pkts'] - prev_time_calc['prev_in_pkts'])).otherwise(lit(0))) \
    .withColumn('delta_out_octets',
                when(col("prev_out_octets").isNotNull(),(prev_time_calc['out-octets'] - prev_time_calc['prev_out_octets'])).otherwise(lit(0))) \
    .withColumn('delta_out_pkts',
                when(col("prev_out_pkts").isNotNull(),(prev_time_calc['out-pkts'] - prev_time_calc['prev_out_pkts'])).otherwise(lit(0)))

interface_delta_result.coalesce(1).write.format("json").save()

# selected columns for subinterface level
sub_interface_selected_cols = _subinterface_parsed.select("ucgDeviceName","interface","timestamp","subinterface_name","in_octets","in_pkts","out_octets","out_pkts") \
    .withColumnRenamed("subinterface_name","subinterface").na.drop(subset=["ucgDeviceName","timestamp","interface","in_octets"])

# ### Defining the window
Windowspec_sub = Window.partitionBy("ucgDeviceName","interface","subinterface").orderBy("timestamp")

### Calculating lag of price at each day level
sub_prev_time_calc= sub_interface_selected_cols.withColumn('prev_time',
                                                           func.lag(sub_interface_selected_cols['timestamp'])
                                                           .over(Windowspec_sub)) \
    .withColumn('prev_in_octets',
                func.lag(sub_interface_selected_cols['in_octets'])
                .over(Windowspec_sub)) \
    .withColumn('prev_in_pkts',
                func.lag(sub_interface_selected_cols['in_pkts'])
                .over(Windowspec_sub)) \
    .withColumn('prev_out_octets',
                func.lag(sub_interface_selected_cols['out_octets'])
                .over(Windowspec_sub)) \
    .withColumn('prev_out_pkts',
                func.lag(sub_interface_selected_cols['out_pkts'])
                .over(Windowspec_sub))

### Calculating the delta
delta_sub_result = sub_prev_time_calc.withColumn('delta_time', when(col("prev_time").isNotNull(),(sub_prev_time_calc['timestamp'] - sub_prev_time_calc['prev_time']).cast(StringType())).otherwise(lit(0))) \
    .withColumn('delta_in_octets', when( col("prev_in_octets").isNotNull(), (sub_prev_time_calc['in_octets'] - sub_prev_time_calc['prev_in_octets'])).otherwise(lit(0))) \
    .withColumn('delta_in_pkts', when( col("prev_in_pkts").isNotNull(), (sub_prev_time_calc['in_pkts'] - sub_prev_time_calc['prev_in_pkts'])).otherwise(lit(0))) \
    .withColumn('delta_out_octets', when(col("prev_out_octets").isNotNull(),(sub_prev_time_calc['out_octets'] - sub_prev_time_calc['prev_out_octets'])).otherwise(lit(0))) \
    .withColumn('delta_out_pkts', when(col("prev_out_pkts").isNotNull(),(sub_prev_time_calc['out_pkts'] - sub_prev_time_calc['prev_out_pkts'])).otherwise(lit(0)))


delta_sub_result.coalesce(1).write.format("csv").option("header",True).save("gs://h6zv-dev-npipr-0-usmr-internal/ruthvik/delta_out_sub_1")
