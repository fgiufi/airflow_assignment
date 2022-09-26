import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
#from pyspark.shell import spark
import sys


def test_struct():
    # spark = SparkSession.builder.getOrCreate()
    # spark.sparkContext.addPyFile("/Users/shiyu.chenthoughtworks.com/airflow/dags/postgresql-42.5.0.jar")
    appName = "PySpark SQL Server Example - via JDBC"
    master = "local"
    conf = SparkConf() \
        .setAppName(appName) \
        .setMaster(master) \
        .set("spark.driver.extraClassPath", "/Users/shiyu.chenthoughtworks.com/airflow/dags/postgresql-42.5.0.jar")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession
    # spark = SparkSession.builder.master("local*")\
    #     .config("spark.driver.userClassPathFirst", "/Users/shiyu.chenthoughtworks.com/airflow/dags/postgresql-42.5.0.jar")\
    #     .getOrCreate()
    # print(spark.sparkContext.getConf().getAll())
    # sys.path.append("/Users/shiyu.chenthoughtworks.com/airflow/dags/postgresql-42.5.0.jar")
    # spark1.sparkContext._jsc.addJar("/Users/shiyu.chenthoughtworks.com/airflow/dags/postgresql-42.5.0.jar")
    # spark.sparkContext.addPyFile("/Users/shiyu.chenthoughtworks.com/airflow/dags/postgresql-42.5.0.jar")
    file_path = "/Users/shiyu.chenthoughtworks.com/github/scalatest/scalatest01/src/data/"
    sales_raw_path = "raw/2022-09-22/"
    sales_order_data_file_name = file_path + sales_raw_path + "SalesOrderData.csv"
    sales_order_data_csv = spark.read.csv(
        path=sales_order_data_file_name,
        header=True,
    )
    schema = StructType([StructField("sales_order_id", IntegerType()),
                         StructField("sales_order_detail_id", IntegerType()),
                         StructField("revision_number", IntegerType()),
                         StructField("order_date", StringType()),
                         StructField("due_date", StringType()),
                         StructField("ship_date", StringType()),
                         StructField("status", StringType()),
                         StructField("online_order_flag", StringType()),
                         StructField("sales_order_number", StringType()),
                         StructField("purchase_order_number", StringType()),
                         StructField("account_number", StringType()),
                         StructField("customer_id", StringType()),
                         StructField("ship_to_address_id", StringType()),
                         StructField("bill_to_address_id", StringType()),
                         StructField("ship_method", StringType()),
                         StructField("credit_card_approval_code", StringType()),
                         StructField("sub_total", DoubleType()),
                         StructField("tax_amt", DoubleType()),
                         StructField("freight", DoubleType()),
                         StructField("total_due", DoubleType()),
                         StructField("comment", StringType()),
                         StructField("order_gty", IntegerType()),
                         StructField("product_id", IntegerType()),
                         StructField("unit_price", DoubleType()),
                         StructField("unit_price_discount", DoubleType()),
                         StructField("line_total", DoubleType()),
                         StructField("rowguid", StringType()),
                         StructField("modified_date", StringType())
                         ])
    # new_df = spark.createDataFrame()
    df = sales_order_data_csv.toDF(*(schema.fieldNames())).selectExpr(
        *[f"CAST ({c.name} AS {c.dataType.simpleString()}) {c.name}" for c in schema])
    df = df.select(col("sales_order_id"), col("sales_order_detail_id"))
    df.show()
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:54321/report") \
        .option("dbtable", "report.test") \
        .option("user", "shiyu") \
        .option("password", "123456") \
        .save()

    # df1 = spark.read.parquet("/Users/shiyu.chenthoughtworks.com/github/scalatest/scalatest01/src/data/parquet_file/2022-09-22/salesOrder_parquet")
    # df1.show()


if __name__ == '__main__':
    test_struct()
